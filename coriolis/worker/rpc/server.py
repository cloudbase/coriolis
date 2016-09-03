# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from logging import handlers
import multiprocessing
import os
import queue
import shutil
import signal
import sys

import psutil
from oslo_config import cfg
from oslo_log import log as logging

from coriolis.conductor.rpc import client as rpc_conductor_client
from coriolis import constants
from coriolis import events
from coriolis import exception
from coriolis.tasks import factory as task_runners_factory
from coriolis import utils


worker_opts = [
    cfg.StrOpt('export_base_path',
               default='/tmp',
               help='The path used for hosting exported disks.'),
]

CONF = cfg.CONF
CONF.register_opts(worker_opts, 'worker')

LOG = logging.getLogger(__name__)

VERSION = "1.0"


class _ConductorProviderEventHandler(events.BaseEventHandler):
    def __init__(self, ctxt, task_id):
        self._ctxt = ctxt
        self._task_id = task_id
        self._rpc_conductor_client = rpc_conductor_client.ConductorClient()

    def progress_update(self, current_step, total_steps, message):
        LOG.info("Progress update: %s", message)
        self._rpc_conductor_client.task_progress_update(
            self._ctxt, self._task_id, current_step, total_steps, message)

    def info(self, message):
        LOG.info(message)
        self._rpc_conductor_client.task_event(
            self._ctxt, self._task_id, constants.TASK_EVENT_INFO, message)

    def warn(self, message):
        LOG.warn(message)
        self._rpc_conductor_client.task_event(
            self._ctxt, self._task_id, constants.TASK_EVENT_WARNING, message)

    def error(self, message):
        LOG.error(message)
        self._rpc_conductor_client.task_event(
            self._ctxt, self._task_id, constants.TASK_EVENT_ERROR, message)


class WorkerServerEndpoint(object):
    def __init__(self):
        self._server = utils.get_hostname()
        self._rpc_conductor_client = rpc_conductor_client.ConductorClient()

    def _check_remove_dir(self, path):
        try:
            if os.path.exists(path):
                shutil.rmtree(path)
        except Exception as ex:
            # Ignore the exception
            LOG.exception(ex)

    def cancel_task(self, ctxt, task_id, process_id, force):
        if not force and os.name == "nt":
            LOG.warn("Windows does not support SIGINT, performing a "
                     "forced task termination")
            force = True

        try:
            p = psutil.Process(process_id)

            if force:
                LOG.warn("Killing process: %s", process_id)
                p.kill()
            else:
                LOG.info("Sending SIGINT to process: %s", process_id)
                p.send_signal(signal.SIGINT)
        except psutil.NoSuchProcess:
            err_msg = "Task process not found: %s" % process_id
            LOG.info(err_msg)
            self._rpc_conductor_client.set_task_error(ctxt, task_id, err_msg)

    def _handle_mp_log_events(self, p, mp_log_q):
        while True:
            try:
                record = mp_log_q.get(timeout=1)
                if record is None:
                    break
                logger = logging.getLogger(record.name).logger
                logger.handle(record)
            except queue.Empty:
                if not p.is_alive():
                    break

    def _exec_task_process(self, ctxt, task_id, task_type, origin, destination,
                           instance, task_info):
        mp_ctx = multiprocessing.get_context('spawn')
        mp_q = mp_ctx.Queue()
        mp_log_q = mp_ctx.Queue()
        p = mp_ctx.Process(
            target=_task_process,
            args=(ctxt, task_id, task_type, origin, destination, instance,
                  task_info, mp_q, mp_log_q))

        p.start()
        LOG.info("Task process started: %s", task_id)
        self._rpc_conductor_client.set_task_host(
            ctxt, task_id, self._server, p.pid)

        self._handle_mp_log_events(p, mp_log_q)
        p.join()

        if mp_q.empty():
            raise exception.CoriolisException("Task canceled")
        result = mp_q.get(False)

        if isinstance(result, str):
            raise exception.TaskProcessException(result)
        return result

    def exec_task(self, ctxt, task_id, task_type, origin, destination,
                  instance, task_info):
        export_path = task_info.get("export_path")
        if not export_path:
            export_path = _get_task_export_path(task_id, create=True)
            task_info["export_path"] = export_path
        retain_export_path = False
        task_info["retain_export_path"] = retain_export_path

        try:
            new_task_info = self._exec_task_process(
                ctxt, task_id, task_type, origin, destination,
                instance, task_info)

            if new_task_info:
                LOG.info("Task info: %s", new_task_info)

            # TODO: replace the temp storage with a host independent option
            retain_export_path = new_task_info.get("retain_export_path", False)
            if not retain_export_path:
                del new_task_info["export_path"]

            LOG.info("Task completed: %s", task_id)
            self._rpc_conductor_client.task_completed(ctxt, task_id,
                                                      new_task_info)
        except Exception as ex:
            LOG.exception(ex)
            self._rpc_conductor_client.set_task_error(ctxt, task_id, str(ex))
        finally:
            if not retain_export_path:
                self._check_remove_dir(export_path)


def _get_task_export_path(task_id, create=False):
    export_path = os.path.join(CONF.worker.export_base_path, task_id)
    if create and not os.path.exists(export_path):
        os.makedirs(export_path)
    return export_path


def _setup_task_process(mp_log_q):
    # Setting up logging and cfg, needed since this is a new process
    cfg.CONF(sys.argv[1:], project='coriolis', version="1.0.0")
    utils.setup_logging()

    # Log events need to be handled in the parent process
    log_root = logging.getLogger(None).logger
    for handler in log_root.handlers:
        log_root.removeHandler(handler)
    log_root.addHandler(handlers.QueueHandler(mp_log_q))


def _task_process(ctxt, task_id, task_type, origin, destination, instance,
                  task_info, mp_q, mp_log_q):
    try:
        _setup_task_process(mp_log_q)

        task_runner = task_runners_factory.get_task_runner(task_type)
        event_handler = _ConductorProviderEventHandler(ctxt, task_id)

        LOG.debug("Executing task: %(task_id)s, type: %(task_type)s, "
                  "origin: %(origin)s, destination: %(destination)s, "
                  "instance: %(instance)s, task_info: %(task_info)s",
                  {"task_id": task_id, "task_type": task_type,
                   "origin": origin, "destination": destination,
                   "instance": instance, "task_info": task_info})

        new_task_info = task_runner.run(
            ctxt, instance, origin, destination, task_info, event_handler)

        # mq_p.put() doesn't raise if new_task_info is not serializable
        utils.is_serializable(new_task_info)

        mp_q.put(new_task_info)
    except Exception as ex:
        mp_q.put(str(ex))
        LOG.exception(ex)
    finally:
        # Signal the log event handler that there are no more events
        mp_log_q.put(None)
