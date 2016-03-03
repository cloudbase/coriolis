from logging import handlers
import multiprocessing
import os
import queue
import shutil
import sys

from oslo_config import cfg
from oslo_log import log as logging
import psutil

from coriolis.conductor.rpc import client as rpc_conductor_client
from coriolis import constants
from coriolis import events
from coriolis import exception
from coriolis.providers import factory
from coriolis import secrets
from coriolis import utils

worker_opts = [
    cfg.StrOpt('export_base_path',
               default='/tmp',
               help='The path used for hosting exported disks.'),
]

CONF = cfg.CONF
CONF.register_opts(worker_opts, 'worker')

LOG = logging.getLogger(__name__)

TMP_DIRS_KEY = "__tmp_dirs"

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

    def _cleanup_task_resources(self, task_id, task_info=None):
        try:
            export_path = _get_task_export_path(task_id)
            if (not task_info or export_path not in
                    task_info.get(TMP_DIRS_KEY, [])):
                # Don't remove folder if it's needed by the dependent tasks
                if os.path.exists(export_path):
                    shutil.rmtree(export_path)
        except Exception as ex:
            # Ignore the exception
            LOG.exception(ex)

    def _remove_tmp_dirs(self, task_info):
        if task_info:
            for tmp_dir in task_info.get(TMP_DIRS_KEY, []):
                if os.path.exists(tmp_dir):
                    try:
                        shutil.rmtree(tmp_dir)
                    except Exception as ex:
                        # Ignore exception
                        LOG.exception(ex)

    def cancel_task(self, ctxt, process_id):
        try:
            p = psutil.Process(process_id)
            p.kill()
        except psutil.NoSuchProcess:
            LOG.info("Task process not found: %s", process_id)

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
        try:
            new_task_info = self._exec_task_process(
                ctxt, task_id, task_type, origin, destination,
                instance, task_info)

            if new_task_info:
                LOG.info("Task info: %s", new_task_info)

            LOG.info("Task completed: %s", task_id)
            self._rpc_conductor_client.task_completed(ctxt, task_id,
                                                      new_task_info)

            self._cleanup_task_resources(task_id, new_task_info)
        except Exception as ex:
            LOG.exception(ex)
            self._rpc_conductor_client.set_task_error(ctxt, task_id, str(ex))

            self._cleanup_task_resources(task_id)
        finally:
            self._remove_tmp_dirs(task_info)


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

        if task_type == constants.TASK_TYPE_EXPORT_INSTANCE:
            provider_type = constants.PROVIDER_TYPE_EXPORT
            data = origin
        elif task_type == constants.TASK_TYPE_IMPORT_INSTANCE:
            provider_type = constants.PROVIDER_TYPE_IMPORT
            data = destination
        else:
            raise exception.NotFound(
                "Unknown task type: %s" % task_type)

        event_handler = _ConductorProviderEventHandler(ctxt, task_id)
        provider = factory.get_provider(data["type"], provider_type,
                                        event_handler)

        connection_info = data.get("connection_info", {})
        target_environment = data.get("target_environment", {})

        secret_ref = connection_info.get("secret_ref")
        if secret_ref:
            LOG.info("Retrieving connection info from secret: %s", secret_ref)
            connection_info = secrets.get_secret(ctxt, secret_ref)

        if provider_type == constants.PROVIDER_TYPE_EXPORT:
            export_path = _get_task_export_path(task_id, create=True)

            result = provider.export_instance(ctxt, connection_info, instance,
                                              export_path)
            result[TMP_DIRS_KEY] = [export_path]
        else:
            result = provider.import_instance(ctxt, connection_info,
                                              target_environment, instance,
                                              task_info)
        mp_q.put(result)
    except Exception as ex:
        mp_q.put(str(ex))
        LOG.exception(ex)
    finally:
        # Signal the log event handler that there are no more events
        mp_log_q.put(None)
