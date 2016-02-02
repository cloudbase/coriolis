import os
import multiprocessing
import shutil

from oslo_config import cfg
from oslo_log import log as logging
import psutil

from coriolis.conductor.rpc import client as rpc_conductor_client
from coriolis import constants
from coriolis import exception
from coriolis.providers import base
from coriolis.providers import factory
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


class _ConductorProgressUpdateManager(base.BaseProgressUpdateManager):
    def __init__(self, ctxt, task_id):
        self._ctxt = ctxt
        self._task_id = task_id
        self._rpc_conductor_client = rpc_conductor_client.ConductorClient()

    def progress_update(self, current_step, total_steps, message):
        LOG.info("Progress update: %s", message)
        self._rpc_conductor_client.task_progress_update(
            self._ctxt, self._task_id, current_step, total_steps, message)


class WorkerServerEndpoint(object):
    def __init__(self):
        self._server = utils.get_hostname()
        self._rpc_conductor_client = rpc_conductor_client.ConductorClient()

    def _get_task_export_path(self, task_id):
        return os.path.join(CONF.worker.export_base_path, task_id)

    def _cleanup_task_resources(self, task_id, task_info=None):
        try:
            export_path = self._get_task_export_path(task_id)
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

    def _exec_task_process(self, ctxt, task_id, target, args):
        mp_ctx = multiprocessing.get_context('spawn')
        mp_q = mp_ctx.Queue()
        p = mp_ctx.Process(target=target, args=(args + (ctxt, task_id, mp_q,)))

        p.start()
        LOG.info("Task process started: %s", task_id)
        self._rpc_conductor_client.set_task_host(
            ctxt, task_id, self._server, p.pid)

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
            new_task_info = None

            if task_type == constants.TASK_TYPE_EXPORT_INSTANCE:
                provider = factory.get_provider(
                    origin["type"], constants.PROVIDER_TYPE_EXPORT)
                export_path = self._get_task_export_path(task_id)
                if not os.path.exists(export_path):
                    os.makedirs(export_path)

                new_task_info = self._exec_task_process(
                    ctxt, task_id, _export_instance,
                    (provider, origin["connection_info"],
                     instance, export_path))

                new_task_info[TMP_DIRS_KEY] = [export_path]

            elif task_type == constants.TASK_TYPE_IMPORT_INSTANCE:
                provider = factory.get_provider(
                    destination["type"], constants.PROVIDER_TYPE_IMPORT)

                self._exec_task_process(
                    ctxt, task_id, _import_instance,
                    (provider, destination["connection_info"],
                     destination["target_environment"],
                     instance, task_info))
            else:
                raise exception.CoriolisException("Unknown task type: %s" %
                                                  task_type)

            LOG.info("Task completed: %s", task_id)
            LOG.info("Task info: %s", new_task_info)
            self._rpc_conductor_client.task_completed(ctxt, task_id,
                                                      new_task_info)

            self._cleanup_task_resources(task_id, new_task_info)
        except Exception as ex:
            LOG.exception(ex)
            self._rpc_conductor_client.set_task_error(ctxt, task_id, str(ex))

            self._cleanup_task_resources(task_id)
        finally:
            self._remove_tmp_dirs(task_info)


def _export_instance(provider, connection_info, instance, export_path,
                     ctxt, task_id, mp_q):
    try:
        # Setting up logging, needed since this is a new process
        utils.setup_logging()

        progress_update_manager = _ConductorProgressUpdateManager(ctxt,
                                                                  task_id)
        provider.set_progress_update_manager(progress_update_manager)
        vm_info = provider.export_instance(connection_info, instance,
                                           export_path)
        mp_q.put(vm_info)
    except Exception as ex:
        mp_q.put(str(ex))
        LOG.exception(ex)


def _import_instance(provider, connection_info, target_environment, instance,
                     export_info, ctxt, task_id, mp_q):
    try:
        # Setting up logging, needed since this is a new process
        utils.setup_logging()

        progress_update_manager = _ConductorProgressUpdateManager(ctxt,
                                                                  task_id)
        provider.set_progress_update_manager(progress_update_manager)
        provider.import_instance(connection_info, target_environment,
                                 instance, export_info)
        mp_q.put(None)
    except Exception as ex:
        mp_q.put(str(ex))
        LOG.exception(ex)
