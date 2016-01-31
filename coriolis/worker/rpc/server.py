import os
import multiprocessing
import shutil

from oslo_config import cfg
from oslo_log import log as logging
import psutil

from coriolis.conductor.rpc import client as rpc_conductor_client
from coriolis import constants
from coriolis import exception
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

VERSION = "1.0"


class WorkerServerEndpoint(object):
    def __init__(self):
        self._server = utils.get_hostname()
        self._rpc_conductor_client = rpc_conductor_client.ConductorClient()

    def _get_task_export_path(self, task_id):
        return os.path.join(CONF.worker.export_base_path, task_id)

    def _cleanup_task_resources(self, task_id):
        try:
            export_path = self._get_task_export_path(task_id)
            if os.path.exists(export_path):
                shutil.rmtree(export_path)
        except Exception as ex:
            # Swallow the exception
            LOG.exception(ex)

    def stop_task(self, ctxt, process_id):
        try:
            p = psutil.Process(process_id)
            p.kill()
        except psutil.NoSuchProcess:
            LOG.info("Task process not found: %s", process_id)

    def _exec_task_process(self, ctxt, task_id, target, args):
        mp_ctx = multiprocessing.get_context('spawn')
        mp_q = mp_ctx.Queue()
        p = mp_ctx.Process(target=target, args=(args + (mp_q,)))

        p.start()
        LOG.info("Task process started: %s", task_id)
        self._rpc_conductor_client.set_task_host(
            ctxt, task_id, self._server, p.pid)

        p.join()

        if mp_q.empty():
            raise Exception("Task process terminated")
        result = mp_q.get(False)

        if isinstance(result, str):
            raise exception.TaskProcessException(result)
        return result

    def exec_task(self, ctxt, task_id, task_type, origin, destination,
                  instance, task_info):
        try:
            task_info = None

            if task_type == constants.TASK_TYPE_EXPORT_INSTANCE:
                provider = factory.get_provider(
                    origin["type"], constants.PROVIDER_TYPE_EXPORT)
                export_path = self._get_task_export_path(task_id)
                if not os.path.exists(export_path):
                    os.makedirs(export_path)

                task_info = self._exec_task_process(
                    ctxt, task_id, _export_instance,
                    (provider, origin["connection_info"],
                     instance, export_path))

            elif task_type == constants.TASK_TYPE_IMPORT_INSTANCE:
                provider = factory.get_provider(
                    destination["type"], constants.PROVIDER_TYPE_IMPORT)

                self._exec_task_process(
                    ctxt, task_id, _import_instance,
                    (provider, destination["connection_info"],
                     destination["target_environment"],
                     instance, task_info))
            else:
                raise Exception("Unknown task type: %s" % task_type)

            LOG.info("Task completed: %s", task_id)
            LOG.info("Task info: %s", task_info)
            self._rpc_conductor_client.task_completed(ctxt, task_id, task_info)

            # Resources are needed by dependent import tasks
            if task_type != constants.TASK_TYPE_EXPORT_INSTANCE:
                self._cleanup_task_resources(task_id)
        except Exception as ex:
            LOG.exception(ex)
            self._rpc_conductor_client.set_task_error(ctxt, task_id, str(ex))

            self._cleanup_task_resources(task_id)


def _export_instance(export_provider, connection_info,
                     instance, export_path, mp_q):
    try:
        # Setting up logging, needed since this is a new process
        utils.setup_logging()

        vm_info = export_provider.export_instance(
            connection_info, instance, export_path)
        mp_q.put(vm_info)
    except Exception as ex:
        mp_q.put(str(ex))
        LOG.exception(ex)


def _import_instance(import_provider, connection_info,
                     target_environment, instance, export_info, mp_q):
    try:
        # Setting up logging, needed since this is a new process
        utils.setup_logging()

        import_provider.import_instance(
            connection_info, target_environment, instance, export_info)
        mp_q.put(None)
    except Exception as ex:
        mp_q.put(str(ex))
        LOG.exception(ex)
