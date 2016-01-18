import os
import multiprocessing
import queue
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
        mp_q = multiprocessing.Queue()
        p = multiprocessing.Process(target=target, args=(args + (mp_q,)))

        p.start()
        LOG.info("Task process started: %s", task_id)
        self._rpc_conductor_client.set_task_host(
            ctxt, task_id, self._server, p.pid)

        p.join()

        try:
            result = mp_q.get(False)
        except queue.Empty:
            raise Exception("Task process terminated")

        if isinstance(result, str):
            raise exception.TaskProcessException(result)
        return result

    def export_instance(self, ctxt, task_id, origin, instance):
        def _export_instance(export_provider, connection_info,
                             instance, export_path, mp_q):
            try:
                vm_info = export_provider.export_instance(
                    connection_info, instance, export_path)
                mp_q.put(vm_info)
            except Exception as ex:
                mp_q.put(utils.get_exception_details())
                LOG.exception(ex)

        try:
            export_provider = factory.get_provider(
                origin["type"], constants.PROVIDER_TYPE_EXPORT)
            export_path = self._get_task_export_path(task_id)
            if not os.path.exists(export_path):
                os.makedirs(export_path)

            vm_info = self._exec_task_process(
                ctxt, task_id, _export_instance,
                (export_provider, origin["connection_info"],
                 instance, export_path))

            LOG.info("Exported VM: %s" % vm_info)
            self._rpc_conductor_client.export_completed(
                ctxt, task_id, vm_info)
        except Exception as ex:
            LOG.exception(ex)
            if isinstance(ex, exception.TaskProcessException):
                stack_trace = ex.message
            else:
                stack_trace = utils.get_exception_details()

            self._cleanup_task_resources(task_id)
            self._rpc_conductor_client.set_task_error(
                ctxt, task_id, stack_trace)

    def import_instance(self, ctxt, task_id, destination, instance,
                        export_info):
        def _import_instance(import_provider, connection_info,
                             target_environment, instance, export_info, mp_q):
            try:
                import_provider.import_instance(
                    connection_info, target_environment, instance, export_info)
                mp_q.put(None)
            except Exception as ex:
                mp_q.put(utils.get_exception_details())
                LOG.exception(ex)

        try:
            import_provider = factory.get_provider(
                destination["type"], constants.PROVIDER_TYPE_IMPORT)

            self._exec_task_process(
                ctxt, task_id, _import_instance,
                (import_provider, destination["connection_info"],
                 destination["target_environment"],
                 instance, export_info))

            LOG.info("Import completed")
            self._rpc_conductor_client.import_completed(ctxt, task_id)
        except Exception as ex:
            LOG.exception(ex)
            if isinstance(ex, exception.TaskProcessException):
                stack_trace = ex.message
            else:
                stack_trace = utils.get_exception_details()

            self._rpc_conductor_client.set_task_error(
                ctxt, task_id, stack_trace)
        finally:
            self._cleanup_task_resources(task_id)
