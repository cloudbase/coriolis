import os

from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging

from coriolis.conductor.rpc import client as rpc_conductor_client
from coriolis import constants
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

    def _get_operation_export_path(self, operation_id):
        path = os.path.join(CONF.worker.export_base_path, operation_id)
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def export_instance(self, ctxt, operation_id, origin, instance):
        self._rpc_conductor_client.set_operation_host(
            ctxt, operation_id, self._server)

        try:
            export_provider = factory.get_provider(
                origin["type"], constants.PROVIDER_TYPE_EXPORT)
            export_path = self._get_operation_export_path(operation_id)
            vm_info = export_provider.export_instance(
                origin["connection_info"], instance, export_path)
            LOG.info("Exported VM: %s" % vm_info)

            self._rpc_conductor_client.export_completed(
                ctxt, operation_id, vm_info)
        except Exception as ex:
            LOG.exception(ex)
            # TODO: set error state
            # self._rpc_conductor_client.set_operation_error(ctxt,
            # operation_id, ex)

    def import_instance(self, ctxt, operation_id, destination, instance,
                        export_info):
        self._rpc_conductor_client.set_operation_host(
            ctxt, operation_id, self._server)

        try:
            import_provider = factory.get_provider(
                destination["type"], constants.PROVIDER_TYPE_IMPORT)
            import_provider.import_instance(
                destination["connection_info"],
                destination["target_environment"],
                instance, export_info)

            self._rpc_conductor_client.import_completed(ctxt, operation_id)
        except Exception as ex:
            LOG.exception(ex)
            # TODO: set error state
            # self._rpc_conductor_client.set_operation_error(
            # ctxt, operation_id, ex)
