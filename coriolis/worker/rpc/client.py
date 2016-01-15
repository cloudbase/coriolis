from oslo_config import cfg
import oslo_messaging as messaging

CONF = cfg.CONF
CONF.import_opt("messaging_transport_url", "coriolis.service")

VERSION = "1.0"


class WorkerClient(object):
    def __init__(self):
        target = messaging.Target(topic='coriolis_worker', version=VERSION)
        transport = messaging.get_transport(
            cfg.CONF, CONF.messaging_transport_url)
        self._client = messaging.RPCClient(transport, target)

    def begin_export_instance(self, ctxt, operation_id, origin, instance):
        self._client.cast(
            ctxt, 'export_instance', operation_id=operation_id, origin=origin,
            instance=instance)

    def begin_import_instance(self, ctxt, server, operation_id, destination,
                              instance, export_info):
        # Needs to be executed on the same server
        cctxt = self._client.prepare(server=server)
        cctxt.cast(
            ctxt, 'import_instance', operation_id=operation_id,
            destination=destination, instance=instance,
            export_info=export_info)

    def update_migration_status(self, ctx, operation_id, status):
        self._client.call(ctxt, "update_migration_status", status=status)
