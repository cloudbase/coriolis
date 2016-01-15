from oslo_config import cfg
import oslo_messaging as messaging

CONF = cfg.CONF
CONF.import_opt("messaging_transport_url", "coriolis.service")

VERSION = "1.0"


class ConductorClient(object):
    def __init__(self):
        target = messaging.Target(topic='coriolis_conductor', version=VERSION)
        transport = messaging.get_transport(
            cfg.CONF, CONF.messaging_transport_url)
        self._client = messaging.RPCClient(transport, target)

    def get_migrations(self, ctxt):
        return self._client.call(ctxt, 'get_migrations')

    def get_migration(self, ctxt, migration_id):
        return self._client.call(
            ctxt, 'get_migration', migration_id=migration_id)

    def begin_migrate_instances(self, ctxt, origin, destination, instances):
        self._client.call(
            ctxt, 'migrate_instances', origin=origin, destination=destination,
            instances=instances)

    def set_operation_host(self, ctxt, operation_id, host):
        self._client.call(
            ctxt, 'set_operation_host', operation_id=operation_id, host=host)

    def export_completed(self, ctxt, operation_id, export_info):
        self._client.call(
            ctxt, 'export_completed', operation_id=operation_id,
            export_info=export_info)

    def import_completed(self, ctxt, operation_id):
        self._client.call(ctxt, 'import_completed', operation_id=operation_id)
