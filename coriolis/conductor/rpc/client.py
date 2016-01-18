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

    def stop_instances_migration(self, ctxt, migration_id):
        self._client.call(
            ctxt, 'stop_instances_migration', migration_id=migration_id)

    def set_task_host(self, ctxt, task_id, host, process_id):
        self._client.call(
            ctxt, 'set_task_host', task_id=task_id, host=host,
            process_id=process_id)

    def export_completed(self, ctxt, task_id, export_info):
        self._client.call(
            ctxt, 'export_completed', task_id=task_id,
            export_info=export_info)

    def import_completed(self, ctxt, task_id):
        self._client.call(ctxt, 'import_completed', task_id=task_id)

    def set_task_error(self, ctxt, task_id, exception_details):
        self._client.call(ctxt, 'set_task_error', task_id=task_id,
                          exception_details=exception_details)
