from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def start(self, ctxt, origin, destination, instances):
        self._rpc_client.begin_migrate_instances(
            ctxt.to_dict(), origin, destination, instances)

    def stop(ctxt, self, migration_id):
        self._rpc_client.stop_instances_migration(
            ctxt.to_dict(), migration_id)

    def get_migrations(self, ctxt):
        return self._rpc_client.get_migrations(ctxt.to_dict())

    def get_migration(self, ctxt, migration_id):
        return self._rpc_client.get_migration(ctxt.to_dict(), migration_id)
