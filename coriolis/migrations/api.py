from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def start(self, ctxt, origin, destination, instances):
        return self._rpc_client.begin_migrate_instances(
            ctxt, origin, destination, instances)

    def stop(ctxt, self, migration_id):
        self._rpc_client.stop_instances_migration(ctxt, migration_id)

    def get_migrations(self, ctxt):
        return self._rpc_client.get_migrations(ctxt)

    def get_migration(self, ctxt, migration_id):
        return self._rpc_client.get_migration(ctxt, migration_id)
