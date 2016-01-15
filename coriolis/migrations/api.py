from coriolis.conductor.rpc import client as rpc_client
from coriolis import context


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def start(self, origin, destination, instances):
        ctxt = context.CoriolisContext()
        self._rpc_client.begin_migrate_instances(
            ctxt.to_dict(), origin, destination, instances)

    def get_migrations(self):
        ctxt = context.CoriolisContext()
        return self._rpc_client.get_migrations(ctxt.to_dict())

    def get_migration(self, migration_id):
        ctxt = context.CoriolisContext()
        return self._rpc_client.get_migration(ctxt.to_dict(), migration_id)
