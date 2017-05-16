# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def migrate_instances(self, ctxt, origin_endpoint_id,
                          destination_endpoint_id, destination_environment,
                          instances, notes=None):
        return self._rpc_client.migrate_instances(
            ctxt, origin_endpoint_id, destination_endpoint_id,
            destination_environment, instances, notes)

    def deploy_replica_instances(self, ctxt, replica_id, clone_disks=False,
                                 force=False):
        return self._rpc_client.deploy_replica_instances(
            ctxt, replica_id, clone_disks, force)

    def delete(self, ctxt, migration_id):
        self._rpc_client.delete_migration(ctxt, migration_id)

    def cancel(self, ctxt, migration_id, force):
        self._rpc_client.cancel_migration(ctxt, migration_id, force)

    def get_migrations(self, ctxt, include_tasks=False):
        return self._rpc_client.get_migrations(ctxt, include_tasks)

    def get_migration(self, ctxt, migration_id):
        return self._rpc_client.get_migration(ctxt, migration_id)
