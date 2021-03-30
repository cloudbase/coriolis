# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient(
            reset_transport_on_call=False)

    def migrate_instances(self, ctxt, origin_endpoint_id,
                          destination_endpoint_id, origin_minion_pool_id,
                          destination_minion_pool_id,
                          instance_osmorphing_minion_pool_mappings,
                          source_environment, destination_environment,
                          instances, network_map, storage_mappings,
                          replication_count,
                          shutdown_instances, notes=None,
                          skip_os_morphing=False, user_scripts=None):
        return self._rpc_client.migrate_instances(
            ctxt, origin_endpoint_id, destination_endpoint_id,
            origin_minion_pool_id, destination_minion_pool_id,
            instance_osmorphing_minion_pool_mappings, source_environment,
            destination_environment, instances, network_map,
            storage_mappings, replication_count,
            shutdown_instances=shutdown_instances,
            notes=notes, skip_os_morphing=skip_os_morphing,
            user_scripts=user_scripts)

    def deploy_replica_instances(self, ctxt, replica_id,
                                 instance_osmorphing_minion_pool_mappings,
                                 clone_disks=False, force=False,
                                 skip_os_morphing=False, user_scripts=None):
        return self._rpc_client.deploy_replica_instances(
            ctxt, replica_id, instance_osmorphing_minion_pool_mappings=(
                instance_osmorphing_minion_pool_mappings),
            clone_disks=clone_disks, force=force,
            skip_os_morphing=skip_os_morphing,
            user_scripts=user_scripts)

    def delete(self, ctxt, migration_id):
        self._rpc_client.delete_migration(ctxt, migration_id)

    def cancel(self, ctxt, migration_id, force):
        self._rpc_client.cancel_migration(ctxt, migration_id, force)

    def get_migrations(self, ctxt, include_tasks=False):
        return self._rpc_client.get_migrations(ctxt, include_tasks)

    def get_migration(self, ctxt, migration_id):
        return self._rpc_client.get_migration(ctxt, migration_id)
