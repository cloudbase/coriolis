# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.migrations import api as migrations_module
from coriolis.tests import test_base


class APITestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Migrations API."""

    def setUp(self):
        super(APITestCase, self).setUp()
        self.api = migrations_module.API()
        self.rpc_client = mock.MagicMock()
        self.api._rpc_client = self.rpc_client
        self.ctxt = mock.sentinel.ctxt
        self.migration_id = mock.sentinel.migration_id

    def test_migrate_instances(self):
        origin_endpoint_id = mock.sentinel.origin_endpoint_id
        destination_endpoint_id = mock.sentinel.destination_endpoint_id
        origin_minion_pool_id = mock.sentinel.origin_minion_pool_id
        destination_minion_pool_id = mock.sentinel.destination_minion_pool_id
        instance_osmorphing_minion_pool_mappings = (
            mock.sentinel.instance_osmorphing_minion_pool_mappings)
        source_environment = mock.sentinel.source_environment
        destination_environment = mock.sentinel.destination_environment
        instances = mock.sentinel.instances
        network_map = mock.sentinel.network_map
        storage_mappings = mock.sentinel.storage_mappings
        replication_count = mock.sentinel.replication_count
        shutdown_instances = mock.sentinel.shutdown_instances

        result = self.api.migrate_instances(
            self.ctxt, origin_endpoint_id, destination_endpoint_id,
            origin_minion_pool_id, destination_minion_pool_id,
            instance_osmorphing_minion_pool_mappings, source_environment,
            destination_environment, instances, network_map, storage_mappings,
            replication_count, shutdown_instances)
        self.rpc_client.migrate_instances.assert_called_once_with(
            self.ctxt, origin_endpoint_id, destination_endpoint_id,
            origin_minion_pool_id, destination_minion_pool_id,
            instance_osmorphing_minion_pool_mappings, source_environment,
            destination_environment, instances, network_map, storage_mappings,
            replication_count, shutdown_instances=shutdown_instances,
            notes=None, skip_os_morphing=False, user_scripts=None)
        self.assertEqual(result,
                         self.rpc_client.migrate_instances.return_value)

    def test_deploy_replica_instances(self):
        replica_id = mock.sentinel.replica_id
        instance_osmorphing_minion_pool_mappings = (
            mock.sentinel.instance_osmorphing_minion_pool_mappings)

        result = self.api.deploy_replica_instances(
            self.ctxt, replica_id, instance_osmorphing_minion_pool_mappings)

        self.rpc_client.deploy_replica_instances.assert_called_once_with(
            self.ctxt, replica_id,
            instance_osmorphing_minion_pool_mappings=(
                instance_osmorphing_minion_pool_mappings),
            clone_disks=False, force=False, skip_os_morphing=False,
            user_scripts=None)
        self.assertEqual(result,
                         self.rpc_client.deploy_replica_instances.return_value)

    def test_delete(self):
        self.api.delete(self.ctxt, self.migration_id)
        self.rpc_client.delete_migration.assert_called_once_with(
            self.ctxt, self.migration_id)

    def test_cancel(self):
        self.api.cancel(self.ctxt, self.migration_id, True)
        self.rpc_client.cancel_migration.assert_called_once_with(
            self.ctxt, self.migration_id, True)

    def test_get_migrations(self):
        result = self.api.get_migrations(self.ctxt, include_tasks=False,
                                         include_task_info=False)

        self.rpc_client.get_migrations.assert_called_once_with(
            self.ctxt, False, include_task_info=False)
        self.assertEqual(result, self.rpc_client.get_migrations.return_value)

    def test_get_migration(self):
        result = self.api.get_migration(self.ctxt, self.migration_id,
                                        include_task_info=False)

        self.rpc_client.get_migration.assert_called_once_with(
            self.ctxt, self.migration_id, include_task_info=False)
        self.assertEqual(result, self.rpc_client.get_migration.return_value)
