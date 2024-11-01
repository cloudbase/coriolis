# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.replicas import api as replicas_module
from coriolis.tests import test_base


class APITestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis API class."""

    def setUp(self):
        super(APITestCase, self).setUp()
        self.api = replicas_module.API()
        self.rpc_client = mock.MagicMock()
        self.api._rpc_client = self.rpc_client
        self.ctxt = mock.sentinel.ctxt
        self.replica_id = mock.sentinel.transfer_id

    def test_create(self):
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

        result = self.api.create(
            self.ctxt, mock.sentinel.replica_scenario,
            origin_endpoint_id, destination_endpoint_id,
            origin_minion_pool_id, destination_minion_pool_id,
            instance_osmorphing_minion_pool_mappings, source_environment,
            destination_environment, instances, network_map, storage_mappings)

        self.rpc_client.create_instances_replica.assert_called_once_with(
            self.ctxt, mock.sentinel.replica_scenario,
            origin_endpoint_id, destination_endpoint_id,
            origin_minion_pool_id, destination_minion_pool_id,
            instance_osmorphing_minion_pool_mappings, source_environment,
            destination_environment, instances, network_map, storage_mappings,
            None, None)
        self.assertEqual(result,
                         self.rpc_client.create_instances_replica.return_value)

    def test_update(self):
        updated_properties = mock.sentinel.updated_properties

        result = self.api.update(self.ctxt, self.replica_id,
                                 updated_properties)

        self.rpc_client.update_replica.assert_called_once_with(
            self.ctxt, self.replica_id, updated_properties)
        self.assertEqual(result,
                         self.rpc_client.update_replica.return_value)

    def test_delete(self):
        self.api.delete(self.ctxt, self.replica_id)
        self.rpc_client.delete_replica.assert_called_once_with(
            self.ctxt, self.replica_id)

    def test_get_replicas(self):
        result = self.api.get_replicas(
            self.ctxt, include_tasks_executions=False, include_task_info=False)

        self.rpc_client.get_replicas.assert_called_once_with(
            self.ctxt, False, include_task_info=False)
        self.assertEqual(result, self.rpc_client.get_replicas.return_value)

    def test_get_replica(self):
        result = self.api.get_replica(self.ctxt, self.replica_id)

        self.rpc_client.get_replica.assert_called_once_with(
            self.ctxt, self.replica_id, include_task_info=False)
        self.assertEqual(result, self.rpc_client.get_replica.return_value)

    def test_delete_disks(self):
        result = self.api.delete_disks(self.ctxt, self.replica_id)

        self.rpc_client.delete_replica_disks.assert_called_once_with(
            self.ctxt, self.replica_id)
        self.assertEqual(result,
                         self.rpc_client.delete_replica_disks.return_value)
