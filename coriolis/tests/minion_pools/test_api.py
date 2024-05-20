# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.minion_pools import api as minion_pools_module
from coriolis.tests import test_base


class APITestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Minion Pools API."""

    def setUp(self):
        super(APITestCase, self).setUp()
        self.api = minion_pools_module.API()
        self.rpc_client = mock.MagicMock()
        self.api._rpc_client = self.rpc_client
        self.ctxt = mock.sentinel.ctxt
        self.minion_pool_id = mock.sentinel.minion_pool_id

    def test_create(self):
        name = mock.sentinel.name
        endpoint_id = mock.sentinel.endpoint_id
        pool_platform = mock.sentinel.pool_platform
        pool_os_type = mock.sentinel.pool_os_type
        environment_options = mock.sentinel.environment_options
        minimum_minions = mock.sentinel.minimum_minions
        maximum_minions = mock.sentinel.maximum_minions
        minion_max_idle_time = mock.sentinel.minion_max_idle_time
        minion_retention_strategy = mock.sentinel.minion_retention_strategy

        result = self.api.create(
            self.ctxt, name, endpoint_id, pool_platform, pool_os_type,
            environment_options, minimum_minions, maximum_minions,
            minion_max_idle_time, minion_retention_strategy)

        self.rpc_client.create_minion_pool.assert_called_once_with(
            self.ctxt, name, endpoint_id, pool_platform, pool_os_type,
            environment_options, minimum_minions, maximum_minions,
            minion_max_idle_time, minion_retention_strategy, notes=None,
            skip_allocation=False)
        self.assertEqual(result,
                         self.rpc_client.create_minion_pool.return_value)

    def test_update(self):
        updated_values = mock.sentinel.updated_values

        result = self.api.update(self.ctxt, self.minion_pool_id,
                                 updated_values)

        self.rpc_client.update_minion_pool.assert_called_once_with(
            self.ctxt, self.minion_pool_id, updated_values=updated_values)
        self.assertEqual(result,
                         self.rpc_client.update_minion_pool.return_value)

    def test_delete(self):
        self.api.delete(self.ctxt, self.minion_pool_id)
        self.rpc_client.delete_minion_pool.assert_called_once_with(
            self.ctxt, self.minion_pool_id)

    def test_get_minion_pools(self):
        result = self.api.get_minion_pools(self.ctxt)

        self.rpc_client.get_minion_pools.assert_called_once_with(self.ctxt)
        self.assertEqual(result, self.rpc_client.get_minion_pools.return_value)

    def test_get_minion_pool(self):
        result = self.api.get_minion_pool(self.ctxt, self.minion_pool_id)

        self.rpc_client.get_minion_pool.assert_called_once_with(
            self.ctxt, self.minion_pool_id)
        self.assertEqual(result, self.rpc_client.get_minion_pool.return_value)

    def test_allocate_minion_pool(self):
        result = self.api.allocate_minion_pool(self.ctxt, self.minion_pool_id)

        self.rpc_client.allocate_minion_pool.assert_called_once_with(
            self.ctxt, self.minion_pool_id)
        self.assertEqual(result,
                         self.rpc_client.allocate_minion_pool.return_value)

    def test_refresh_minion_pool(self):
        result = self.api.refresh_minion_pool(self.ctxt, self.minion_pool_id)

        self.rpc_client.refresh_minion_pool.assert_called_once_with(
            self.ctxt, self.minion_pool_id)
        self.assertEqual(result,
                         self.rpc_client.refresh_minion_pool.return_value)

    def test_deallocate_minion_pool(self):
        result = self.api.deallocate_minion_pool(
            self.ctxt, self.minion_pool_id, force=False)

        self.rpc_client.deallocate_minion_pool.assert_called_once_with(
            self.ctxt, self.minion_pool_id, force=False)
        self.assertEqual(result,
                         self.rpc_client.deallocate_minion_pool.return_value)
