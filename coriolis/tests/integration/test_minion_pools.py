# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Integration tests for the Minion Pool lifecycle API.

Exercises minion-pool operations via the Coriolis REST API:
- CRUD without allocation (create skip_allocation=True, list, get, update,
  delete)
- Full allocation lifecycle (allocate -> wait for ALLOCATED -> refresh ->
  deallocate -> wait for DEALLOCATED -> delete)
"""

from coriolis import constants
from coriolis.tests.integration import base


class MinionPoolLifecycleTest(base.MinionPoolTestBase):
    def setUp(self):
        super().setUp()

        self._endpoint = self._create_endpoint(
            name="pool-dst",
            endpoint_type=self._imp_platform,
            connection_info=self._imp_conn_info,
        )

    def test_minion_pool_crud(self):
        # Create
        pool = self._create_pool(self._endpoint.id)

        self.assertEqual("test-pool", pool.name)
        self.assertEqual(constants.MINION_POOL_STATUS_DEALLOCATED, pool.status)

        # List
        pools = self._client.minion_pools.list()

        pool_ids = [p.id for p in pools]
        self.assertIn(pool.id, pool_ids)

        # Get
        fetched = self._client.minion_pools.get(pool.id)

        self.assertEqual(pool.id, fetched.id)
        self.assertEqual("test-pool", fetched.name)

        # Update
        updated = self._client.minion_pools.update(pool.id, {"notes": "updated notes"})

        self.assertEqual("updated notes", updated.notes)

        # Delete
        self._safe_delete_pool(pool.id)

        pools = self._client.minion_pools.list()
        self.assertNotIn(pool.id, [p.id for p in pools])

    def test_allocate_deallocate(self):
        pool = self._create_pool(self._endpoint.id)
        self.assertEqual(constants.MINION_POOL_STATUS_DEALLOCATED, pool.status)

        # Allocate
        self._client.minion_pools.allocate_minion_pool(pool.id)

        final = self._wait_for_pool(pool.id, base.MINION_ALLOCATED_TERMINAL)
        self.assertEqual(
            constants.MINION_POOL_STATUS_ALLOCATED,
            final.status,
            "Pool allocation ended in unexpected status '%s'" % final.status,
        )

        # Deallocate
        self._client.minion_pools.deallocate_minion_pool(pool.id)

        final = self._wait_for_pool(pool.id, base.MINION_DEALLOCATED_TERMINAL)
        self.assertEqual(
            constants.MINION_POOL_STATUS_DEALLOCATED,
            final.status,
            "Pool deallocation ended in unexpected status '%s'" % final.status,
        )
