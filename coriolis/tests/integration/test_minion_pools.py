# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Integration tests for the Minion Pool lifecycle API.

Exercises minion-pool operations via the Coriolis REST API:
- CRUD without allocation (create skip_allocation=True, list, get, update,
  delete)
- Full allocation lifecycle (allocate -> wait for ALLOCATED -> refresh ->
  deallocate -> wait for DEALLOCATED -> delete)
"""

import time

from oslo_config import cfg

from coriolis import constants
from coriolis.db import api as db_api
from coriolis.tests.integration import base

CONF = cfg.CONF


class MinionPoolLifecycleTest(base.MinionPoolTestBase):

    def setUp(self):
        super().setUp()

        self._endpoint = self._create_endpoint(
            name="pool-dst",
            endpoint_type=self._imp_platform,
            connection_info=self._imp_conn_info,
        )

    def _wait_for_machine_status(self, pool_id, status, timeout=120):
        """Poll the DB until the pool's single machine reaches *status*."""
        ctxt = self._get_db_context()
        deadline = time.monotonic() + timeout
        machine = None

        while time.monotonic() < deadline:
            pool = db_api.get_minion_pool(ctxt, pool_id, include_machines=True)
            machine = pool.minion_machines[0]
            if machine.allocation_status == status:
                return machine
            time.sleep(1)

        self.fail(
            "Minion pool machine '%s' did not reach status '%s' within %ds "
            "(last status: %s)"
            % (pool_id, status, timeout, machine.allocation_status))

    def test_minion_pool_crud(self):
        # Create
        pool = self._create_pool(self._endpoint.id)

        self.assertEqual("test-pool", pool.name)
        self.assertEqual(
            constants.MINION_POOL_STATUS_DEALLOCATED, pool.status)

        # List
        pools = self._client.minion_pools.list()

        pool_ids = [p.id for p in pools]
        self.assertIn(pool.id, pool_ids)

        # Get
        fetched = self._client.minion_pools.get(pool.id)

        self.assertEqual(pool.id, fetched.id)
        self.assertEqual("test-pool", fetched.name)

        # Update
        updates = {
            "notes": "updated notes",
            "environment_options": self._pool_env,
        }
        updated = self._client.minion_pools.update(pool.id, updates)

        self.assertEqual("updated notes", updated.notes)
        self.assertEqual(self._pool_env, updated.environment_options)

        # Delete
        self._safe_delete_pool(pool.id)

        pools = self._client.minion_pools.list()
        self.assertNotIn(pool.id, [p.id for p in pools])

    def test_allocate_deallocate(self):
        pool = self._create_pool(self._endpoint.id)
        self.assertEqual(
            constants.MINION_POOL_STATUS_DEALLOCATED, pool.status)

        # Allocate
        self._client.minion_pools.allocate_minion_pool(pool.id)

        final = self._wait_for_pool(pool.id, base.MINION_ALLOCATED_TERMINAL)
        self.assertEqual(
            constants.MINION_POOL_STATUS_ALLOCATED,
            final.status,
            "Pool allocation ended in unexpected status '%s'" % final.status,
        )

        # Refresh: healthchecks the allocated machine and returns it to
        # AVAILABLE.
        self._client.minion_pools.refresh_minion_pool(pool.id)
        self._wait_for_machine_status(
            pool.id, constants.MINION_MACHINE_STATUS_AVAILABLE)

        # Deallocate
        self._client.minion_pools.deallocate_minion_pool(pool.id)

        final = self._wait_for_pool(pool.id, base.MINION_DEALLOCATED_TERMINAL)
        self.assertEqual(
            constants.MINION_POOL_STATUS_DEALLOCATED,
            final.status,
            "Pool deallocation ended in unexpected status '%s'" % final.status,
        )

    def test_cron_triggered_refresh(self):
        """Cron-scheduled refresh.

        Minion pools refresh periodically based on on the config option
        minion_manager.minion_pool_default_refresh_period_minutes. This test
        verifies that the refresh actually fires.
        """
        CONF.set_override(
            "minion_pool_default_refresh_period_minutes", 1,
            group="minion_manager")
        self.addCleanup(
            CONF.clear_override,
            "minion_pool_default_refresh_period_minutes",
            group="minion_manager")

        # Refresh jobs are registered at pool-creation time based on the
        # CONF value above, so the pool must be created after the override.
        pool = self._create_pool(self._endpoint.id)

        self._client.minion_pools.allocate_minion_pool(pool.id)
        self._wait_for_pool(pool.id, base.MINION_ALLOCATED_TERMINAL)

        machine = self._wait_for_machine_status(
            pool.id, constants.MINION_MACHINE_STATUS_AVAILABLE)
        baseline_updated_at = machine.updated_at

        ctxt = self._get_db_context()
        deadline = time.monotonic() + 120
        refreshed = False
        while time.monotonic() < deadline:
            pool = db_api.get_minion_pool(ctxt, pool.id, include_machines=True)
            m = pool.minion_machines[0]
            status = m.allocation_status
            if (status == constants.MINION_MACHINE_STATUS_AVAILABLE and
                    m.updated_at != baseline_updated_at):
                refreshed = True
                break
            time.sleep(2)

        self.assertTrue(
            refreshed,
            "Minion pool machine '%s' was not refreshed by the automatic "
            "cron job in time" % pool.id,
        )
