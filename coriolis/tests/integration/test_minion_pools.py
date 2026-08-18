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


class MinionPoolLifecycleTestMixin:
    _MINION_PLATFORM = None

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
            "(last status: %s)" % (pool_id, status, timeout, machine.allocation_status)
        )

    def test_minion_pool_crud(self):
        # Create
        pool = self._create_pool(self._endpoint.id)

        self.assertEqual("test-pool", pool.name)
        self.assertEqual(self._MINION_PLATFORM, pool.platform)
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
        self.assertEqual(constants.MINION_POOL_STATUS_DEALLOCATED, pool.status)

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
            pool.id, constants.MINION_MACHINE_STATUS_AVAILABLE
        )

        # Deallocate
        self._client.minion_pools.deallocate_minion_pool(pool.id)

        final = self._wait_for_pool(pool.id, base.MINION_DEALLOCATED_TERMINAL)
        self.assertEqual(
            constants.MINION_POOL_STATUS_DEALLOCATED,
            final.status,
            "Pool deallocation ended in unexpected status '%s'" % final.status,
        )


class MinionPoolLifecycleTests(
    MinionPoolLifecycleTestMixin, base.DestinationMinionPoolTestBase
):
    _MINION_PLATFORM = constants.PROVIDER_PLATFORM_DESTINATION

    def setUp(self):
        super().setUp()

        self._endpoint = self._create_endpoint(
            name="pool-dst",
            endpoint_type=self._imp_platform,
            connection_info=self._imp_conn_info,
        )
        self._pool_env = self._imp_pool_env

    def test_cron_triggered_refresh(self):
        """Cron-scheduled refresh.

        Minion pools refresh periodically based on on the config option
        minion_manager.minion_pool_default_refresh_period_minutes. This test
        verifies that the refresh actually fires.
        """
        CONF.set_override(
            "minion_pool_default_refresh_period_minutes", 1, group="minion_manager"
        )
        self.addCleanup(
            CONF.clear_override,
            "minion_pool_default_refresh_period_minutes",
            group="minion_manager",
        )

        # Refresh jobs are registered at pool-creation time based on the
        # CONF value above, so the pool must be created after the override.
        pool = self._create_pool(self._endpoint.id)

        self._client.minion_pools.allocate_minion_pool(pool.id)
        self._wait_for_pool(pool.id, base.MINION_ALLOCATED_TERMINAL)

        machine = self._wait_for_machine_status(
            pool.id, constants.MINION_MACHINE_STATUS_AVAILABLE
        )
        baseline_updated_at = machine.updated_at

        ctxt = self._get_db_context()
        deadline = time.monotonic() + 120
        refreshed = False
        while time.monotonic() < deadline:
            pool = db_api.get_minion_pool(ctxt, pool.id, include_machines=True)
            m = pool.minion_machines[0]
            status = m.allocation_status
            if (
                status == constants.MINION_MACHINE_STATUS_AVAILABLE
                and m.updated_at != baseline_updated_at
            ):
                refreshed = True
                break
            time.sleep(2)

        self.assertTrue(
            refreshed,
            "Minion pool machine '%s' was not refreshed by the automatic "
            "cron job in time" % pool.id,
        )


class SourceMinionPoolLifecycleTests(
    MinionPoolLifecycleTestMixin, base.SourceMinionPoolTestBase
):
    _MINION_PLATFORM = constants.PROVIDER_PLATFORM_SOURCE

    def setUp(self):
        super().setUp()

        self._endpoint = self._create_endpoint(
            name="pool-src",
            endpoint_type=self._exp_platform,
            connection_info=self._exp_conn_info,
        )
        self._pool_env = self._exp_pool_env

    def _create_pool(self, endpoint_id, **kwargs):
        return super()._create_pool(
            endpoint_id, platform=constants.PROVIDER_PLATFORM_SOURCE, **kwargs
        )


class _MinionPoolPowerCycleTestMixin:
    """Transfer that reuses pool machines across a power cycle.

    The pool allows up to 2 machines (minimum 1) with a tiny idle time and the
    "poweroff" retention strategy. Two separate transfers are executed concurrently;
    the second execution finds the pre-existing minimum machine already reserved by the
    first and allocates a brand new one instead. Once both go idle, refreshing the pool
    powers the excess one off. Re-running both transfers concurrently then reuses both
    machines, powering the idled-off one back on before healthchecking and reusing it.

    Subclasses select which side's pool gets exercised by overriding the ``_pool_id``
    property.
    """

    _POOL_MAXIMUM_MINIONS = 2
    _POOL_MINION_MAX_IDLE_TIME = 1
    _POOL_MINION_RETENTION_STRATEGY = (
        constants.MINION_POOL_MACHINE_RETENTION_STRATEGY_POWEROFF
    )

    @property
    def _pool_id(self):
        raise NotImplementedError

    def setUp(self):
        super().setUp()

        # A second transfer, independent from self._transfer (created by
        # ReplicaIntegrationTestBase.setUp). Running it concurrently with self._transfer
        # forces the pool to allocate a second machine, since the first is already
        # reserved by self._transfer's execution.
        self._pool_transfer_b = self._create_transfer(
            self._src_endpoint.id,
            self._dst_endpoint.id,
            instances=[self._instance_name],
            source_environment=self._transfer._info["source_environment"],
            destination_minion_pool_id=self._dst_pool_id,
            origin_minion_pool_id=self._src_pool_id,
        )

    def _execute_concurrently_and_wait(self, transfer_ids, timeout=600):
        """Start one execution per transfer id before waiting on any."""
        executions = [
            self._client.transfer_executions.create(
                transfer_id, shutdown_instances=False
            )
            for transfer_id in transfer_ids
        ]
        for execution in executions:
            self.assertExecutionCompleted(execution.id, timeout=timeout)

    def _wait_for_power_status(self, status, timeout=120):
        """Poll until one of the pool's machines reaches *status*."""
        ctxt = self._get_db_context()
        deadline = time.monotonic() + timeout
        machines = []

        while time.monotonic() < deadline:
            pool = db_api.get_minion_pool(ctxt, self._pool_id, include_machines=True)
            machines = pool.minion_machines
            if any(m.power_status == status for m in machines):
                return machines
            time.sleep(1)

        self.fail(
            "No minion machine of pool '%s' reached power status '%s' within %ds "
            "(last statuses: %s)"
            % (
                self._pool_id,
                status,
                timeout,
                [m.power_status for m in machines],
            )
        )

    def test_transfer_after_pool_machine_power_cycle(self):
        transfer_ids = [self._transfer.id, self._pool_transfer_b.id]

        # Concurrently executing both transfers forces the second one to allocate a new
        # machine, since the pre-existing minimum one is already reserved by the first
        # (up to the pool's maximum of 2).
        self._execute_concurrently_and_wait(transfer_ids)

        pool = db_api.get_minion_pool(
            self._get_db_context(), self._pool_id, include_machines=True
        )
        self.assertEqual(2, len(pool.minion_machines))
        provider_properties_before = {
            machine.id: machine.provider_properties for machine in pool.minion_machines
        }

        # Let both machines' idle time expire, then refresh the pool: since their count
        # exceeds the pool minimum of 1, the excess one gets powered off.
        time.sleep(self._POOL_MINION_MAX_IDLE_TIME + 1)
        self._client.minion_pools.refresh_minion_pool(self._pool_id)
        self._wait_for_power_status(constants.MINION_MACHINE_POWER_STATUS_POWERED_OFF)

        # Re-running both transfers concurrently reuses both machines, powering the
        # idled-off one back on before healthchecking and reusing it.
        self._execute_concurrently_and_wait(transfer_ids)

        # The power-cycled machine must genuinely have been reused, not silently deleted
        # and recreated from scratch by the healthcheck-failure fallback (which would
        # defeat the whole point of the "poweroff" retention strategy)
        pool = db_api.get_minion_pool(
            self._get_db_context(), self._pool_id, include_machines=True
        )
        self.assertEqual(2, len(pool.minion_machines))
        for machine in pool.minion_machines:
            self.assertEqual(
                provider_properties_before[machine.id],
                machine.provider_properties,
                "Minion machine '%s' provider properties changed across the power "
                "cycle; it was likely deleted and recreated instead of reused."
                % machine.id,
            )


class MinionPoolPowerCycleTransferTest(
    _MinionPoolPowerCycleTestMixin, base.MinionPoolReplicaTestBase
):
    """Power-cycle test exercising a destination minion pool."""

    @property
    def _pool_id(self):
        return self._dst_pool_id


class SourceMinionPoolPowerCycleTransferTest(
    _MinionPoolPowerCycleTestMixin, base.SourceMinionPoolReplicaTestBase
):
    """Power-cycle test exercising a source minion pool."""

    @property
    def _pool_id(self):
        return self._src_pool_id
