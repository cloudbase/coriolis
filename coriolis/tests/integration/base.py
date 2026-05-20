# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Base test classes for the Coriolis integration tests.

Ensures the shared ``_IntegrationHarness`` is running (started once per
process) and exposes its port and paths as class attributes. The heavy
lifting - CONF overrides, DB sync, service startup - happens inside the
harness and is not repeated for each subclass.

Subclasses must be run as root.
"""

import os
import subprocess
import time
import unittest
from unittest import mock

from coriolisclient import client as coriolis_client
from keystoneauth1 import session as ks_session
from keystoneauth1 import token_endpoint
from oslo_config import cfg
from oslo_db.sqlalchemy import models
from oslo_log import log as logging
import oslo_messaging as messaging

from coriolis import constants
from coriolis import context
from coriolis.db import api as db_api
from coriolis import exception
from coriolis.providers import factory as providers_factory
from coriolis.tests.integration import harness
from coriolis.tests.integration import utils as test_utils
from coriolis.tests import test_base

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

# Statuses that represent a completed allocation attempt.
MINION_ALLOCATED_TERMINAL = {
    constants.MINION_POOL_STATUS_ALLOCATED,
    constants.MINION_POOL_STATUS_ERROR,
}

# Statuses that represent a completed deallocation attempt.
MINION_DEALLOCATED_TERMINAL = {
    constants.MINION_POOL_STATUS_DEALLOCATED,
    constants.MINION_POOL_STATUS_ERROR,
}


class CoriolisIntegrationTestBase(test_base.CoriolisBaseTestCase):
    """Base class for integration tests."""

    @classmethod
    def setUpClass(cls):
        if os.geteuid() != 0:
            raise unittest.SkipTest("Integration tests must run as root")

        super().setUpClass()
        cls._harness = harness._IntegrationHarness.get()
        cls._workdir = cls._harness.workdir
        cls._lock_path = cls._harness.lock_path
        cls._api_port = cls._harness.api_port
        cls._exp_platform = cls._harness.exp_provider_platform
        cls._exp_conn_info = cls._harness.exp_conn_info

        cls._imp_platform = cls._harness.imp_provider_platform
        cls._imp_conn_info = cls._harness.imp_conn_info

        cls._client = cls.get_client()

    def setUp(self):
        super().setUp()

        to_patch = [
            # Prevent the test runner from being killed by Coriolis sending
            # SIGINT to in-process workers.
            "psutil.Process.send_signal",
        ]
        for thing in to_patch:
            patcher = mock.patch(thing)
            patcher.start()
            self.addCleanup(patcher.stop)

        # fake:// oslo_messaging doesn't serialize objects. After calls, some
        # actions may remain as objects, yet they are expected to be dicts.
        self._patch_rpc_client_method("call")
        self._patch_rpc_client_method("cast")

    def _patch_rpc_client_method(self, method):
        original_call = getattr(messaging.RPCClient, method)

        def _call(self, ctxt, method, **kwargs):
            for key, value in kwargs.items():
                if isinstance(value, models.ModelBase):
                    kwargs[key] = dict(value.items())

            return original_call(self, ctxt, method, **kwargs)

        patcher = mock.patch.object(
            messaging.RPCClient, method, _call)
        patcher.start()
        self.addCleanup(patcher.stop)

    # Helpers for subclasses
    @classmethod
    def get_client(cls):
        """Return a coriolisclient.Client pointed at the in-process API."""
        auth = token_endpoint.Token(
            endpoint='http://127.0.0.1:%d' % cls._api_port,
            token='integration-dummy-token',
        )
        return coriolis_client.Client(
            session=ks_session.Session(auth=auth),
            project_id=harness._TEST_PROJECT_ID,
        )

    @classmethod
    def _create_endpoint(cls, **kwargs):
        endpoint_kwargs = {
            "description": "",
            "regions": [],
        }
        endpoint_kwargs.update(kwargs)
        endpoint = cls._client.endpoints.create(**endpoint_kwargs)
        cls.addClassCleanup(cls._client.endpoints.delete, endpoint.id)

        return endpoint

    def _create_transfer(
            self, src_id, dst_id, instances, source_environment=None,
            destination_environment=None, **kwargs):
        """Create a Replica transfer object and return its ID."""

        transfer = self._client.transfers.create(
            origin_endpoint_id=src_id,
            destination_endpoint_id=dst_id,
            source_environment=source_environment or {},
            destination_environment=destination_environment or {},
            instances=instances,
            transfer_scenario=constants.TRANSFER_SCENARIO_REPLICA,
            network_map={},
            storage_mappings={},
            notes="integration test replica",
            skip_os_morphing=True,
            **kwargs,
        )
        self.addCleanup(self._client.transfers.delete, transfer.id)

        return transfer

    @classmethod
    def _create_pool(
            cls, endpoint_id, name="test-pool", skip_allocation=True):
        pool = cls._client.minion_pools.create(
            name=name,
            endpoint=endpoint_id,
            platform=constants.PROVIDER_PLATFORM_DESTINATION,
            os_type=constants.OS_TYPE_LINUX,
            environment_options={},
            minimum_minions=1,
            maximum_minions=1,
            minion_max_idle_time=3600,
            minion_retention_strategy=(
                constants.MINION_POOL_MACHINE_RETENTION_STRATEGY_DELETE),
            skip_allocation=skip_allocation,
        )
        cls.addClassCleanup(cls._safe_delete_pool, pool.id)

        return pool

    @classmethod
    def _safe_delete_pool(cls, pool_id):
        """Delete pool, force-deallocating first if needed."""
        try:
            pool = cls._client.minion_pools.get(pool_id)
        except Exception:
            LOG.info("[Cleanup]: Pool '%s' not found. Skip delete.")
            return

        if pool.status not in MINION_DEALLOCATED_TERMINAL:
            cls._client.minion_pools.deallocate_minion_pool(
                pool_id, force=True)
            cls._wait_for_pool(pool_id, MINION_DEALLOCATED_TERMINAL)

        with mock.patch("coriolis.keystone.delete_trust"):
            # When removing minion pools, this is also called.
            # There is no Keystone, so it needs to be mocked.
            cls._client.minion_pools.delete(pool_id)

    @classmethod
    def _wait_for_pool(cls, pool_id, terminal_statuses, timeout=180):
        """Poll the DB until *pool_id* reaches one of *terminal_statuses*.

        :returns: minion pool ORM object.
        :raises: AssertionError on timeout.
        """
        ctxt = cls._get_db_context()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            pool = db_api.get_minion_pool(ctxt, pool_id)
            if pool and pool.status in terminal_statuses:
                return pool
            time.sleep(1)
        pool = db_api.get_minion_pool(ctxt, pool_id)
        last = pool.status if pool else "not found"
        raise AssertionError(
            "Pool %s did not reach one of %r within %ds (last: %s)"
            % (pool_id, terminal_statuses, timeout, last)
        )

    @staticmethod
    def _get_db_context():
        return context.RequestContext(
            user='int-test',
            project_id=harness._TEST_PROJECT_ID,
            is_admin=True,
        )

    @staticmethod
    def _ignoreExc(func, ignored_exc=Exception):
        """Wrap the given function, ignoring exceptions."""
        def f(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ignored_exc as ex:
                LOG.warn("Exception encountered: %s", ex)

        return f


class ReplicaIntegrationTestBase(CoriolisIntegrationTestBase):

    _CREATE_MINION_POOLS = False
    _SCSI_DEBUG_SIZE_MB = 16

    @classmethod
    def setUpClass(cls):
        result = subprocess.run(
            ["docker", "image", "inspect", test_utils.DATA_MINION_IMAGE],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode != 0:
            raise unittest.SkipTest(
                "Docker image not found; build it with: "
                "docker build -t %s "
                "coriolis/tests/integration/dockerfiles/data-minion/"
                % test_utils.DATA_MINION_IMAGE)

        super().setUpClass()

        cls._src_endpoint = cls._create_endpoint(
            name="test-src",
            endpoint_type=cls._exp_platform,
            description="integration source endpoint",
            connection_info=cls._exp_conn_info,
        )

        cls._dst_endpoint = cls._create_endpoint(
            name="test-dest",
            endpoint_type=cls._imp_platform,
            description="integration destination endpoint",
            connection_info=cls._imp_conn_info,
        )

        # Create minion pool if needed.
        cls._pool_id = None
        if cls._CREATE_MINION_POOLS:
            pool = cls._create_pool(
                cls._dst_endpoint.id, "transfer-pool", skip_allocation=False)
            cls._pool_id = pool.id

            pool_obj = cls._wait_for_pool(pool.id, MINION_ALLOCATED_TERMINAL)
            if pool_obj.status != constants.MINION_POOL_STATUS_ALLOCATED:
                raise AssertionError(
                    "Pool did not reach ALLOCATED (got %s)" % pool_obj.status,
                )

        # (re)init the scsi_debug module.
        test_utils.destroy_scsi_debug()
        test_utils.init_scsi_debug(size_mb=cls._SCSI_DEBUG_SIZE_MB)

    def setUp(self):
        super().setUp()

        self._src_device = test_utils.add_scsi_debug_device()
        self.addCleanup(test_utils.remove_scsi_debug_device)
        self._dst_device = test_utils.add_scsi_debug_device()
        self.addCleanup(test_utils.remove_scsi_debug_device)

        # Write a test pattern on the src device.
        # Incremental transfer tests update the second chunk (offset=4096).
        # We need to reset any residual data left in the scsi_debug backing
        # store from a previous test run.
        test_utils.write_test_pattern(self._src_device, 8192)

        # Create transfer replica.
        # Use basename as instance name; real VM names do not contain slashes,
        # and some providers use the name as is in resource indentifiers.
        self._instance_name = os.path.basename(self._src_device)
        self._transfer = self._create_transfer(
            self._src_endpoint.id,
            self._dst_endpoint.id,
            instances=[self._instance_name],
            destination_minion_pool_id=self._pool_id,
            source_environment={"block_device_path": self._src_device},
            destination_environment={"devices": [self._dst_device]},
        )

        # mock a few commands that are going to be ran through ssh; they won't
        # pass anyway.
        bkup = "coriolis.providers.backup_writers.HTTPBackupWriterBootstrapper"
        repl = "coriolis.providers.replicator.Replicator"
        for prop in [
            "coriolis.providers.backup_writers._disable_lvm2_lvmetad",
            f"{bkup}._add_firewalld_port",
            f"{bkup}._change_binary_se_context",
            f"{repl}._change_binary_se_context",
        ]:
            mocker = mock.patch(prop)
            mocker.start()
            self.addCleanup(mocker.stop)

    def _execute_and_wait(self, transfer_id, timeout=300):
        """Trigger one execution of *transfer_id* and wait for completion."""
        execution = self._client.transfer_executions.create(
            transfer_id, shutdown_instances=False)
        self.assertExecutionCompleted(execution.id, timeout=timeout)

    def _cleanup_execution(self, transfer_id, execution_id):
        """Cancel a running execution if needed, then delete it.

        Cancels the transfer execution if it has not yet reached a terminal
        state, waits up to 60s for it to finish, then deletes it.

        This avoids the 400 HTTP "cannot delete a RUNNING execution" error that
        occurs when an execution is still in-flight at cleanup time, which can
        happen with slow providers when a test fails or times out before the
        execution completes.
        """
        ctxt = self._get_db_context()
        try:
            execution = db_api.get_tasks_execution(ctxt, execution_id)
        except exception.NotFound:
            LOG.info(
                "Task execution '%s' not found. Skip cleanup.", execution_id)
            return

        if execution.status not in constants.FINALIZED_EXECUTION_STATUSES:
            self._client.transfer_executions.cancel(transfer_id, execution_id)
            self.wait_for_execution(execution_id, timeout=60)

        self._client.transfer_executions.delete(transfer_id, execution_id)

    def wait_for_execution(self, execution_id, timeout=300,
                           desired_statuses=None):
        """Block until *execution_id* reaches a terminal state.

        Polls the DB directly and yields on each iteration so in-process
        services can make progress.

        Returns the finalised TasksExecution ORM object.
        Raises ``AssertionError`` on timeout.
        """
        if not desired_statuses:
            desired_statuses = constants.FINALIZED_EXECUTION_STATUSES

        ctxt = self._get_db_context()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            execution = db_api.get_tasks_execution(ctxt, execution_id)
            if execution.status in desired_statuses:
                return execution
            time.sleep(1)
        self.fail(
            "Execution %s did not reach one of the states %r within %ds "
            "(last status: %s)"
            % (execution_id, desired_statuses, timeout, execution.status)
        )

    def assertExecutionCompleted(self, execution_id, timeout=300):
        """Assert that *execution_id* completes successfully."""
        execution = self.wait_for_execution(execution_id, timeout=timeout)
        self.assertEqual(
            constants.EXECUTION_STATUS_COMPLETED,
            execution.status,
            "Execution %s ended with status %s; task details: %s"
            % (
                execution_id,
                execution.status,
                [
                    (t.task_type, t.status, t.exception_details)
                    for t in execution.tasks
                    if t.status not in (
                        constants.TASK_STATUS_COMPLETED,
                        constants.TASK_STATUS_CANCELED,
                    )
                ],
            ),
        )

    def assertExecutionErrored(self, execution_id, timeout=300):
        """Assert that *execution_id* ends in an error state."""
        execution = self.wait_for_execution(execution_id, timeout=timeout)
        self.assertIn(
            execution.status,
            [
                constants.EXECUTION_STATUS_ERROR,
                constants.EXECUTION_STATUS_DEADLOCKED,
            ],
            "Expected an error status for execution %s, got %s"
            % (execution_id, execution.status),
        )

    def _cleanup_deployment(self, deployment_id):
        """Cancel a running deployment if needed, then delete it.

        Cancels the deployment if it has not yet reached a terminal state,
        waits up to 60s for it to finish, then deletes it.

        This avoids the 400 HTTP "cannot delete a RUNNING deployment" error
        that occurs when a deployment is still in-flight at cleanup time, which
        can happen with slow providers when a test fails or times out before
        the deployment completes.
        """
        ctxt = self._get_db_context()
        deployment = db_api.get_deployment(ctxt, deployment_id)
        if deployment is None:
            LOG.info(
                "Deployment '%s' not found. Skip cleanup.", deployment_id)
            return

        if deployment.last_execution_status in (
                constants.ACTIVE_EXECUTION_STATUSES):
            self._client.deployments.cancel(deployment_id)
            self.wait_for_deployment(deployment_id, timeout=60)

        self._client.deployments.delete(deployment_id)

    def wait_for_deployment(self, deployment_id, timeout=300,
                            desired_statuses=None):
        """Block until *deployment_id* reaches any terminal state.

        Returns the finalised deployment ORM object.
        Raises ``AssertionError`` on timeout.
        """
        if not desired_statuses:
            desired_statuses = constants.FINALIZED_EXECUTION_STATUSES

        ctxt = self._get_db_context()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            deployment = db_api.get_deployment(ctxt, deployment_id)
            if deployment.last_execution_status in desired_statuses:
                return deployment
            time.sleep(1)
        self.fail(
            "Deployment %s did not reach one of the states %r within %ds "
            "(last status: %s)"
            % (deployment_id, desired_statuses, timeout,
               deployment.last_execution_status)
        )

    def assertDeploymentCompleted(self, deployment_id, timeout=300):
        """Assert that *deployment_id* finishes with a completed status."""
        deployment = self.wait_for_deployment(deployment_id, timeout=timeout)
        self.assertEqual(
            constants.EXECUTION_STATUS_COMPLETED,
            deployment.last_execution_status,
            "Deployment %s ended with status %s"
            % (deployment_id, deployment.last_execution_status),
        )

    def _patch_add_delay(self, obj, method_name):
        _orig = getattr(obj, method_name)

        def _slow_call(*args, **kwargs):
            time.sleep(10)
            return _orig(*args, **kwargs)

        self._execute_and_wait(self._transfer.id)

        patcher = mock.patch.object(
            obj, method_name, side_effect=_slow_call, autospec=True,
        )
        patcher.start()
        self.addCleanup(patcher.stop)


class MinionPoolTestBase(CoriolisIntegrationTestBase):
    """Base class for minion pool integration tests.

    Skips the entire test class when the import provider does not advertise
    ``PROVIDER_TYPE_DESTINATION_MINION_POOL`` support.
    """

    @classmethod
    def setUpClass(cls):
        # Check before super(), so that ReplicaIntegrationTestBase.setUpClass
        # does not attempt pool creation against a provider that doesn't
        # support it.
        h = harness._IntegrationHarness.get()
        available = providers_factory.get_available_providers()
        imp_types = available.get(h.imp_provider_platform, {}).get("types", [])
        if constants.PROVIDER_TYPE_DESTINATION_MINION_POOL not in imp_types:
            raise unittest.SkipTest(
                "Import provider '%s' does not support minion pools"
                % h.imp_provider_platform
            )

        super().setUpClass()


class MinionPoolReplicaTestBase(
        MinionPoolTestBase, ReplicaIntegrationTestBase):
    """Base class for replica integration tests using minion pools.

    Extends the assertions to also verify that the minions in the pool have
    been used, and that the minions and the pool returns to an available state.
    """

    _CREATE_MINION_POOLS = True

    def _execute_and_wait(self, transfer_id, timeout=300):
        super()._execute_and_wait(transfer_id, timeout=timeout)
        self.assertPoolAllocated(self._pool_id)
        self.assertMachinesAvailable(self._pool_id)

    def assertExecutionCompleted(self, execution_id, timeout=300):
        super().assertExecutionCompleted(execution_id, timeout=timeout)
        self.assertPoolAllocated(self._pool_id)
        self.assertMachinesAvailable(self._pool_id)

    def assertDeploymentCompleted(self, deployment_id, timeout=300):
        super().assertDeploymentCompleted(deployment_id, timeout=timeout)
        self.assertPoolAllocated(self._pool_id)
        self.assertMachinesAvailable(self._pool_id)

    def assertPoolAllocated(self, pool_id):
        """Assert the pool is healthy and still in ALLOCATED status."""
        ctxt = self._get_db_context()
        pool = db_api.get_minion_pool(ctxt, pool_id)
        self.assertIsNotNone(pool, "Pool %s not found" % pool_id)
        self.assertEqual(
            constants.MINION_POOL_STATUS_ALLOCATED,
            pool.status,
            "Pool %s is not ALLOCATED (got %s)" % (pool_id, pool.status),
        )

    def assertMachinesAvailable(self, pool_id):
        """Assert all machines in the pool are AVAILABLE and have been used."""
        ctxt = self._get_db_context()
        pool = db_api.get_minion_pool(ctxt, pool_id, include_machines=True)
        self.assertIsNotNone(pool, "Pool %s not found" % pool_id)
        self.assertTrue(
            pool.minion_machines,
            "Pool %s has no minion machines" % pool_id,
        )
        for machine in pool.minion_machines:
            self.assertEqual(
                constants.MINION_MACHINE_STATUS_AVAILABLE,
                machine.allocation_status,
                "Machine %s in pool %s is not AVAILABLE (got %s)"
                % (machine.id, pool_id, machine.allocation_status),
            )
            self.assertIsNotNone(
                machine.last_used_at,
                "Machine %s in pool %s has no last_used_at; "
                "it may not have been used by the transfer"
                % (machine.id, pool_id),
            )
