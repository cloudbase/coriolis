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
from oslo_log import log as logging

from coriolis import constants
from coriolis import context
from coriolis.db import api as db_api
from coriolis.tests.integration import harness
from coriolis.tests.integration import utils as test_utils
from coriolis.tests import test_base

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

# Path to the SSH private key used to connect to the (local) provider minions.
# Override via the CORIOLIS_TEST_SSH_KEY_PATH environment variable.
_TEST_SSH_KEY_PATH = os.environ.get(
    'CORIOLIS_TEST_SSH_KEY_PATH', '/root/.ssh/id_rsa')


class CoriolisIntegrationTestBase(test_base.CoriolisBaseTestCase):
    """Base class for integration tests."""

    @classmethod
    def setUpClass(cls):
        if os.geteuid() != 0:
            raise unittest.SkipTest("Integration tests must run as root")

        super().setUpClass()
        cls._harness = harness._IntegrationHarness.get()
        cls._workdir = cls._harness.workdir
        cls._db_path = cls._harness.db_path
        cls._lock_path = cls._harness.lock_path
        cls._api_port = cls._harness.api_port
        cls._client = cls.get_client()

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

    def _create_endpoint(self, **kwargs):
        endpoint_kwargs = {
            "description": "",
            "regions": [],
        }
        endpoint_kwargs.update(kwargs)
        endpoint = self._client.endpoints.create(**endpoint_kwargs)
        self.addCleanup(self._client.endpoints.delete, endpoint.id)

        return endpoint

    def _create_transfer(self, src_id, dst_id, instances):
        """Create a Replica transfer object and return its ID."""
        transfer = self._client.transfers.create(
            origin_endpoint_id=src_id,
            destination_endpoint_id=dst_id,
            source_environment={},
            destination_environment={},
            instances=instances,
            transfer_scenario=constants.TRANSFER_SCENARIO_REPLICA,
            network_map={},
            storage_mappings={},
            notes="integration test replica",
            skip_os_morphing=True,
        )
        self.addCleanup(self._client.transfers.delete, transfer.id)

        return transfer

    def _ignoreExc(self, func):
        """Wrap the given function, ignoring exceptions."""
        def f(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as ex:
                LOG.warn("Exception encountered: %s", ex)

        return f


class ReplicaIntegrationTestBase(CoriolisIntegrationTestBase):

    @classmethod
    def setUpClass(cls):
        if not os.path.exists(_TEST_SSH_KEY_PATH):
            raise unittest.SkipTest(
                "SSH private key not found at %r; set "
                "CORIOLIS_TEST_SSH_KEY_PATH to the path of your key."
                % _TEST_SSH_KEY_PATH)

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

    def setUp(self):
        super().setUp()

        self._src_device = test_utils.add_scsi_debug_device()
        self.addCleanup(test_utils.remove_scsi_debug_device)
        self._dst_device = test_utils.add_scsi_debug_device()
        self.addCleanup(test_utils.remove_scsi_debug_device)

        # Write a test pattern on the src device.
        test_utils.write_test_pattern(self._src_device)

        # Create endpoints.
        self._src_endpoint = self._create_endpoint(
            name="test-src",
            endpoint_type="test-src",
            description="integration source endpoint",
            connection_info={
                "block_device_path": self._src_device,
                "pkey_path": _TEST_SSH_KEY_PATH,
            },
        )

        self._dst_endpoint = self._create_endpoint(
            name="test-dest",
            endpoint_type="test-dest",
            description="integration destination endpoint",
            connection_info={
                "devices": [self._dst_device],
                "pkey_path": _TEST_SSH_KEY_PATH,
            },
        )

        # Create transfer replica.
        self._transfer = self._create_transfer(
            self._src_endpoint.id,
            self._dst_endpoint.id,
            instances=[self._src_device],
        )

        # mock a few commands that are going to be ran through ssh; they won't
        # pass anyway.
        bkup = "coriolis.providers.backup_writers.HTTPBackupWriterBootstrapper"
        repl = "coriolis.providers.replicator.Replicator"
        for prop in [
            "coriolis.providers.backup_writers._disable_lvm2_lvmetad",
            "coriolis.providers.backup_writers._disable_lvm_metad_udev_rule",
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

    def _get_db_context(self):
        return context.RequestContext(
            user='int-test',
            project_id=harness._TEST_PROJECT_ID,
            is_admin=True,
        )

    def wait_for_execution(self, execution_id, timeout=300):
        """Block until *execution_id* reaches a terminal state.

        Polls the DB directly and yields on each iteration so in-process
        services can make progress.

        Returns the finalised TasksExecution ORM object.
        Raises ``AssertionError`` on timeout.
        """
        ctxt = self._get_db_context()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            execution = db_api.get_tasks_execution(ctxt, execution_id)
            if execution.status in constants.FINALIZED_EXECUTION_STATUSES:
                return execution
            time.sleep(1)
        self.fail(
            "Execution %s did not reach a terminal state within %ds "
            "(last status: %s)"
            % (execution_id, timeout, execution.status)
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

    def assertDeploymentCompleted(self, deployment_id, timeout=300):
        """Assert that *deployment_id* finishes with a completed status.

        Polls last_execution_status from the DB (the API view does not expose
        the execution ID directly, so DB polling is used for status tracking).
        """
        ctxt = self._get_db_context()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            deployment = db_api.get_deployment(ctxt, deployment_id)
            status = deployment.last_execution_status
            if status in constants.FINALIZED_EXECUTION_STATUSES:
                self.assertEqual(
                    constants.EXECUTION_STATUS_COMPLETED,
                    status,
                    "Deployment %s ended with status %s"
                    % (deployment_id, status),
                )
                return
            time.sleep(1)
        self.fail(
            "Deployment %s did not reach a terminal state within %ds"
            % (deployment_id, timeout)
        )
