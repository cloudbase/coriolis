# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Integration tests for the transfer executions.
"""

import time
from unittest import mock

from coriolis import constants
from coriolis import context
from coriolis.db import api as db_api
from coriolis.tests.integration import base
from coriolis.tests.integration import harness
from coriolis.tests.integration.providers.test_provider import imp


class TransferExecutionsTests(base.ReplicaIntegrationTestBase):

    def test_executions(self):
        # We didn't start the execution yet.
        executions = self._client.transfer_executions.list(self._transfer.id)
        self.assertIsInstance(executions, list)
        self.assertEqual(0, len(executions))

        # Start the execution.
        execution = self._client.transfer_executions.create(
            self._transfer.id, shutdown_instances=False)
        self.addCleanup(
            self._ignoreExc(self._client.transfer_executions.delete),
            self._transfer.id, execution.id)

        self.assertExecutionCompleted(execution.id)
        executions = self._client.transfer_executions.list(self._transfer.id)
        ids = [e.id for e in executions]
        self.assertIn(execution.id, ids)

        # Get the execution.
        fetched = self._client.transfer_executions.get(
            self._transfer.id, execution.id)
        self.assertEqual(execution.id, fetched.id)

        # Delete the execution.
        self._client.transfer_executions.delete(
            self._transfer.id, execution.id)

        executions = self._client.transfer_executions.list(self._transfer.id)
        ids = [e.id for e in executions]
        self.assertNotIn(execution.id, ids)

    def test_shutdown_instances(self):
        # shutdown_instances=True calls provider.shutdown_instance().
        execution = self._client.transfer_executions.create(
            self._transfer.id, shutdown_instances=True)
        self.addCleanup(
            self._client.transfer_executions.delete,
            self._transfer.id, execution.id)

        self.assertExecutionCompleted(execution.id)

    def test_cancel_running_execution(self):
        self._test_cancel_running_execution(False)

    def test_force_cancel_running_execution(self):
        self._test_cancel_running_execution(True)

    def _test_cancel_running_execution(self, force):
        """Test execution cancellation.

        Verifies that a RUNNING transfer execution can be cancelled via the API
        and that the execution reaches a finalized (CANCELED or ERROR) state.
        """
        # Artificially bump the execution time of a transfer.
        _orig = imp.TestImportProvider.deploy_replica_target_resources

        def _slow_deploy(*args, **kwargs):
            time.sleep(10)
            return _orig(*args, **kwargs)

        patcher = mock.patch.object(
            imp.TestImportProvider,
            "deploy_replica_target_resources",
            side_effect=_slow_deploy,
            autospec=True,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        execution = self._client.transfer_executions.create(
            self._transfer.id, shutdown_instances=False)
        self.addCleanup(
            self._client.transfer_executions.delete,
            self._transfer.id, execution.id)

        # Wait until the execution is RUNNING before issuing the cancel.
        ctxt = context.RequestContext(
            user='int-test',
            project_id=harness._TEST_PROJECT_ID,
            is_admin=True)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            db_exec = db_api.get_tasks_execution(ctxt, execution.id)
            if db_exec.status == constants.EXECUTION_STATUS_RUNNING:
                break
            time.sleep(0.5)
        else:
            self.fail(
                "Execution %s did not reach RUNNING within 30s "
                "(last status: %s)" % (execution.id, db_exec.status))

        # Cancel the execution.
        self._client.transfer_executions.cancel(
            self._transfer.id, execution.id, force=force)

        final = self.wait_for_execution(execution.id)
        expected_statuses = [
            constants.EXECUTION_STATUS_CANCELED,
            constants.EXECUTION_STATUS_ERROR,
            constants.EXECUTION_STATUS_CANCELED_FOR_DEBUGGING,
        ]
        self.assertIn(
            final.status,
            expected_statuses,
            "Expected a canceled/error status after cancel, got %s"
            % final.status,
        )
