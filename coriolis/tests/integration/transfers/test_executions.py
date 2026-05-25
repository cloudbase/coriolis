# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Integration tests for the transfer executions.
"""

from coriolis import constants
from coriolis.tests.integration import base
from coriolis.tests.integration.test_provider import imp


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
            self._cleanup_execution,
            self._transfer.id,
            execution.id,
        )

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
            self._cleanup_execution,
            self._transfer.id,
            execution.id,
        )

        self.assertExecutionCompleted(execution.id)

    def test_execution_auto_deploy(self):
        execution = self._client.transfer_executions.create(
            self._transfer.id,
            shutdown_instances=False,
            auto_deploy=True,
        )
        self.addCleanup(
            self._cleanup_execution,
            self._transfer.id,
            execution.id,
        )

        self.assertExecutionCompleted(execution.id)

        deployments = self._client.deployments.list()
        transfer_deployments = [
            d for d in deployments if d.transfer_id == self._transfer.id
        ]
        self.assertEqual(1, len(transfer_deployments))
        self.addCleanup(
            self._cleanup_deployment, transfer_deployments[0].id)

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
        self._patch_add_delay(
            imp.TestImportProvider,
            "deploy_replica_target_resources",
        )

        execution = self._client.transfer_executions.create(
            self._transfer.id, shutdown_instances=False)
        self.addCleanup(
            self._cleanup_execution,
            self._transfer.id,
            execution.id,
        )

        # Wait until the execution is RUNNING before issuing the cancel.
        self.wait_for_execution(
            execution.id, 30, [constants.EXECUTION_STATUS_RUNNING])

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


class MinionPoolTransferExecutionsTests(
        base.MinionPoolReplicaTestBase, TransferExecutionsTests):
    """Transfer executions that use a pre-allocated destination minion pool."""
