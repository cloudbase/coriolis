# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Integration tests for the transfer executions.
"""

from unittest import mock

from coriolis import constants
from coriolis.tests.integration import base


class TransferExecutionsTests(base.ReplicaIntegrationTestBase):
    # Provider method to fail in test_execution_auto_deploy_transfer_failure.
    # Plain transfers deploy fresh target resources via deploy_replica_target_resources,
    # minion-pool-backed transfers instead attach volumes to a pre-allocated minion via
    # attach_volumes_to_minion, so deploy_replica_target_resources is never called and
    # would not inject any failure there.
    _AUTO_DEPLOY_FAILURE_METHOD = "deploy_replica_target_resources"

    def test_executions(self):
        # We didn't start the execution yet.
        executions = self._client.transfer_executions.list(self._transfer.id)
        self.assertIsInstance(executions, list)
        self.assertEqual(0, len(executions))

        # Start the execution.
        execution = self._client.transfer_executions.create(
            self._transfer.id, shutdown_instances=False
        )
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
        fetched = self._client.transfer_executions.get(self._transfer.id, execution.id)
        self.assertEqual(execution.id, fetched.id)

        # Delete the execution.
        self._client.transfer_executions.delete(self._transfer.id, execution.id)

        executions = self._client.transfer_executions.list(self._transfer.id)
        ids = [e.id for e in executions]
        self.assertNotIn(execution.id, ids)

    def test_shutdown_instances(self):
        # shutdown_instances=True calls provider.shutdown_instance().
        execution = self._client.transfer_executions.create(
            self._transfer.id, shutdown_instances=True
        )
        self.addCleanup(
            self._cleanup_execution,
            self._transfer.id,
            execution.id,
        )

        self.assertExecutionCompleted(execution.id)

    def _get_transfer_deployment(self):
        deployments = self._client.deployments.list()
        transfer_deployments = [
            d for d in deployments if d.transfer_id == self._transfer.id
        ]
        self.assertEqual(1, len(transfer_deployments))

        return transfer_deployments[0]

    def test_execution_auto_deploy(self):
        """auto_deploy=True hands the deployment off to the deployer manager.

        Exercises the deployer_manager -> conductor.confirm_deployer_completed handoff:
        the deployer_manager service polls the PENDING deployment, notices the
        underlying transfer execution (the "deployer") completed, and calls back into
        the conductor to kick off the actual deployment.
        """
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

        deployment = self._get_transfer_deployment()
        self.addCleanup(self._cleanup_deployment, deployment.id)

        self.assertDeploymentCompleted(deployment.id)

    def test_execution_auto_deploy_transfer_failure(self):
        """A failed "deployer" transfer execution errors out the deployment.

        Exercises the deployer_manager -> conductor.report_deployer_failure path: when
        the transfer execution backing an auto-deployed deployment ends up in an error
        state instead of COMPLETED, the deployer_manager service must report the failure
        back to the conductor, so the PENDING deployment gets moved to ERROR instead of
        being stuck forever.
        """
        injected_error = Exception("injected auto-deploy transfer failure")

        with mock.patch.object(
            self._harness.imp_provider_class,
            self._AUTO_DEPLOY_FAILURE_METHOD,
            side_effect=injected_error,
        ):
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

            self.assertExecutionErrored(execution.id)

        deployment = self._get_transfer_deployment()
        self.addCleanup(self._cleanup_deployment, deployment.id)

        deployment = self.wait_for_deployment(
            deployment.id, desired_statuses=[constants.EXECUTION_STATUS_ERROR]
        )
        self.assertEqual(
            constants.EXECUTION_STATUS_ERROR,
            deployment.last_execution_status,
            "Deployment %s ended with status %s"
            % (deployment.id, deployment.last_execution_status),
        )

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
            self._harness.imp_provider_class,
            "deploy_replica_target_resources",
        )

        execution = self._client.transfer_executions.create(
            self._transfer.id, shutdown_instances=False
        )
        self.addCleanup(
            self._cleanup_execution,
            self._transfer.id,
            execution.id,
        )

        # Wait until the execution is RUNNING before issuing the cancel.
        self.wait_for_execution(execution.id, 30, [constants.EXECUTION_STATUS_RUNNING])

        # Cancel the execution.
        self._client.transfer_executions.cancel(
            self._transfer.id, execution.id, force=force
        )

        final = self.wait_for_execution(execution.id)
        expected_statuses = [
            constants.EXECUTION_STATUS_CANCELED,
            constants.EXECUTION_STATUS_ERROR,
            constants.EXECUTION_STATUS_CANCELED_FOR_DEBUGGING,
        ]
        self.assertIn(
            final.status,
            expected_statuses,
            "Expected a canceled/error status after cancel, got %s" % final.status,
        )


class MinionPoolTransferExecutionsTests(
    base.MinionPoolReplicaTestBase, TransferExecutionsTests
):
    """Transfer executions that use a pre-allocated destination minion pool."""

    _AUTO_DEPLOY_FAILURE_METHOD = "attach_volumes_to_minion"
