# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Integration test for the replica deployment.

Runs a full replica execution to completion, then deploys (instantiates) the
replica and asserts that the deployment execution reaches COMPLETED.

Covers deployments.list(), get(), list(detail=True), clone_disks=False,
deployments.cancel().

Must be run as root; requires losetup support (loop devices).
"""

from unittest import mock

from coriolis import constants
from coriolis.db import api as db_api
from coriolis.tests.integration import base
from coriolis.tests.integration.test_provider import imp


class ReplicaDeploymentIntegrationTest(base.ReplicaIntegrationTestBase):
    def _make_deployment(self, **kwargs):
        # replica execution
        self._execute_and_wait(self._transfer.id)

        deployment = self._client.deployments.create_from_transfer(
            self._transfer.id, **kwargs
        )
        self.addCleanup(self._cleanup_deployment, deployment.id, deployment.instances)

        return deployment

    def test_replica_deployment_crud(self):
        # Create.
        deployment = self._make_deployment(skip_os_morphing=True)
        self.assertDeploymentCompleted(deployment.id)

        # Get.
        fetched = self._client.deployments.get(deployment.id)
        self.assertEqual(deployment.id, fetched.id)

        # List.
        deployments = self._client.deployments.list()
        ids = [d.id for d in deployments]
        self.assertIn(deployment.id, ids)

        # Delete.
        self._client.deployments.delete(deployment.id)

        deployments = self._client.deployments.list()
        ids = [s.id for s in deployments]
        self.assertNotIn(deployment.id, ids)

    def test_replica_deployment_without_disk_cloning(self):
        # clone_disks=False reuses the already-transferred replica disks
        # directly instead of cloning them first.
        deployment = self._make_deployment(clone_disks=False, skip_os_morphing=True)

        self.assertDeploymentCompleted(deployment.id)

    def test_replica_deployment_without_disk_cloning_reverts_on_failure(self):
        # With clone_disks=False, the deployment modifies the replica's own disks
        # in place, so a snapshot of them is taken beforehand. If the deployment fails:
        #  - the snapshot is kept (its deletion task does not run on error);
        #  - CLEANUP_FAILED_INSTANCE_DEPLOYMENT runs;
        #  - RESTORE_TRANSFER_DISK_SNAPSHOTS then reverts the disks to the snapshot,
        #    so the replica is left in its pre-deployment state.
        # This test checks that the last two tasks run to completion.
        self._execute_and_wait(self._transfer.id)

        injected_error = Exception("injected finalize deployment failure")
        with mock.patch.object(
            self._harness.imp_provider_class,
            "finalize_replica_instance_deployment",
            side_effect=injected_error,
        ):
            deployment = self._client.deployments.create_from_transfer(
                self._transfer.id, clone_disks=False, skip_os_morphing=True
            )
            self.addCleanup(
                self._cleanup_deployment, deployment.id, deployment.instances
            )

            final = self.wait_for_deployment(deployment.id)

        self.assertNotEqual(
            constants.EXECUTION_STATUS_COMPLETED, final.last_execution_status
        )

        ctxt = self._get_db_context()
        deployment_obj = db_api.get_deployment(ctxt, deployment.id)
        tasks = deployment_obj.executions[0].tasks
        for task_type in (
            constants.TASK_TYPE_RESTORE_TRANSFER_DISK_SNAPSHOTS,
            constants.TASK_TYPE_CLEANUP_FAILED_INSTANCE_DEPLOYMENT,
        ):
            task = next((t for t in tasks if t.task_type == task_type), None)
            self.assertIsNotNone(
                task, "%s task not found in deployment %s" % (task_type, deployment.id)
            )
            self.assertEqual(
                constants.TASK_STATUS_COMPLETED,
                task.status,
                "%s task ended with status %s instead of COMPLETED; details: %s"
                % (task_type, task.status, task.exception_details),
            )

    def test_cancel_deployment(self):
        # Artificially bump the execution time of a deployment.
        self._patch_add_delay(
            imp.TestImportProvider, "finalize_replica_instance_deployment"
        )

        deployment = self._client.deployments.create_from_transfer(
            self._transfer.id, skip_os_morphing=True
        )
        self.addCleanup(self._client.deployments.delete, deployment.id)

        # Wait until the deployment is RUNNING before issuing the cancel.
        self.wait_for_deployment(
            deployment.id, 30, [constants.EXECUTION_STATUS_RUNNING]
        )

        # Cancel the deployment.
        self._client.deployments.cancel(deployment.id)
        final = self.wait_for_deployment(deployment.id)
        self.assertIn(
            final.last_execution_status,
            [
                constants.EXECUTION_STATUS_CANCELED,
                constants.EXECUTION_STATUS_ERROR,
                constants.EXECUTION_STATUS_CANCELED_FOR_DEBUGGING,
            ],
        )
