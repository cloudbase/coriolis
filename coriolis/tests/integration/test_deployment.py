# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Integration test for the replica deployment.

Runs a full replica execution to completion, then deploys (instantiates) the
replica and asserts that the deployment execution reaches COMPLETED.

Covers deployments.list(), get(), list(detail=True), clone_disks=False,
deployments.cancel().

Must be run as root; requires the scsi_debug kernel module.
"""

from coriolis import constants
from coriolis.tests.integration import base
from coriolis.tests.integration.test_provider import imp


class ReplicaDeploymentIntegrationTest(base.ReplicaIntegrationTestBase):

    def _make_deployment(self, **kwargs):
        # replica execution
        self._execute_and_wait(self._transfer.id)

        deployment = self._client.deployments.create_from_transfer(
            self._transfer.id, **kwargs)
        self.addCleanup(
            self._ignoreExc(self._client.deployments.delete), deployment.id)

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
        deployment = self._make_deployment(
            clone_disks=False, skip_os_morphing=True)

        self.assertDeploymentCompleted(deployment.id)

    def test_cancel_deployment(self):
        # Artificially bump the execution time of a deployment.
        self._patch_add_delay(
            imp.TestImportProvider,
            "finalize_replica_instance_deployment"
        )

        deployment = self._client.deployments.create_from_transfer(
            self._transfer.id, skip_os_morphing=True)
        self.addCleanup(self._client.deployments.delete, deployment.id)

        # Wait until the deployment is RUNNING before issuing the cancel.
        self.wait_for_deployment(
            deployment.id, 30, [constants.EXECUTION_STATUS_RUNNING])

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


class MinionPoolReplicaDeploymentTests(
        base.MinionPoolReplicaTestBase, ReplicaDeploymentIntegrationTest):
    """Replica deployment that uses a pre-allocated destination minion pool."""
