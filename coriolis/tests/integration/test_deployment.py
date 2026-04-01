# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Integration test for the replica deployment.

Runs a full replica execution to completion, then deploys (instantiates) the
replica and asserts that the deployment execution reaches COMPLETED.

Must be run as root; requires the scsi_debug kernel module.
"""

from coriolis.tests.integration import base


class ReplicaDeploymentIntegrationTest(base.ReplicaIntegrationTestBase):
    """Deploy a previously-replicated replica."""

    def test_replica_deployment(self):
        """Execute a replica, then deploy it and assert COMPLETED."""

        # replica execution
        self._execute_and_wait(self._transfer.id)

        # deployment
        deployment = self._client.deployments.create_from_transfer(
            self._transfer.id,
            skip_os_morphing=True,
        )
        self.addCleanup(self._client.deployments.delete, deployment.id)

        self.assertDeploymentCompleted(deployment.id)
