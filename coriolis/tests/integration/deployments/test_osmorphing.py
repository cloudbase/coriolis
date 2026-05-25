# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Integration tests for the OS morphing deployments.

Exercises deployments with skip_os_morphing=False, OS detection, and package
installation in the target OS.
"""

from coriolis.tests.integration import base as integration_base
from coriolis.tests.integration import utils as test_utils


class OsMorphingDeploymentTest(integration_base.ReplicaIntegrationTestBase):

    # NOTE(claudiub): Size must be high enough to contain the tested OS and
    # any new packages to be added during OS morphing.
    _SCSI_DEBUG_SIZE_MB = 256

    def setUp(self):
        super().setUp()
        test_utils.write_os_image_to_disk(self._src_device, "ubuntu:24.04")

    def test_deployment_with_os_morphing(self):
        self.assertFalse(
            test_utils.path_exists_on_device(self._src_device, "usr/bin/jq"),
            "jq was found on the source device before OS morphing",
        )

        self._execute_and_wait(self._transfer.id)

        deployment = self._client.deployments.create_from_transfer(
            self._transfer.id,
            skip_os_morphing=False,
        )
        self.addCleanup(self._cleanup_deployment, deployment.id)

        self.assertDeploymentCompleted(deployment.id)
        self.assertTrue(
            test_utils.path_exists_on_device(self._dst_device, "usr/bin/jq"),
            "jq was not found on the destination device after OS morphing",
        )
