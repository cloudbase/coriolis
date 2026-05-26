# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Integration tests for the OS morphing deployments.

Exercises deployments with skip_os_morphing=False, OS detection, and package
installation in the target OS.
"""

import os
import re
import uuid

from coriolis.tests.integration import base as integration_base
from coriolis.tests.integration import utils as test_utils


class OsMorphingDeploymentTest(integration_base.ReplicaIntegrationTestBase):

    # NOTE(claudiub): Size must be high enough to contain the tested OS and
    # any new packages to be added during OS morphing.
    _SCSI_DEBUG_SIZE_MB = 256

    def setUp(self):
        super().setUp()
        test_utils.write_os_image_to_disk(self._src_device, "ubuntu:24.04")

    def _execute_transfer_and_deployment(self, deployment_kwargs=None):
        deployment_kwargs = deployment_kwargs or {}

        self._execute_and_wait(self._transfer.id)
        deployment = self._client.deployments.create_from_transfer(
            self._transfer.id,
            skip_os_morphing=False,
            **deployment_kwargs,
        )
        self.addCleanup(self._cleanup_deployment, deployment.id)
        self.assertDeploymentCompleted(deployment.id)

    def test_deployment_with_os_morphing(self):
        self.assertFalse(
            test_utils.path_exists_on_device(self._src_device, "usr/bin/jq"),
            "jq was found on the source device before OS morphing",
        )

        self._execute_transfer_and_deployment()

        self.assertTrue(
            test_utils.path_exists_on_device(self._dst_device, "usr/bin/jq"),
            "jq was not found on the destination device after OS morphing",
        )

    def test_os_morphing_global_script_basic_format(self):
        expected_string = str(uuid.uuid4())
        user_scripts = {
            'global': {
                # Coriolis will pass the root path as the first argument.
                # We'll use a Windows style line ending, expecting it to
                # be sanitized.
                'linux': f"echo -n {expected_string} > $1/cookie\r\n",
                'windows': 'should-not-get-executed',
            }
        }
        deployment_kwargs = {
            "user_scripts": user_scripts,
        }
        self._execute_transfer_and_deployment(deployment_kwargs)

        file_contents = test_utils.read_file_from_device(
            self._dst_device,
            "cookie")
        self.assertEqual(expected_string, file_contents)

    def test_os_morphing_instance_script_basic_format(self):
        expected_string = str(uuid.uuid4())
        user_scripts = {
            'instances': {
                self._instance_name: (
                    f"echo -n {expected_string} > $1/cookie\n\r")
            }
        }
        deployment_kwargs = {
            "user_scripts": user_scripts,
        }
        self._execute_transfer_and_deployment(deployment_kwargs)

        file_contents = test_utils.read_file_from_device(
            self._dst_device,
            "cookie")
        self.assertEqual(expected_string, file_contents)

    def test_os_morphing_global_script_extended_format(self):
        user_scripts = {
            'global': {
                'linux': [
                    {
                        "phase": "osmorphing_pre_os_mount",
                        # Write the mounts to the minion temp dir.
                        "payload": "mount > /tmp/pre_mounts\r\n",
                    },
                    {
                        "phase": "osmorphing_post_os_mount",
                        # Copy the mounts file to the migrated OS drive.
                        "payload": "cp /tmp/pre_mounts $1/",
                    },
                    {
                        "phase": "osmorphing_post_os_mount",
                        # Write the mounts to the migrated OS drive.
                        "payload": "mount > $1/post_mounts",
                    },
                ],
                'windows': [
                    {
                        "phase": "osmorphing_pre_os_mount",
                        "payload": "should-not-get-executed",
                    },
                    {
                        "phase": "osmorphing_post_os_mount",
                        "payload": "should-not-get-executed",
                    },
                ]
            }
        }
        deployment_kwargs = {
            "user_scripts": user_scripts,
        }
        self._execute_transfer_and_deployment(deployment_kwargs)

        pre_mounts = test_utils.read_file_from_device(
            self._dst_device,
            "pre_mounts")
        post_mounts = test_utils.read_file_from_device(
            self._dst_device,
            "post_mounts")

        # Ensure that the "osmorphing_pre_os_mount" was executed before
        # the replica OS disk was mounted.
        self.assertNotIn(self._dst_device, pre_mounts)
        self.assertIn(self._dst_device, post_mounts)

    def test_os_morphing_global_script_first_boot(self):
        payload = "mount > /boot_mounts"
        user_scripts = {
            'global': {
                'linux': [
                    {
                        "phase": "replica_first_boot",
                        "payload": "mount > /boot_mounts",
                    },
                ],
                'windows': [
                    {
                        "phase": "replica_first_boot",
                        "payload": "should-not-get-executed",
                    },
                ]
            }
        }
        deployment_kwargs = {
            "user_scripts": user_scripts,
        }
        self._execute_transfer_and_deployment(deployment_kwargs)

        # TODO(lpetrut): the test import provider doesn't actually create
        # replica instances (containers). If it did, we'd have no way to clean
        # them up using Coriolis APIs.
        #
        # For this reason, we can't ensure that the first boot scripts
        # actually get executed. We'll merely verify that those files
        # have been injected at the expected location.
        first_boot_script_dir = "usr/lib/coriolis/firstboot/user"
        first_boot_scripts = test_utils.list_files_from_device(
            self._dst_device, first_boot_script_dir)
        if not first_boot_scripts:
            raise AssertionError("Couldn't find first boot script dir.")

        found = False
        for file_name in first_boot_scripts:
            if re.match(r"\d+-\w+\.sh", file_name):
                first_boot_script_path = os.path.join(
                    first_boot_script_dir, file_name)
                first_boot_script = test_utils.read_file_from_device(
                    self._dst_device,
                    first_boot_script_path)
                if payload == first_boot_script:
                    found = True

        if not found:
            raise AssertionError(
                "Couldn't find the expected first boot script.")
