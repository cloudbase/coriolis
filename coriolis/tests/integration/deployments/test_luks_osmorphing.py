# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Integration tests for LUKS-encrypted OS morphing deployments.

The source disk is formatted with LUKS and contains a minimal Linux OS
inside. The transfer copies the raw encrypted chunks to the destination
device. During OS morphing, the osmount layer detects the LUKS container,
unlocks it with the supplied passphrase, mounts the filesystem, and morphs
it. OS families tested:

- Ubuntu 24.04 (initramfs-based): initramfs is regenerated via
  update-initramfs.
- Rocky Linux 9 (dracut-based): initramfs is regenerated via dracut.

Must be run as root; requires the scsi_debug kernel module and cryptsetup.
"""

import os
import tempfile
import unittest

from coriolis import constants
from coriolis.tests.integration import base as integration_base
from coriolis.tests.integration import harness as integration_harness
from coriolis.tests.integration import osmorphing_utils

_LUKS_PASSPHRASE = "it-luks-encrypted"


class _LUKSOSMorphingMixin:

    # Extra space for initramfs-tools and cryptsetup-initramfs packages that
    # the LUKS morphing tools install on top of the base OS image.
    _SCSI_DEBUG_SIZE_MB = 512

    @classmethod
    def setUpClass(cls):
        harness = integration_harness._IntegrationHarness.get()
        if not harness.uses_core_test_import_provider():
            raise unittest.SkipTest(
                "OS morphing tests require local disk access")
        super().setUpClass()

    def setUp(self):
        with tempfile.NamedTemporaryFile(
                mode="w", suffix=".key", delete=False) as fh:
            self._key_file = fh.name
            fh.write(_LUKS_PASSPHRASE)
        self.addCleanup(os.unlink, self._key_file)

        super().setUp()
        self._prepare_src_device()

    def _prepare_src_device(self):
        osmorphing_utils.make_luks_device(
            self._src_device, self._key_file, "ubuntu:24.04")

        dest_env = {
            "devices": [self._dst_device],
            constants.ENCRYPTED_DISKS_PASS: _LUKS_PASSPHRASE,
        }
        self._client.transfers.update(
            self._transfer.id,
            {"destination_environment": dest_env},
        )

    def _check_path_exists(self, device, path):
        with osmorphing_utils.luks_open(device, self._key_file) as mapper_path:
            return osmorphing_utils.path_exists_on_device(mapper_path, path)

    def _assert_luks_common_firstboot_files(self):
        dst_basename = os.path.basename(self._dst_device)
        for path in [
            "usr/lib/coriolis/firstboot/service/luks-firstboot.sh",
            "usr/lib/coriolis/firstboot/run-firstboot.sh",
            "etc/systemd/system/coriolis-firstboot.service",
            "etc/systemd/system/multi-user.target.wants/"
            "coriolis-firstboot.service",
            "etc/luks/coriolis_%s.key" % dst_basename,
        ]:
            self.assertTrue(
                self._check_path_exists(self._dst_device, path),
                "%s not found after LUKS OS morphing" % path,
            )

    def test_deployment_with_os_morphing(self):
        self.assertFalse(
            self._check_path_exists(self._src_device, "usr/bin/jq"),
            "jq was found on the source device before OS morphing",
        )

        self._execute_transfer_and_deployment()

        self.assertTrue(
            self._check_path_exists(self._dst_device, "usr/bin/jq"),
            "jq was not found on the destination device after OS morphing",
        )
        self._assert_firstboot_setup()


class LUKSOSMorphingDeploymentTest(
        _LUKSOSMorphingMixin, integration_base.ReplicaIntegrationTestBase):
    """LUKS + initramfs OS morphing test using Ubuntu 24.04."""

    def _assert_firstboot_setup(self):
        self._assert_luks_common_firstboot_files()
        self.assertTrue(
            self._check_path_exists(
                self._dst_device, "etc/cryptsetup-initramfs/conf-hook"),
            "cryptsetup-initramfs conf-hook not found after LUKS OS morphing",
        )


class LUKSRockyLinuxOSMorphingDeploymentTest(
        _LUKSOSMorphingMixin, integration_base.ReplicaIntegrationTestBase):
    """LUKS + dracut OS morphing test using Rocky Linux 9."""

    # kernel-core (~150 MB installed) needs extra room on top of the base
    # container image and the other morphing packages.
    _SCSI_DEBUG_SIZE_MB = 777

    def _prepare_src_device(self):
        osmorphing_utils.make_luks_device(
            self._src_device, self._key_file, "rockylinux:9")

        dest_env = {
            "devices": [self._dst_device],
            constants.ENCRYPTED_DISKS_PASS: _LUKS_PASSPHRASE,
        }
        self._client.transfers.update(
            self._transfer.id,
            {"destination_environment": dest_env},
        )

    def _assert_firstboot_setup(self):
        self._assert_luks_common_firstboot_files()
        self.assertTrue(
            self._check_path_exists(
                self._dst_device,
                "etc/dracut.conf.d/99-coriolis-luks.conf"),
            "dracut LUKS keyfile config not found after LUKS OS morphing",
        )
