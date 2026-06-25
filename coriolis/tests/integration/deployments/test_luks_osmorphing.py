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
_MIGRATION_PASSPHRASE = "it-luks-migratable"
_TPM_SLOT_SECRET = "fake-tpm-sealed-secret"


class _LUKSOSMorphingMixin:
    """Mixin for LUKS OS morphing integration tests.

    Simulates the pre-migration workflow during setUp: the source disk is
    encrypted with an original key (which may be TPM-sealed on a real VM),
    then a separate migration key is added via the volume master key without
    needing the original passphrase. Only the migration passphrase is given
    to Coriolis, matching what would happen in an actual migration.

    After OS morphing:
    - The original keyslot is intact (Coriolis never touches it).
    - The migration passphrase is written to /etc/luks/coriolis_<dev>.key, so
      the firstboot script can remove the keyslot autonomously.
    """

    # Extra space for initramfs-tools and cryptsetup-initramfs packages that
    # the LUKS morphing tools install on top of the base OS image.
    _SCSI_DEBUG_SIZE_MB = 512
    _CONTAINER_IMAGE = "ubuntu:24.04"

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

        with tempfile.NamedTemporaryFile(
                mode="w", suffix=".key", delete=False) as fh:
            self._migration_key_file = fh.name
            fh.write(_MIGRATION_PASSPHRASE)
        self.addCleanup(os.unlink, self._migration_key_file)

        with tempfile.NamedTemporaryFile(
                mode="w", suffix=".key", delete=False) as fh:
            self._tpm_key_file = fh.name
            fh.write(_TPM_SLOT_SECRET)
        self.addCleanup(os.unlink, self._tpm_key_file)

        super().setUp()
        self._prepare_src_device()

    def _prepare_src_device(self):
        osmorphing_utils.make_luks_device(
            self._src_device, self._key_file, self._CONTAINER_IMAGE)

        # Simulate the real pre-migration step: the disk is already mounted
        # (the VM is running), so the migration key can be added via the
        # volume master key without needing the original passphrase (which
        # may be TPM-sealed, Tang-bound, etc.).
        with osmorphing_utils.luks_open(
                self._src_device, self._key_file,
                disable_keyring=True) as mapper:
            osmorphing_utils.luks_add_key_from_mapper(
                mapper, self._src_device, self._migration_key_file)

        # Add a keyslot representing the TPM-sealed key. LUKS fills slots in
        # ascending order: original (0), migration (1), and this is slot 2.
        osmorphing_utils.luks_add_key(
            self._src_device, self._key_file, self._tpm_key_file)

        # Inject a fake systemd-tpm2 token pointing at the TPM keyslot.
        osmorphing_utils.luks_add_tpm2_token(self._src_device, keyslot_id=2)

        # Overwrite crypttab with the real LUKS UUID but adding
        # tpm2-device=auto, matching what systemd-cryptenroll sets up on a
        # TPM-enrolled system. The real UUID is required so
        # _update_crypttab_keyfile can match it.
        luks_uuid = osmorphing_utils.get_luks_uuid(self._src_device)
        osmorphing_utils.write_file_on_luks_device(
            self._src_device, self._key_file, "etc/crypttab",
            "luks-%s\tUUID=%s\tnone\tluks,tpm2-device=auto,tpm2-pcrs=7\n"
            % (luks_uuid, luks_uuid),
        )

        # Pass only the migration passphrase to Coriolis; the original key
        # is never shared.
        dest_env = {
            "devices": [self._dst_device],
            constants.ENCRYPTED_DISKS_PASS: _MIGRATION_PASSPHRASE,
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

        # The migration keyslot must still be present after morphing; it is
        # supposed to be removed on the first VM boot by the firstboot script.
        self.assertTrue(
            osmorphing_utils.luks_can_open(
                self._dst_device, self._migration_key_file),
            "Migration LUKS keyslot should still be present after OS morphing",
        )

        # Coriolis writes the migration passphrase to the keyfile on the OS,
        # so the filesystem can be unlocked and mounted on the first boot, and
        # the firstboot script can run.
        dst_basename = os.path.basename(self._dst_device)
        keyfile_content = osmorphing_utils.read_file_from_luks_device(
            self._dst_device, self._key_file,
            "etc/luks/coriolis_%s.key" % dst_basename)
        self.assertEqual(
            keyfile_content.strip(), _MIGRATION_PASSPHRASE,
            "Migration keyfile content does not match migration passphrase",
        )

        self.assertFalse(
            osmorphing_utils.luks_has_tpm2_token(self._dst_device),
            "systemd-tpm2 token not removed from destination after morphing",
        )
        self.assertFalse(
            osmorphing_utils.luks_can_open(
                self._dst_device, self._tpm_key_file),
            "TPM2 keyslot was not killed on destination after OS morphing",
        )
        crypttab = osmorphing_utils.read_file_from_luks_device(
            self._dst_device, self._key_file, "etc/crypttab")
        self.assertIsNotNone(
            crypttab, "/etc/crypttab not found on destination")
        self.assertNotIn(
            "tpm2-", crypttab,
            "TPM2 options remain in /etc/crypttab after OS morphing",
        )


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
    _CONTAINER_IMAGE = "rockylinux:9"

    def _assert_firstboot_setup(self):
        self._assert_luks_common_firstboot_files()
        self.assertTrue(
            self._check_path_exists(
                self._dst_device,
                "etc/dracut.conf.d/99-coriolis-luks.conf"),
            "dracut LUKS keyfile config not found after LUKS OS morphing",
        )
