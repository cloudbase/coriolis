# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

import json
import os
from unittest import mock

from coriolis import constants
from coriolis import exception
from coriolis.osmorphing.osmount import base
from coriolis.osmorphing.osmount import luks_mixin
from coriolis.tests import test_base


class ConcreteLinuxLUKSMixin(
    luks_mixin.LinuxLUKSMixin, base.BaseSSHOSMountTools
):
    def check_os(self):
        pass

    def mount_os(self):
        pass

    def dismount_os(self):
        pass

    def run_user_script(self, user_script):
        pass


_CONN_INFO = {"ip": "127.0.0.1", "username": "foo", "password": "lish"}
_OS_ROOT_DIR = "/mnt/os"
_DEV = "/dev/sda"
_PASSPHRASE = "dont-dead-open-inside"
_UUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
_KEYFILE = "/etc/luks/coriolis_sda.key"


class LinuxLUKSMixinTestCase(test_base.CoriolisBaseTestCase):
    @mock.patch.object(base.BaseSSHOSMountTools, "_connect")
    def setUp(self, mock_connect):
        super().setUp()
        self.event_manager = mock.MagicMock()
        self.osmorphing_info = {constants.ENCRYPTED_DISKS_PASS: _PASSPHRASE}
        self.mixin = ConcreteLinuxLUKSMixin(
            _CONN_INFO,
            self.event_manager,
            [],
            30,
            osmorphing_info=self.osmorphing_info,
        )
        self.mixin._ssh = mock.MagicMock()

    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_unlock_luks_device")
    def test__unlock_luks_devices(self, mock_unlock):
        mock_unlock.return_value = "/dev/mapper/coriolis_sda"
        dev_paths = [_DEV]

        self.mixin._unlock_luks_devices(dev_paths)

        self.assertEqual(dev_paths, ["/dev/mapper/coriolis_sda"])
        self.assertEqual(self.mixin._luks_opened, [("coriolis_sda", _DEV)])
        mock_unlock.assert_called_once_with(_DEV, _PASSPHRASE)

    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_close_luks_devices")
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_unlock_luks_device")
    def test__unlock_luks_devices_cleans_up_on_error(
        self, mock_unlock, mock_close
    ):
        _DEV2 = "/dev/sdb"
        mock_unlock.side_effect = [
            "/dev/mapper/coriolis_sda",
            exception.CoriolisException("bad passphrase"),
        ]

        self.assertRaises(
            exception.CoriolisException,
            self.mixin._unlock_luks_devices,
            [_DEV, _DEV2],
        )

        mock_close.assert_called_once_with()

    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_unlock_luks_device")
    def test__unlock_luks_devices_skips_non_luks(self, mock_unlock):
        mock_unlock.return_value = None
        dev_paths = [_DEV]

        self.mixin._unlock_luks_devices(dev_paths)

        self.assertEqual(dev_paths, [_DEV])
        self.assertEqual(self.mixin._luks_opened, [])

    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_is_luks")
    def test__unlock_luks_device(self, mock_is_luks):
        # not LUKS, skipped regardless of passphrase.
        mock_is_luks.return_value = False
        self.assertIsNone(self.mixin._unlock_luks_device(_DEV, None))
        self.assertIsNone(self.mixin._unlock_luks_device(_DEV, _PASSPHRASE))

        # is LUKS, no password.
        mock_is_luks.return_value = True
        self.assertRaises(
            exception.CoriolisException,
            self.mixin._unlock_luks_device,
            _DEV,
            None,
        )

    @mock.patch.object(base.BaseSSHOSMountTools, "_exec_cmd")
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_auth_luks")
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_is_luks")
    def test__unlock_luks_device_success(
        self, mock_is_luks, mock_auth_luks, mock_exec_cmd
    ):
        mock_is_luks.return_value = True

        result = self.mixin._unlock_luks_device(_DEV, _PASSPHRASE)

        self.assertEqual(result, "/dev/mapper/coriolis_sda")
        mock_auth_luks.assert_called_once_with(
            _PASSPHRASE, "/tmp/coriolis_sda.key"
        )
        mock_exec_cmd.assert_called_once_with(
            "sudo cryptsetup luksOpen --disable-keyring "
            "--key-file /tmp/coriolis_sda.key %s coriolis_sda" % _DEV
        )

    @mock.patch.object(base.BaseSSHOSMountTools, "_exec_cmd")
    def test__is_luks(self, mock_exec_cmd):
        # True.
        mock_exec_cmd.return_value = ""
        self.assertTrue(self.mixin._is_luks(_DEV))
        mock_exec_cmd.assert_called_once_with(
            "sudo cryptsetup isLuks %s" % _DEV
        )

        # False.
        mock_exec_cmd.side_effect = Exception("exit code 1")
        self.assertFalse(self.mixin._is_luks(_DEV))

        # SSHCommandNotFoundException, warning.
        mock_exec_cmd.side_effect = exception.SSHCommandNotFoundException()
        with self.assertLogs(
            "coriolis.osmorphing.osmount.luks_mixin", level='WARNING'
        ):
            result = self.mixin._is_luks(_DEV)
        self.assertFalse(result)

    @mock.patch.object(base.BaseSSHOSMountTools, "_exec_cmd")
    def test__close_luks_devices(self, mock_exec_cmd):
        self.mixin._luks_opened = [
            ("coriolis_sda", "/dev/sda"),
            ("coriolis_sdb", "/dev/sdb"),
        ]
        self.mixin._close_luks_devices()

        mock_exec_cmd.assert_any_call(
            "sudo cryptsetup luksClose coriolis_sda || true"
        )
        mock_exec_cmd.assert_any_call(
            "sudo cryptsetup luksClose coriolis_sdb || true"
        )
        self.assertEqual(self.mixin._luks_opened, [])

    @mock.patch.object(luks_mixin.utils, "write_ssh_file")
    @mock.patch.object(base.BaseSSHOSMountTools, "_exec_cmd")
    def test__write_remote_file(self, mock_exec_cmd, mock_write):
        mock_exec_cmd.return_value = "/foo/lish"
        self.mixin._write_remote_file("/bar/tender", "content")

        mock_write.assert_called_once_with(
            self.mixin._ssh, "/foo/lish", b"content"
        )
        mock_exec_cmd.assert_any_call("sudo mv /foo/lish /bar/tender")

        # With mode.
        self.mixin._write_remote_file("/ness/dante", "content", mode="600")

        mock_exec_cmd.assert_any_call(
            "sudo mv /foo/lish /ness/dante && sudo chmod 600 /ness/dante"
        )

    @mock.patch.object(base.BaseSSHOSMountTools, "_exec_cmd")
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_write_remote_file")
    def test__auth_luks(self, mock_write, mock_exec_cmd):
        with self.assertRaises(Exception):
            with self.mixin._auth_luks(_PASSPHRASE, "/tmp/ting") as key_path:
                self.assertEqual(key_path, "/tmp/ting")
                mock_write.assert_called_once_with("/tmp/ting", _PASSPHRASE)

                # raise an exception, cleanup should be performed.
                raise Exception("al code only sometimes breaks.")

        mock_exec_cmd.assert_called_once_with("sudo rm -f /tmp/ting")

    @mock.patch.object(base.BaseSSHOSMountTools, "_exec_cmd")
    def test__get_tpm2_token_info(self, mock_exec_cmd):
        # Exception.
        mock_exec_cmd.return_value = ""

        with self.assertLogs(
            "coriolis.osmorphing.osmount.luks_mixin", level="WARNING"
        ):
            result = self.mixin._get_tpm2_token_info(_DEV)

        self.assertEqual(result, [])

        # Only the last entry is valid / has keyslots.
        header = {
            "tokens": {
                "0": {"type": "systemd-tpm2", "keyslots": []},
                "1": {"type": "luks2-keyring", "keyslots": ["2"]},
                "2": {"type": "systemd-tpm2", "keyslots": ["1"]},
            }
        }
        mock_exec_cmd.return_value = json.dumps(header)

        result = self.mixin._get_tpm2_token_info(_DEV)

        self.assertEqual(result, [("2", "1")])

        # Empty dump.
        mock_exec_cmd.return_value = json.dumps({})

        result = self.mixin._get_tpm2_token_info(_DEV)
        self.assertEqual(result, [])

    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_auth_luks")
    @mock.patch.object(base.BaseSSHOSMountTools, "_exec_cmd")
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_get_tpm2_token_info")
    def test__remove_tpm2_tokens(self, mock_info, mock_exec_cmd, mock_auth):
        # No tokens.
        mock_info.return_value = []
        self.mixin._remove_tpm2_tokens(_DEV, _PASSPHRASE)

        # Remove token.
        mock_info.return_value = [("0", "1")]
        self.mixin._remove_tpm2_tokens(_DEV, _PASSPHRASE)

        mock_exec_cmd.assert_any_call(
            "sudo cryptsetup token remove --token-id 0 %s" % _DEV
        )
        mock_exec_cmd.assert_any_call(
            "sudo cryptsetup luksKillSlot --key-file /tmp/coriolis_sda.key "
            "%s 1" % _DEV
        )
        mock_auth.assert_called_once()

        # Token removal failed.
        mock_exec_cmd.side_effect = Exception("toe ken remove failed")
        with self.assertLogs(
            "coriolis.osmorphing.osmount.luks_mixin", level="WARNING"
        ):
            self.mixin._remove_tpm2_tokens(_DEV, _PASSPHRASE)

    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_write_remote_file")
    @mock.patch.object(luks_mixin.utils, "read_ssh_file")
    @mock.patch.object(luks_mixin.utils, "test_ssh_path")
    def test__transform_crypttab(self, mock_test, mock_read, mock_write):
        # No file.
        mock_test.return_value = False

        result = self.mixin._transform_crypttab(_OS_ROOT_DIR, None)
        self.assertFalse(result)

        # No changes.
        mock_test.return_value = True
        mock_read.return_value = b"# comment\nluks-root UUID=aaa none none\n"
        result = self.mixin._transform_crypttab(_OS_ROOT_DIR, lambda p: None)
        self.assertFalse(result)
        mock_write.assert_not_called()

        # File changed.
        mock_read.return_value = b"luks-root UUID=aaa none none\n"

        def _set_opts(parts):
            parts[3] = 'new-opt'
            return parts

        result = self.mixin._transform_crypttab(_OS_ROOT_DIR, _set_opts)
        self.assertTrue(result)
        mock_write.assert_called_once()

    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_write_remote_file")
    @mock.patch.object(luks_mixin.utils, "read_ssh_file")
    @mock.patch.object(luks_mixin.utils, "test_ssh_path", return_value=True)
    def test__remove_tpm2_crypttab_options(
        self, mock_test_path, mock_read, mock_write
    ):
        line = (
            "luks-root UUID=%s none tpm2-device=auto,x-initrd.attach\n" % _UUID
        )
        mock_read.return_value = line.encode("utf-8")

        with self.assertLogs(
            "coriolis.osmorphing.osmount.luks_mixin", level="INFO"
        ):
            self.mixin._remove_tpm2_crypttab_options(_OS_ROOT_DIR)

        new_content = mock_write.call_args[0][1]
        self.assertNotIn("tpm2-", new_content)
        self.assertIn("x-initrd.attach", new_content)

    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_remove_tpm2_tokens")
    @mock.patch.object(
        luks_mixin.LinuxLUKSMixin, "_remove_tpm2_crypttab_options"
    )
    def test_remove_encryption_artifacts(self, mock_opts, mock_tokens):
        # No LUKS devices open.
        self.mixin._luks_opened = []
        self.mixin.remove_encryption_artifacts(_OS_ROOT_DIR)
        mock_tokens.assert_not_called()
        mock_opts.assert_not_called()

        # Opened LUKS device.
        self.mixin._luks_opened = [("coriolis_sda", _DEV)]
        self.mixin.remove_encryption_artifacts(_OS_ROOT_DIR)

        mock_tokens.assert_called_once_with(_DEV, _PASSPHRASE)
        mock_opts.assert_called_once_with(_OS_ROOT_DIR)
        self.event_manager.progress_update.assert_called()

    def test__get_migration_keyfile_path(self):
        result = self.mixin._get_migration_keyfile_path("/foo/lish")

        self.assertEqual(
            os.path.join(luks_mixin._LUKS_KEYFILE_DIR, "coriolis_lish.key"),
            result,
        )

    @mock.patch.object(base.BaseSSHOSMountTools, "_exec_cmd")
    def test__get_luks_uuid(self, mock_exec_cmd):
        mock_exec_cmd.return_value = "  %s  \n" % _UUID

        result = self.mixin._get_luks_uuid(_DEV)

        self.assertEqual(result, _UUID)
        mock_exec_cmd.assert_called_once_with(
            "sudo cryptsetup luksUUID %s" % _DEV
        )

    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_transform_crypttab")
    def test__update_crypttab_keyfile(self, mock_transform):
        mock_transform.return_value = True
        self.mixin._update_crypttab_keyfile(_OS_ROOT_DIR, {_UUID: _KEYFILE})
        mock_transform.assert_called_once()

        # Extract and exercise the inner _set_keyfile transform.
        transform = mock_transform.call_args[0][1]

        # too few parts.
        self.assertIsNone(transform(["luks-root"]))

        # no UUID match.
        parts = ["luks-root", "PARTUUID=abc", "none", "none"]
        self.assertIsNone(transform(parts))

        # UUID not in map.
        parts = [
            "luks-root",
            "UUID=00000000-0000-0000-0000-000000000000",
            "none",
            "none",
        ]
        self.assertIsNone(transform(parts))

        # UUID= format match; 'initramfs' always appended.
        result = transform(["luks-root", "UUID=%s" % _UUID, "none", "none"])
        self.assertEqual(result[2], _KEYFILE)
        self.assertIn("initramfs", result[3].split(","))

        # /by-uuid/ path also matches.
        parts = ["luks-root", "/dev/disk/by-uuid/%s" % _UUID, "none", "none"]
        result = transform(parts)
        self.assertEqual(result[2], _KEYFILE)

        # transform returns False -> exception.
        mock_transform.return_value = False
        self.assertRaises(
            exception.CoriolisException,
            self.mixin._update_crypttab_keyfile,
            _OS_ROOT_DIR,
            {_UUID: _KEYFILE},
        )

    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_detect_initramfs_tool")
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_configure_dracut_keyfiles")
    @mock.patch.object(
        luks_mixin.LinuxLUKSMixin, "_configure_initramfs_tools_keyfiles"
    )
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_update_crypttab_keyfile")
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_write_remote_file")
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_get_luks_uuid")
    @mock.patch.object(base.BaseSSHOSMountTools, "_exec_cmd")
    def test__write_migration_keyfiles(
        self,
        mock_exec,
        mock_uuid,
        mock_write_file,
        mock_update_ct,
        mock_cfg_initramfs,
        mock_cfg_dracut,
        mock_detect_tool,
    ):
        # No passphrase / opened devices.
        mock_uuid.return_value = _UUID
        self.mixin._osmorphing_info = {}
        self.mixin._luks_opened = []
        self.mixin._write_migration_keyfiles(_OS_ROOT_DIR)
        mock_exec.assert_not_called()

        # dracut branch.
        self.mixin._osmorphing_info = {
            constants.ENCRYPTED_DISKS_PASS: _PASSPHRASE,
        }
        self.mixin._luks_opened = [("coriolis_sda", _DEV)]
        mock_detect_tool.return_value = "dracut"
        self.mixin._write_migration_keyfiles(_OS_ROOT_DIR)

        keyfile_path = os.path.join(
            luks_mixin._LUKS_KEYFILE_DIR,
            "coriolis_%s.key" % os.path.basename(_DEV),
        )
        expected_abs_path = os.path.join(
            _OS_ROOT_DIR, keyfile_path.lstrip("/")
        )
        mock_write_file.assert_called_once_with(
            expected_abs_path, _PASSPHRASE, mode="400"
        )
        mock_update_ct.assert_called_once_with(_OS_ROOT_DIR, {_UUID: _KEYFILE})
        mock_cfg_dracut.assert_called_once_with(
            _OS_ROOT_DIR, {_UUID: _KEYFILE}
        )
        mock_cfg_initramfs.assert_not_called()

        # update-initramfs branch.
        mock_cfg_dracut.reset_mock()
        mock_update_ct.reset_mock()
        mock_write_file.reset_mock()
        mock_detect_tool.return_value = "update-initramfs"
        self.mixin._write_migration_keyfiles(_OS_ROOT_DIR)

        mock_cfg_initramfs.assert_called_once_with(_OS_ROOT_DIR)
        mock_cfg_dracut.assert_not_called()

        # No tool found: CoriolisException raised.
        mock_detect_tool.return_value = None
        self.assertRaises(
            exception.CoriolisException,
            self.mixin._write_migration_keyfiles,
            _OS_ROOT_DIR,
        )

    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_write_remote_file")
    @mock.patch.object(base.BaseSSHOSMountTools, "_exec_cmd")
    @mock.patch.object(luks_mixin.utils, "test_ssh_path")
    def test__configure_dracut_keyfiles(
        self, mock_test_path, mock_exec, mock_write
    ):
        plugin_path = luks_mixin._CRYPTSETUP_TPM2_PLUGIN_PATHS[0]
        plugin_abs = os.path.join(_OS_ROOT_DIR, plugin_path.lstrip("/"))
        conf_abs = os.path.join(
            _OS_ROOT_DIR, luks_mixin._DRACUT_LUKS_CONF_PATH.lstrip("/")
        )

        # no TPM2 plugin.
        mock_test_path.return_value = False
        self.mixin._configure_dracut_keyfiles(_OS_ROOT_DIR, {_UUID: _KEYFILE})

        written = mock_write.call_args[0][1]
        self.assertIn(_KEYFILE, written)
        self.assertIn("install_items+=", written)
        self.assertNotIn(plugin_path, written)
        mock_exec.assert_called_once_with(
            "sudo chown root:root %s && sudo chmod 644 %s"
            % (conf_abs, conf_abs)
        )

        # with TPM2 plugin.
        mock_exec.reset_mock()
        mock_write.reset_mock()
        mock_test_path.side_effect = lambda _ssh, path: path == plugin_abs
        self.mixin._configure_dracut_keyfiles(_OS_ROOT_DIR, {_UUID: _KEYFILE})

        written = mock_write.call_args[0][1]
        self.assertIn(plugin_path, written)

    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_write_remote_file")
    @mock.patch.object(luks_mixin.utils, "read_ssh_file")
    @mock.patch.object(base.BaseSSHOSMountTools, "_exec_cmd")
    @mock.patch.object(luks_mixin.utils, "test_ssh_path")
    def test__configure_initramfs_tools_keyfiles(
        self, mock_test, mock_exec, mock_read, mock_write
    ):
        hook_abs = os.path.join(
            _OS_ROOT_DIR, "etc/cryptsetup-initramfs/conf-hook"
        )

        # existing conf-hook: content is preserved and KEYFILE_PATTERN
        # appended.
        mock_test.side_effect = lambda _ssh, path: path == hook_abs
        mock_read.return_value = b"# existing\n"

        self.mixin._configure_initramfs_tools_keyfiles(_OS_ROOT_DIR)

        written_content = mock_write.call_args[0][1]
        self.assertIn("# existing", written_content)
        self.assertIn("KEYFILE_PATTERN=", written_content)
        self.assertIn("/etc/luks/coriolis_*.key", written_content)

        # no existing conf-hook: KEYFILE_PATTERN still written.
        mock_test.side_effect = None
        mock_test.return_value = False
        mock_write.reset_mock()

        self.mixin._configure_initramfs_tools_keyfiles(_OS_ROOT_DIR)

        written_content = mock_write.call_args[0][1]
        self.assertIn("KEYFILE_PATTERN=", written_content)

    @mock.patch.object(luks_mixin.utils, "test_ssh_path")
    def test__detect_initramfs_tool(self, mock_test):
        # update-initramfs.
        mock_test.side_effect = lambda _ssh, path: "update-initramfs" in path
        result = self.mixin._detect_initramfs_tool(_OS_ROOT_DIR)
        self.assertEqual(result, "update-initramfs")

        # dracut.
        mock_test.side_effect = lambda _ssh, path: "dracut" in path
        result = self.mixin._detect_initramfs_tool(_OS_ROOT_DIR)
        self.assertEqual(result, "dracut")

        # None.
        mock_test.side_effect = None
        mock_test.return_value = False
        result = self.mixin._detect_initramfs_tool(_OS_ROOT_DIR)
        self.assertIsNone(result)

    @mock.patch.object(base.BaseSSHOSMountTools, "_exec_cmd")
    @mock.patch.object(luks_mixin.utils, "test_ssh_path")
    def test__build_dracut_include_args(self, mock_test, mock_exec):
        # no crypttab.
        mock_test.return_value = False
        mock_exec.return_value = ""
        result = self.mixin._build_dracut_include_args(_OS_ROOT_DIR)
        self.assertEqual(result, [])

        # with crypttab and keyfile.
        crypttab_path = os.path.join(_OS_ROOT_DIR, "etc/crypttab")
        luks_dir = os.path.join(_OS_ROOT_DIR, "etc/luks")
        keyfile_abs = os.path.join(luks_dir, "coriolis_sda.key")

        mock_test.side_effect = lambda _ssh, path: path == crypttab_path
        mock_exec.return_value = keyfile_abs
        result = self.mixin._build_dracut_include_args(_OS_ROOT_DIR)

        expected = [
            "--include",
            "/etc/crypttab",
            "/etc/crypttab",
            "--include",
            "/etc/luks/coriolis_sda.key",
            "/etc/luks/coriolis_sda.key",
        ]
        self.assertEqual(result, expected)

    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_build_dracut_include_args")
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_detect_initramfs_tool")
    @mock.patch.object(base.BaseSSHOSMountTools, "_exec_cmd")
    def test__rebuild_initramfs(
        self, mock_exec, mock_detect, mock_include_args
    ):
        # update-initramfs.
        mock_detect.return_value = "update-initramfs"
        self.mixin._rebuild_initramfs(_OS_ROOT_DIR)
        mock_exec.assert_called_once_with(
            "sudo chroot %s update-initramfs -u -k all" % _OS_ROOT_DIR
        )

        # dracut: --regenerate-all --force with --include args.
        mock_exec.reset_mock()
        mock_detect.return_value = "dracut"
        mock_include_args.return_value = [
            "--include",
            "/etc/crypttab",
            "/etc/crypttab",
        ]
        self.mixin._rebuild_initramfs(_OS_ROOT_DIR)

        mock_include_args.assert_called_once_with(_OS_ROOT_DIR)
        mock_exec.assert_called_once_with(
            "sudo chroot %s dracut --regenerate-all --force "
            "--include /etc/crypttab /etc/crypttab" % _OS_ROOT_DIR
        )

        # no tool found.
        mock_detect.return_value = None
        self.assertRaises(
            exception.CoriolisException,
            self.mixin._rebuild_initramfs,
            _OS_ROOT_DIR,
        )

    @mock.patch.object(luks_mixin.utils, 'test_ssh_path')
    def test__detect_init_system(self, mock_test):
        mock_test.side_effect = lambda _ssh, path: 'systemd/systemd' in path
        self.assertEqual(
            self.mixin._detect_init_system(_OS_ROOT_DIR), 'systemd'
        )

        mock_test.side_effect = lambda _ssh, path: path.endswith('openrc')
        self.assertEqual(
            self.mixin._detect_init_system(_OS_ROOT_DIR), 'openrc'
        )

        mock_test.side_effect = lambda _ssh, path: path.endswith('initctl')
        self.assertEqual(
            self.mixin._detect_init_system(_OS_ROOT_DIR), 'upstart'
        )

        # sysvinit fallback.
        mock_test.side_effect = None
        mock_test.return_value = False
        self.assertEqual(
            self.mixin._detect_init_system(_OS_ROOT_DIR), 'sysvinit'
        )

    @mock.patch.object(base.BaseSSHOSMountTools, "_exec_cmd")
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_write_remote_file")
    def test__register_firstboot_script_systemd(self, mock_write, mock_exec):
        self.mixin._register_firstboot_script_systemd(_OS_ROOT_DIR)

        unit_abs = os.path.join(
            _OS_ROOT_DIR, luks_mixin._SYSTEMD_UNIT_PATH.lstrip("/")
        )
        unit = luks_mixin._SYSTEMD_UNIT
        mock_write.assert_called_once_with(unit_abs, unit)
        wants_dir = os.path.join(
            _OS_ROOT_DIR, 'etc/systemd/system/multi-user.target.wants'
        )
        mock_exec.assert_any_call('sudo mkdir -p %s' % wants_dir)
        mock_exec.assert_has_calls(
            [
                mock.call(
                    "sudo chown root:root %s && sudo chmod 644 %s"
                    % (unit_abs, unit_abs)
                ),
                mock.call('sudo mkdir -p %s' % wants_dir),
                mock.call(
                    "sudo ln -sf %s %s/coriolis-luks-firstboot.service"
                    % (luks_mixin._SYSTEMD_UNIT_PATH, wants_dir)
                ),
            ]
        )

    @mock.patch.object(
        luks_mixin.LinuxLUKSMixin, '_register_firstboot_script_systemd'
    )
    @mock.patch.object(
        luks_mixin.LinuxLUKSMixin,
        '_detect_init_system',
        return_value='systemd',
    )
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, "_write_remote_file")
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, '_detect_initramfs_tool')
    def test__install_luks_firstboot_script(
        self,
        mock_detect_tool,
        mock_write,
        mock_exec,
        mock_detect_init,
        mock_reg_systemd,
    ):
        # no initramfs tool found.
        mock_detect_tool.return_value = None
        self.assertRaises(
            exception.CoriolisException,
            self.mixin._install_luks_firstboot_script,
            _OS_ROOT_DIR,
        )

        # update-initramfs.
        mock_detect_tool.return_value = 'update-initramfs'
        mock_detect_init.return_value = "systemd"

        self.mixin._install_luks_firstboot_script(_OS_ROOT_DIR)

        script_abs = os.path.join(
            _OS_ROOT_DIR, luks_mixin._FIRSTBOOT_SCRIPT_PATH.lstrip("/")
        )
        mock_exec.assert_has_calls(
            [
                mock.call("sudo mkdir -p %s" % os.path.dirname(script_abs)),
                mock.call(
                    "sudo chown root:root %s && sudo chmod 500 %s"
                    % (script_abs, script_abs)
                ),
            ]
        )
        mock_reg_systemd.assert_called_once_with(_OS_ROOT_DIR)
        script = luks_mixin._LUKS_FIRSTBOOT_SCRIPTS['update-initramfs']
        mock_write.assert_called_once_with(script_abs, script)

        # dracut.
        mock_detect_tool.return_value = 'dracut'
        mock_write.reset_mock()

        self.mixin._install_luks_firstboot_script(_OS_ROOT_DIR)

        script = luks_mixin._LUKS_FIRSTBOOT_SCRIPTS['dracut']
        mock_write.assert_called_once_with(script_abs, script)

    @mock.patch.object(
        luks_mixin.LinuxLUKSMixin, '_install_luks_firstboot_script'
    )
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, '_rebuild_initramfs')
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, '_fix_grub_luks_root')
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, '_write_migration_keyfiles')
    def test_install_encryption_firstboot_setup(
        self, mock_write_keyfiles, mock_grub, mock_rebuild, mock_install
    ):
        # No LUKS opened.
        self.mixin._luks_opened = []
        self.mixin.install_encryption_firstboot_setup(_OS_ROOT_DIR)
        mock_write_keyfiles.assert_not_called()

        # LUKS opened.
        self.mixin._luks_opened = [("coriolis_sda", _DEV)]

        self.mixin.install_encryption_firstboot_setup(_OS_ROOT_DIR)

        mock_write_keyfiles.assert_called_once_with(_OS_ROOT_DIR)
        mock_grub.assert_called_once_with(_OS_ROOT_DIR)
        mock_rebuild.assert_called_once_with(_OS_ROOT_DIR)
        mock_install.assert_called_once_with(_OS_ROOT_DIR)

    @mock.patch.object(luks_mixin.LinuxLUKSMixin, '_write_remote_file')
    @mock.patch.object(luks_mixin.LinuxLUKSMixin, '_get_luks_uuid')
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(luks_mixin.utils, 'read_ssh_file')
    @mock.patch.object(luks_mixin.utils, 'test_ssh_path')
    def test__fix_grub_luks_root_patches_grub(
        self,
        mock_test_path,
        mock_read_file,
        mock_exec,
        mock_get_luks_uuid,
        mock_write_file,
    ):
        # no opened LUKS devices.
        self.mixin._luks_opened = []
        self.mixin._fix_grub_luks_root(_OS_ROOT_DIR)
        mock_test_path.assert_not_called()

        # no crypttab.
        self.mixin._luks_opened = [("coriolis_sda", _DEV)]
        mock_test_path.return_value = False

        self.mixin._fix_grub_luks_root(_OS_ROOT_DIR)

        crypttab_path = os.path.join(_OS_ROOT_DIR, "etc/crypttab")
        mock_test_path.assert_called_once_with(self.mixin._ssh, crypttab_path)
        mock_read_file.assert_not_called()

        # no uuid_to_crypttab_name.
        mock_test_path.return_value = True
        mock_read_file.return_value = "".encode("utf-8")

        self.mixin._fix_grub_luks_root(_OS_ROOT_DIR)

        mock_read_file.assert_called_once_with(self.mixin._ssh, crypttab_path)
        mock_get_luks_uuid.assert_not_called()

        # no replacements.
        crypttab = 'luks-root UUID=%s none none\n' % _UUID.lower()
        grub_content = (
            'set root=/dev/mapper/coriolis_sda\n'
            'linux /vmlinuz root=/dev/mapper/coriolis_sda\n'
        )
        mock_read_file.return_value = crypttab.encode('utf-8')
        mock_get_luks_uuid.return_value = ""

        self.mixin._fix_grub_luks_root(_OS_ROOT_DIR)

        mock_get_luks_uuid.assert_called_once_with(_DEV)
        mock_exec.assert_not_called()

        # with replacements.
        mock_get_luks_uuid.return_value = _UUID
        grub_path = os.path.join(_OS_ROOT_DIR, 'boot/grub/grub.cfg')
        crypttab_path = os.path.join(_OS_ROOT_DIR, 'etc/crypttab')
        mock_test_path.side_effect = lambda _ssh, path: (
            path in (crypttab_path, grub_path)
        )
        mock_exec.return_value = grub_content

        self.mixin._fix_grub_luks_root(_OS_ROOT_DIR)

        written = mock_write_file.call_args[0][1]
        self.assertIn('/dev/mapper/luks-root', written)
        self.assertNotIn('/dev/mapper/coriolis_sda', written)
