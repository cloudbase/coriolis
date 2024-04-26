# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

from coriolis import exception
from coriolis.osmorphing.osmount import base
from coriolis.tests import test_base
from coriolis.tests import testutils


class CoriolisTestException(Exception):
    pass


class BaseOSMountToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseOSMountTools class."""

    @mock.patch.object(base.BaseOSMountTools, '__abstractmethods__', set())
    def setUp(self):
        super(BaseOSMountToolsTestCase, self).setUp()
        self.event_manager = mock.MagicMock()
        self.base_os_mount_tools = base.BaseOSMountTools(
            mock.sentinel.conn, self.event_manager,
            mock.sentinel.ignore_devices, mock.sentinel.operation_timeout)

    def test_get_environment(self):
        result = self.base_os_mount_tools.get_environment()

        self.assertEqual(result, self.base_os_mount_tools._environment)


# This class is utilized for testing the BaseSSHOSMountTools class, as it's
# an abstract class and can't be instantiated directly.
class TestBaseSSHOSMountTools(base.BaseSSHOSMountTools):
    def check_os(self):
        pass

    def dismount_os(self):
        pass

    def mount_os(self):
        pass


class BaseSSHOSMountToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseSSHOSMountTools class."""

    @mock.patch.object(base.BaseSSHOSMountTools, '_connect')
    def setUp(self, mock_connect):
        super(BaseSSHOSMountToolsTestCase, self).setUp()
        self.event_manager = mock.MagicMock()
        self.ssh = mock.MagicMock()
        self.cmd = "sudo ls -l"
        self.conn_info = {
            "ip": "127.0.0.1",
            "username": "random_username",
            "password": "random_password",
            "pkey": "random_pkey"
        }

        self.base_os_mount_tools = TestBaseSSHOSMountTools(
            self.conn_info, self.event_manager,
            mock.sentinel.ignore_devices, mock.sentinel.operation_timeout)

        self.base_os_mount_tools._ssh = self.ssh

        mock_connect.assert_called_once_with()

    @mock.patch('paramiko.SSHClient')
    @mock.patch.object(base.utils, 'wait_for_port_connectivity')
    def test__connect(self, mock_wait_for_port_connectivity, mock_ssh_client):
        base_os_mount_tools = TestBaseSSHOSMountTools(
            self.conn_info, self.event_manager,
            mock.sentinel.ignore_devices, mock.sentinel.operation_timeout)

        mock_ssh_client.return_value = self.ssh
        original_connect = testutils.get_wrapped_function(
            base_os_mount_tools._connect)

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.INFO):
            original_connect(base_os_mount_tools)

        mock_wait_for_port_connectivity.assert_has_calls([
            mock.call(self.conn_info['ip'], 22),
            mock.call(self.conn_info['ip'], 22),
        ])

        self.ssh.set_missing_host_key_policy.assert_called()
        self.ssh.connect.assert_called_once_with(
            hostname=self.conn_info['ip'], port=22,
            username=self.conn_info['username'],
            pkey=self.conn_info['pkey'],
            password=self.conn_info['password'])

        self.ssh.set_log_channel.assert_called_once_with(
            "paramiko.morpher.%s.%s" % (
                self.conn_info['ip'], 22)
        )

    def test_setup(self):
        self.base_os_mount_tools.setup()
        self.ssh.close_assert_called_once()

    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test__exec_cmd(self, mock_exec_ssh_cmd):
        result = self.base_os_mount_tools._exec_cmd(self.cmd, timeout=120)

        mock_exec_ssh_cmd.assert_called_once_with(
            self.base_os_mount_tools._ssh, self.cmd, {}, get_pty=True,
            timeout=120)

        self.assertEqual(result, mock_exec_ssh_cmd.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test__exec_cmd_without_timeout(self, mock_exec_ssh_cmd):
        result = self.base_os_mount_tools._exec_cmd(self.cmd)

        mock_exec_ssh_cmd.assert_called_once_with(
            self.base_os_mount_tools._ssh, self.cmd, {}, get_pty=True,
            timeout=self.base_os_mount_tools._osmount_operation_timeout)

        self.assertEqual(result, mock_exec_ssh_cmd.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test__exec_cmd_with_exception(self, mock_exec_ssh_cmd):
        mock_exec_ssh_cmd.side_effect = exception.MinionMachineCommandTimeout()

        self.assertRaises(
            exception.OSMorphingSSHOperationTimeout,
            self.base_os_mount_tools._exec_cmd, self.cmd,
            timeout=self.base_os_mount_tools._osmount_operation_timeout)


class TestBaseLinuxOSMountTools(base.BaseLinuxOSMountTools):
    def check_os(self):
        pass


class BaseLinuxOSMountToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseLinuxOSMountTools class."""

    @mock.patch.object(base.BaseSSHOSMountTools, '_connect')
    def setUp(self, mock_connect):
        super(BaseLinuxOSMountToolsTestCase, self).setUp()
        self.event_manager = mock.MagicMock()
        self.os_root_dir = "/root"
        self.all_files = ["etc", "bin", "sbin", "boot"]
        self.one_of_files = ['boot']
        self.devices = ["/dev/sda", "/dev/sdb"]

        self.base_os_mount_tools = TestBaseLinuxOSMountTools(
            mock.sentinel.conn, self.event_manager,
            mock.sentinel.ignore_devices, mock.sentinel.operation_timeout)

        mock_connect.assert_called_once_with()

        self.base_os_mount_tools._ssh = mock.MagicMock()

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    def test__get_pvs(self, mock_exec_cmd):
        mock_exec_cmd.return_value = (
            b"pv1:vg1\nimproper_line\npv2:vg1\n\npv3:vg2")

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.WARN):
                result = self.base_os_mount_tools._get_pvs()

        mock_exec_cmd.assert_called_once_with("sudo pvdisplay -c")
        expected_result = {"vg1": ["pv1", "pv2"], "vg2": ["pv3"]}

        self.assertEqual(result, expected_result)

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    def test__get_pvs_improper_output(self, mock_exec_cmd):
        mock_exec_cmd.return_value = b"improper_line"

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.WARN):
            result = self.base_os_mount_tools._get_pvs()

        mock_exec_cmd.assert_called_once_with("sudo pvdisplay -c")

        self.assertEqual(result, {})

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    def test__get_vgs(self, mock_exec_cmd):
        mock_exec_cmd.return_value = (
            b"vg1:pv1:uuid1\nimproper_line\n\n\nvg2:pv3:uuid2")

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.WARN):
            result = self.base_os_mount_tools._get_vgs()

        mock_exec_cmd.assert_called_once_with(
            "sudo vgs -o vg_name,pv_name,vg_uuid, --noheadings --separator :")
        expected_result = {"vg1": "pv1", "vg2": "pv3"}

        self.assertEqual(result, expected_result)

    @mock.patch.object(base.uuid, 'uuid4')
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    def test__get_vgs_duplicate_vg_names(self, mock_exec_cmd, mock_uuid4):
        mock_exec_cmd.return_value = b"vg1:pv1:uuid1\nvg1:pv2:uuid2"
        mock_uuid4.return_value = "random_uuid"

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.DEBUG):
            result = self.base_os_mount_tools._get_vgs()

        vgs_cmd = (
            "sudo vgs -o vg_name,pv_name,vg_uuid, --noheadings --separator :"
        )
        mock_exec_cmd.assert_has_calls([
            mock.call(vgs_cmd),
            mock.call("sudo vgrename uuid2 random_uuid")
        ])
        expected_result = {"vg1": "pv1", "random_uuid": "pv2"}

        self.assertEqual(result, expected_result)

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    def test__get_vgs_improper_output(self, mock_exec_cmd):
        mock_exec_cmd.return_value = b"improper_line"

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.WARN):
            result = self.base_os_mount_tools._get_vgs()

        mock_exec_cmd.assert_called_once_with(
            "sudo vgs -o vg_name,pv_name,vg_uuid, --noheadings --separator :")

        self.assertEqual(result, {})

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    def test__check_vgs(self, mock_exec_cmd):
        self.base_os_mount_tools._check_vgs()

        mock_exec_cmd.assert_called_once_with("sudo vgck")

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    def test__check_vgs_with_exception(self, mock_exec_cmd):
        mock_exec_cmd.side_effect = Exception()

        self.assertRaises(
            exception.CoriolisException,
            self.base_os_mount_tools._check_vgs)

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    def test__get_vgnames(self, mock_exec_cmd):
        mock_exec_cmd.return_value = (
            b"  Found volume group \"vg1\" using metadata type lvm2\n"
            b"  Found volume group \"vg2\" using metadata type lvm2"
        )

        result = self.base_os_mount_tools._get_vgnames()

        mock_exec_cmd.assert_called_with("sudo vgscan")
        expected_result = ["vg1", "vg2"]

        self.assertEqual(result, expected_result)

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    def test__get_lv_paths(self, mock_exec_cmd):
        mock_exec_cmd.return_value = (
            b"/dev/vg1/lv1:vg1:lv1:-wi-ao----100.00g\n"
            b"/dev/vg2/lv2:vg2:lv2:-wi-a-----50.00g"
        )

        result = self.base_os_mount_tools._get_lv_paths()

        mock_exec_cmd.assert_called_with("sudo lvdisplay -c")
        expected_result = ["/dev/vg1/lv1", "/dev/vg2/lv2"]

        self.assertEqual(result, expected_result)

    @mock.patch.object(base.utils, 'test_ssh_path')
    @mock.patch.object(base.utils, 'read_ssh_file')
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_get_device_file_paths')
    def test__check_mount_fstab_partitions(self, mock_get_device_file_paths,
                                           mock_exec_cmd, mock_read_ssh_file,
                                           mock_test_ssh_path):
        mocked_full_path = self.os_root_dir + "/etc/fstab"
        mock_test_ssh_path.return_value = True
        mock_get_device_file_paths.return_value = ["/dev/sda1", "/dev/sda2"]
        mock_exec_cmd.return_value = (
            b"/dev/sda1:UUID=uuid1\n/dev/sda2:UUID=uuid2"
        )
        mock_read_ssh_file.return_value = (
            b"Unparseable line\n"
            b"UUID=uuid2 /mnt1 ext4 defaults 0 0"
        )

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.WARN):
            result = self.base_os_mount_tools._check_mount_fstab_partitions(
                self.os_root_dir, mountable_lvm_devs=["/dev/sda1"])

        expected_result = [self.os_root_dir + "/mnt1"]

        self.assertEqual(result, expected_result)

        mock_get_device_file_paths.assert_called_once()
        mock_exec_cmd.assert_called_once_with(
            "sudo mount -t ext4 -o defaults"
            " /dev/disk/by-uuid/uuid2 '/root/mnt1'"
        )
        mock_read_ssh_file.assert_called_once_with(
            self.base_os_mount_tools._ssh, mocked_full_path)
        mock_test_ssh_path.assert_has_calls([
            mock.call(self.base_os_mount_tools._ssh, mocked_full_path),
            mock.call(
                self.base_os_mount_tools._ssh, "/dev/disk/by-uuid/uuid2")
        ])

    @mock.patch.object(base.utils, 'test_ssh_path')
    def test__check_mount_fstab_partitions_no_fstab(self, mock_test_ssh_path):
        mock_test_ssh_path.return_value = False

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.WARNING):
            result = self.base_os_mount_tools._check_mount_fstab_partitions(
                self.os_root_dir)

            self.assertEqual(result, [])

        mock_test_ssh_path.assert_called_once_with(
            self.base_os_mount_tools._ssh, self.os_root_dir + "/etc/fstab")

    @mock.patch.object(base.utils, 'test_ssh_path')
    @mock.patch.object(base.utils, 'read_ssh_file')
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_get_symlink_target')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_get_device_file_paths')
    def test__check_mount_fstab_partitions_no_device_path(
            self, mock_get_device_file_paths, mock_get_symlink_target,
            mock_exec_cmd, mock_read_ssh_file, mock_test_ssh_path):
        mocked_full_path = self.os_root_dir + "/etc/fstab"

        mock_test_ssh_path.side_effect = [True, False]
        mock_get_device_file_paths.return_value = ["/dev/sda1", "/dev/sda2"]
        mock_get_symlink_target.return_value = "/dev/sda1"
        mock_exec_cmd.return_value = (
            b"/dev/sda1:UUID=uuid1\n/dev/sda2:UUID=uuid2"
        )
        mock_read_ssh_file.return_value = (
            b"Unparseable line\n"
            b"UUID=uuid2 /mnt1 ext4 defaults 0 0"
        )

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.WARN):
            result = self.base_os_mount_tools._check_mount_fstab_partitions(
                self.os_root_dir)
            self.assertEqual(result, [])

        mock_get_device_file_paths.assert_called_once()
        mock_get_symlink_target.assert_not_called()
        mock_exec_cmd.assert_not_called()
        mock_read_ssh_file.assert_called_once_with(
            self.base_os_mount_tools._ssh, mocked_full_path)
        mock_test_ssh_path.assert_has_calls([
            mock.call(self.base_os_mount_tools._ssh, mocked_full_path),
            mock.call(
                self.base_os_mount_tools._ssh, "/dev/disk/by-uuid/uuid2")
        ])

    @mock.patch.object(base.utils, 'test_ssh_path')
    @mock.patch.object(base.utils, 'read_ssh_file')
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_get_device_file_paths')
    def test__check_mount_fstab_partitions_unsupported_device(
            self, mock_get_device_file_paths, mock_exec_cmd,
            mock_read_ssh_file, mock_test_ssh_path):
        mocked_full_path = self.os_root_dir + "/etc/fstab"

        mock_test_ssh_path.return_value = True
        mock_get_device_file_paths.return_value = ["/dev/sda1", "/dev/sda2"]
        mock_exec_cmd.return_value = (
            b"/dev/sda1:UUID=uuid1\n/dev/sda2:UUID=uuid2"
        )
        mock_read_ssh_file.return_value = (
            b"/dev/sda3 /mnt1 ext4 defaults 0 0"
        )

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.WARN):
            result = self.base_os_mount_tools._check_mount_fstab_partitions(
                self.os_root_dir, mountable_lvm_devs=[
                    "/dev/sda1", "/dev/sda2"])
            self.assertEqual(result, [])

        mock_get_device_file_paths.assert_called_once()
        mock_exec_cmd.assert_called_once_with(
            "readlink -en /dev/sda3")
        mock_read_ssh_file.assert_called_once_with(
            self.base_os_mount_tools._ssh, mocked_full_path)
        mock_test_ssh_path.assert_called_once_with(
            self.base_os_mount_tools._ssh, mocked_full_path)

    @mock.patch.object(base.utils, 'test_ssh_path')
    @mock.patch.object(base.utils, 'read_ssh_file')
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_get_device_file_paths')
    def test__check_mount_fstab_partitions_skip_undesired_mount(
            self, mock_get_device_file_paths, mock_exec_cmd,
            mock_read_ssh_file, mock_test_ssh_path):
        mocked_full_path = self.os_root_dir + "/etc/fstab"
        mock_test_ssh_path.return_value = True
        mock_get_device_file_paths.return_value = ["/dev/sda1", "/dev/sda2"]
        mock_exec_cmd.return_value = (
            b"/dev/sda1:UUID=uuid1\n/dev/sda2:UUID=uuid2"
        )
        mock_read_ssh_file.return_value = (
            b"UUID=uuid2 /mnt1 ext4 defaults 0 0"
        )

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.DEBUG):
            result = self.base_os_mount_tools._check_mount_fstab_partitions(
                self.os_root_dir, skip_mounts=["/mnt1"])
            self.assertEqual(result, [])

        mock_get_device_file_paths.assert_called_once()
        mock_exec_cmd.assert_not_called()
        mock_read_ssh_file.assert_called_once_with(
            self.base_os_mount_tools._ssh, mocked_full_path)
        mock_test_ssh_path.assert_called_once_with(
            self.base_os_mount_tools._ssh, mocked_full_path)

    @mock.patch.object(base.utils, 'test_ssh_path')
    @mock.patch.object(base.utils, 'read_ssh_file')
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_get_device_file_paths')
    def test__check_mount_fstab_partitions_skip_filesystems(
            self, mock_get_device_file_paths, mock_exec_cmd,
            mock_read_ssh_file, mock_test_ssh_path):
        mocked_full_path = self.os_root_dir + "/etc/fstab"

        mock_test_ssh_path.return_value = True
        mock_get_device_file_paths.return_value = ["/dev/sda1", "/dev/sda2"]
        mock_exec_cmd.return_value = (
            b"/dev/sda1:UUID=uuid1\n/dev/sda2:UUID=uuid2"
        )
        mock_read_ssh_file.return_value = (
            b"UUID=uuid1 /mnt1 ext4 defaults 0 0"
        )

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.DEBUG):
            result = self.base_os_mount_tools._check_mount_fstab_partitions(
                self.os_root_dir, skip_filesystems=["ext4"])
            self.assertEqual(result, [])

        mock_get_device_file_paths.assert_called_once()
        mock_exec_cmd.assert_not_called()
        mock_read_ssh_file.assert_called_once_with(
            self.base_os_mount_tools._ssh, mocked_full_path)
        mock_test_ssh_path.assert_called_once_with(
            self.base_os_mount_tools._ssh, mocked_full_path)

    @mock.patch.object(base.utils, 'test_ssh_path')
    @mock.patch.object(base.utils, 'read_ssh_file')
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_get_device_file_paths')
    def test__check_mount_fstab_partitions_duplicate_mounts(
            self, mock_get_device_file_paths, mock_exec_cmd,
            mock_read_ssh_file, mock_test_ssh_path):
        mocked_full_path = self.os_root_dir + "/etc/fstab"

        mock_test_ssh_path.return_value = True
        mock_get_device_file_paths.return_value = ["/dev/sda1", "/dev/sda2"]
        mock_exec_cmd.return_value = (
            b"/dev/sda1:UUID=uuid1\n/dev/sda2:UUID=uuid2"
        )
        mock_read_ssh_file.return_value = (
            b"UUID=uuid1 /mnt1 ext4 defaults 0 0\n"
            b"UUID=uuid2 /mnt1 ext4 defaults 0 0"
        )

        self.assertRaises(
            exception.CoriolisException,
            self.base_os_mount_tools._check_mount_fstab_partitions,
            self.os_root_dir)

        mock_get_device_file_paths.assert_not_called()
        mock_exec_cmd.assert_not_called()
        mock_read_ssh_file.assert_called_once_with(
            self.base_os_mount_tools._ssh, mocked_full_path)
        mock_test_ssh_path.assert_called_once_with(
            self.base_os_mount_tools._ssh, mocked_full_path)

    @mock.patch.object(base.utils, 'test_ssh_path')
    @mock.patch.object(base.utils, 'read_ssh_file')
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_get_device_file_paths')
    def test__check_mount_fstab_partitions_mountcmd_with_exception(
            self, mock_get_device_file_paths, mock_exec_cmd,
            mock_read_ssh_file, mock_test_ssh_path):
        mocked_full_path = self.os_root_dir + "/etc/fstab"
        mock_test_ssh_path.return_value = True
        mock_get_device_file_paths.return_value = ["/dev/sda1", "/dev/sda2"]
        mock_exec_cmd.side_effect = Exception()
        mock_read_ssh_file.return_value = (
            b"UUID=uuid1 /mnt1 ext4 defaults 0 0"
        )

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.WARNING):
            self.base_os_mount_tools._check_mount_fstab_partitions(
                self.os_root_dir)

        mock_get_device_file_paths.assert_called_once()
        mock_exec_cmd.assert_called()
        mock_read_ssh_file.assert_called_once_with(
            self.base_os_mount_tools._ssh, mocked_full_path)
        mock_test_ssh_path.assert_has_calls([
            mock.call(self.base_os_mount_tools._ssh, mocked_full_path),
            mock.call(
                self.base_os_mount_tools._ssh, "/dev/disk/by-uuid/uuid1")
        ])

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    def test__get_symlink_target(self, mock_exec_cmd):
        mock_exec_cmd.return_value = b"/dev/sda1"

        result = self.base_os_mount_tools._get_symlink_target(
            "/dev/sda1")

        mock_exec_cmd.assert_called_once_with(
            'readlink -en %s' % "/dev/sda1")

        self.assertEqual(result, "/dev/sda1")

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    def test__get_symlink_target_with_exception(self, mock_exec_cmd):
        mock_exec_cmd.side_effect = Exception()

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.WARN):
            self.base_os_mount_tools._get_symlink_target(
                "/dev/sda1")

    @mock.patch.object(base.BaseLinuxOSMountTools, '_get_symlink_target')
    def test__get_device_file_paths(self, mock_get_symlink_target):
        symlink_list = ['/dev/GROUP/VOLUME0', '/dev/mapper/GROUP-VOLUME0']
        mock_get_symlink_target.side_effect = ['/dev/dm0', None]

        result = self.base_os_mount_tools._get_device_file_paths(
            symlink_list)

        expected_result = ['/dev/dm0', '/dev/mapper/GROUP-VOLUME0']

        self.assertEqual(result, expected_result)

        mock_get_symlink_target.assert_has_calls([
            mock.call('/dev/GROUP/VOLUME0'),
            mock.call('/dev/mapper/GROUP-VOLUME0')
        ])

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    def test__get_mounted_devices(self, mock_exec_cmd):
        mock_exec_cmd.side_effect = [
            b"/dev/sda1 on / type ext4 (rw,relatime,errors=remount-ro)\n",
            b"/dev/sda1",
            b"8:1",
            b"brw-rw---- 1 root disk 8, 1 Jan  1 00:00 sda1 sda2",
            b""
        ]
        result = self.base_os_mount_tools._get_mounted_devices()

        mock_exec_cmd.assert_has_calls([
            mock.call("cat /proc/mounts"),
            mock.call("readlink -en /dev/sda1"),
            mock.call("mountpoint -x /dev/sda1"),
            mock.call("ls -al /dev | grep ^b"),
        ])

        self.assertEqual(result, ['/dev/sda1', '/dev/sda2'])

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    def test__get_mount_destinations(self, mock_exec_cmd):
        mock_exec_cmd.return_value = (
            b"/dev/sda1 / ext4 rw,relatime 0 0\n"
            b"/dev/sda2 /mnt ext4 rw,relatime 0 0\n"
            b"/dev/sda3 /mnt1 ext4 rw,relatime 0 0\n"
        )

        result = self.base_os_mount_tools._get_mount_destinations()

        mock_exec_cmd.assert_called_once_with("cat /proc/mounts")
        expected_result = set(["/", "/mnt", "/mnt1"])

        self.assertEqual(result, expected_result)

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    def test__get_volume_block_devices(self, mock_exec_cmd):
        mock_exec_cmd.return_value = b"sda\nsda1\nsda2\nsdb\nsdb1\nsdb2"
        self.base_os_mount_tools._ignore_devices = ["/dev/sda1"]

        result = self.base_os_mount_tools._get_volume_block_devices()

        mock_exec_cmd.assert_called_once_with("lsblk -lnao KNAME")
        expected_result = ["/dev/sda", "/dev/sdb"]

        self.assertEqual(result, expected_result)

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.utils, 'list_ssh_dir')
    def test__find_dev_with_contents(self, mock_list_ssh_dir, mock_exec_cmd):
        mock_exec_cmd.return_value = b"/tmp/tmp_dir"
        mock_list_ssh_dir.return_value = ["etc", "bin", "sbin", "boot"]

        result = self.base_os_mount_tools._find_dev_with_contents(
            self.devices, all_files=self.all_files)

        mock_exec_cmd.assert_has_calls([
            mock.call("mktemp -d"),
            mock.call("sudo mount /dev/sda /tmp/tmp_dir"),
            mock.call("sudo umount /tmp/tmp_dir"),
        ])
        mock_list_ssh_dir.assert_called_once_with(
            self.base_os_mount_tools._ssh, "/tmp/tmp_dir")

        expected_result = "/dev/sda"
        self.assertEqual(result, expected_result)

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.utils, 'list_ssh_dir')
    def test__find_dev_with_contents_both_all_and_one_of_files(
            self, mock_list_ssh_dir, mock_exec_cmd):
        self.assertRaises(
            exception.CoriolisException,
            self.base_os_mount_tools._find_dev_with_contents,
            self.devices, all_files=self.all_files,
            one_of_files=self.one_of_files)

        mock_exec_cmd.assert_not_called()
        mock_list_ssh_dir.assert_not_called()

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.utils, 'list_ssh_dir')
    def test__find_dev_with_contents_one_of_files(self, mock_list_ssh_dir,
                                                  mock_exec_cmd):
        mock_exec_cmd.return_value = b"/tmp/tmp_dir"
        mock_list_ssh_dir.return_value = ["etc", "bin", "sbin", "boot"]

        result = self.base_os_mount_tools._find_dev_with_contents(
            self.devices, one_of_files=self.one_of_files)

        mock_exec_cmd.assert_has_calls([
            mock.call("mktemp -d"),
            mock.call("sudo mount /dev/sda /tmp/tmp_dir"),
            mock.call("sudo umount /tmp/tmp_dir"),
        ])
        mock_list_ssh_dir.assert_called_once_with(
            self.base_os_mount_tools._ssh, "/tmp/tmp_dir")

        expected_result = "/dev/sda"
        self.assertEqual(result, expected_result)

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.utils, 'list_ssh_dir')
    def test__find_dev_with_contents_missing_all_files(self, mock_list_ssh_dir,
                                                       mock_exec_cmd):
        mock_exec_cmd.return_value = b"/tmp/tmp_dir"
        mock_list_ssh_dir.return_value = ["etc", "bin", "sbin", "boot"]

        # Append a missing file to the list of all_files
        all_files = self.all_files + ["missing_file"]

        result = self.base_os_mount_tools._find_dev_with_contents(
            self.devices, all_files=all_files)

        mock_exec_cmd.assert_has_calls([
            mock.call("mktemp -d"),
            mock.call("sudo mount /dev/sda /tmp/tmp_dir"),
            mock.call("sudo umount /tmp/tmp_dir"),
            mock.call("mktemp -d"),
            mock.call("sudo mount /dev/sdb /tmp/tmp_dir"),
            mock.call("sudo umount /tmp/tmp_dir"),
        ])
        mock_list_ssh_dir.assert_has_calls([
            mock.call(self.base_os_mount_tools._ssh, "/tmp/tmp_dir"),
            mock.call(self.base_os_mount_tools._ssh, "/tmp/tmp_dir"),
        ])

        self.assertIsNone(result)

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.utils, 'list_ssh_dir')
    def test__find_dev_with_contents_with_exception(self, mock_list_ssh_dir,
                                                    mock_exec_cmd):
        mock_exec_cmd.side_effect = [
            b"/tmp/tmp_dir", Exception(), None, None,  # First device
            b"/tmp/tmp_dir", Exception(), None, None  # Second device
        ]

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.WARN):
            self.base_os_mount_tools._find_dev_with_contents(
                self.devices, all_files=self.all_files)

        mock_list_ssh_dir.assert_not_called()

    @mock.patch.object(base.utils, 'test_ssh_path')
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_find_dev_with_contents')
    def test__find_and_mount_root(self, mock_find_dev_with_contents,
                                  mock_exec_cmd, mock_test_ssh_path):
        devices = ["/dev/sda", "/dev/sdb"]
        mock_exec_cmd.return_value = b"/tmp/tmp_dir"
        mock_find_dev_with_contents.return_value = "/dev/sda"
        mock_test_ssh_path.side_effect = [True, False, True, True]

        result = self.base_os_mount_tools._find_and_mount_root(devices)

        mock_exec_cmd.assert_has_calls([
            mock.call("mktemp -d"),
            mock.call("sudo mount /dev/sda /tmp/tmp_dir"),
            mock.call("sudo mount -o bind /proc/ /tmp/tmp_dir/proc"),
            mock.call("sudo mount -o bind /dev/ /tmp/tmp_dir/dev"),
            mock.call("sudo mount -o bind /run/ /tmp/tmp_dir/run"),
        ])
        mock_find_dev_with_contents.assert_called_once_with(
            ["/dev/sdb"], all_files=self.all_files)
        mock_test_ssh_path.assert_has_calls([
            mock.call(self.base_os_mount_tools._ssh, "/tmp/tmp_dir/proc"),
            mock.call(self.base_os_mount_tools._ssh, "/tmp/tmp_dir/sys"),
            mock.call(self.base_os_mount_tools._ssh, "/tmp/tmp_dir/dev"),
            mock.call(self.base_os_mount_tools._ssh, "/tmp/tmp_dir/run"),
        ])

        expected_result = ('/tmp/tmp_dir', '/dev/sda')

        self.assertEqual(result, expected_result)

    @mock.patch.object(base.utils, 'test_ssh_path')
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_find_dev_with_contents')
    def test__find_and_mount_root_with_exception(
            self, mock_find_dev_with_contents, mock_exec_cmd,
            mock_test_ssh_path):
        devices = ["/dev/sda", "/dev/sdb"]
        mock_exec_cmd.return_value = b"/tmp/tmp_dir"
        mock_find_dev_with_contents.return_value = None

        self.assertRaises(exception.OperatingSystemNotFound,
                          self.base_os_mount_tools._find_and_mount_root,
                          devices)

        mock_find_dev_with_contents.assert_called_once_with(
            devices, all_files=self.all_files)
        mock_exec_cmd.assert_not_called()
        mock_test_ssh_path.assert_not_called()

    @mock.patch.object(base.utils, 'test_ssh_path')
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_find_dev_with_contents')
    def test__find_and_mount_root_exec_cmd_exception(
            self, mock_find_dev_with_contents, mock_exec_cmd,
            mock_test_ssh_path):
        devices = ["/dev/sda", "/dev/sdb"]
        mock_exec_cmd.side_effect = [b"/tmp/tmp_dir", CoriolisTestException()]
        mock_find_dev_with_contents.return_value = "/dev/sda"
        mock_test_ssh_path.return_value = True

        with self.assertLogs('coriolis.osmorphing.osmount.base',
                             level=logging.WARN):
            self.assertRaises(
                CoriolisTestException,
                self.base_os_mount_tools._find_and_mount_root,
                devices
            )
        mock_exec_cmd.assert_has_calls([
            mock.call("mktemp -d"),
            mock.call("sudo mount /dev/sda /tmp/tmp_dir"),
            mock.call("sudo umount /tmp/tmp_dir"),
            mock.call("sudo rmdir /tmp/tmp_dir"),
        ])

        mock_find_dev_with_contents.assert_called_once_with(
            devices, all_files=['etc', 'bin', 'sbin', 'boot'])
        mock_test_ssh_path.assert_not_called()

    @mock.patch.object(base.utils, 'check_fs')
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_get_vgs')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_get_mounted_devices')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_find_and_mount_root')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_find_dev_with_contents')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_get_volume_block_devices')
    @mock.patch.object(
        base.BaseLinuxOSMountTools, '_check_mount_fstab_partitions'
    )
    def test_mount_os(self, mock_check_mount_fstab_partitions,
                      mock_get_volume_block_devices,
                      mock_find_dev_with_contents, mock_find_and_mount_root,
                      mock_get_mounted_devices, mock_get_vgs, mock_exec_cmd,
                      mock_check_fs):
        mock_get_volume_block_devices.return_value = ["/dev/sda", "/dev/sdb"]
        mock_find_and_mount_root.return_value = ("/tmp/tmp_dir", "/dev/sdb1")
        mock_get_vgs.return_value = {"vg1": "/dev/sda1"}
        mock_get_mounted_devices.return_value = ["/dev/sda1"]
        mock_find_dev_with_contents.return_value = "/dev/sdb1"
        mock_exec_cmd.side_effect = [
            b"",
            b"/dev/sda1",
            b"",
            b"/dev/sdb1",
            b"",
            b"",
            b"",
            b"/dev/vg1/lv1",
            b"/dev/sda1",
            b"/dev/sdb1",
            b"ext4",
            b"/dev/vg1/lv1",
            b"ext4",
            b"/dev/vg1/random-lv",
            b"ext4",
        ]

        result = self.base_os_mount_tools.mount_os()

        mock_exec_cmd.assert_has_calls([
            mock.call('sudo partx -v -a /dev/sda || true'),
            mock.call('sudo ls -1 /dev/sda*'),
            mock.call('sudo partx -v -a /dev/sdb || true'),
            mock.call('sudo ls -1 /dev/sdb*'),
            mock.call('sudo vgck'),
            mock.call('sudo vgchange -ay vg1'),
            mock.call('sudo vgchange --refresh'),
            mock.call('sudo ls -1 /dev/vg1/*'),
            mock.call('readlink -en /dev/sda1'),
            mock.call('readlink -en /dev/sdb1'),
            mock.call('sudo blkid -o value -s TYPE /dev/sdb1 || true'),
            mock.call('readlink -en /dev/vg1/lv1'),
            mock.call('sudo blkid -o value -s TYPE /dev/vg1/lv1 || true'),
            mock.call('sudo mount /dev/sdb1 "/tmp/tmp_dir/boot"'),
            mock.call('sudo lvdisplay -c')
        ])
        mock_get_volume_block_devices.assert_called_once()
        mock_find_dev_with_contents.assert_called_once_with(
            ['/dev/sdb1', '/dev/vg1/lv1'], one_of_files=["grub", "grub2"])
        mock_get_mounted_devices.assert_called_once()
        mock_find_and_mount_root.assert_called_once()
        mock_get_vgs.assert_called_once()
        mock_check_mount_fstab_partitions.assert_called_once_with(
            "/tmp/tmp_dir", mountable_lvm_devs=['ext4'])
        mock_check_fs.assert_has_calls([
            mock.call(self.base_os_mount_tools._ssh, "ext4", "/dev/sdb1"),
            mock.call(self.base_os_mount_tools._ssh, "ext4", "/dev/vg1/lv1"),
        ])

        self.assertEqual(result, ('/tmp/tmp_dir', '/dev/sdb1'))

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_get_vgs')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_get_mounted_devices')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_find_and_mount_root')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_find_dev_with_contents')
    @mock.patch.object(base.BaseLinuxOSMountTools, '_get_volume_block_devices')
    @mock.patch.object(
        base.BaseLinuxOSMountTools, '_check_mount_fstab_partitions'
    )
    def test_mount_os_run_xfs(self, mock_check_mount_fstab_partitions,
                              mock_get_volume_block_devices,
                              mock_find_dev_with_contents,
                              mock_find_and_mount_root,
                              mock_get_mounted_devices, mock_get_vgs,
                              mock_exec_cmd):
        mock_get_volume_block_devices.return_value = ["/dev/sda", "/dev/sdb"]
        mock_find_and_mount_root.return_value = ("/tmp/tmp_dir", "/dev/sdb1")
        mock_get_vgs.return_value = {"vg1": "/dev/sda1"}
        mock_get_mounted_devices.return_value = ["/dev/sda1"]
        mock_exec_cmd.side_effect = [
            b"",
            b"xfs\n",
            b"",
            b"xfs\n",
            b"",
            b"",
            b"",
            b"xfs\n",
            b"xfs\n",
            b"xfs\n",
            b"ext4\n",
            b"xfs\n",
            b"ext4\n",
            b"xfs\n",
        ]

        result = self.base_os_mount_tools.mount_os()

        mock_exec_cmd.assert_has_calls([
            mock.call('sudo partx -v -a /dev/sda || true'),
            mock.call('sudo ls -1 /dev/sda*'),
            mock.call('sudo partx -v -a /dev/sdb || true'),
            mock.call('sudo ls -1 /dev/sdb*'),
            mock.call('sudo vgck'),
            mock.call('readlink -en xfs'),
            mock.call('sudo blkid -o value -s TYPE xfs || true'),
            mock.call('readlink -en xfs'),
            mock.call('sudo blkid -o value -s TYPE xfs || true'),
            mock.call('sudo mount %s "/tmp/tmp_dir/boot"' %
                      mock_find_dev_with_contents.return_value),
            mock.call("sudo lvdisplay -c"),
        ])
        mock_get_volume_block_devices.assert_called_once()
        mock_find_dev_with_contents.assert_called_once_with(
            ['xfs'], one_of_files=["grub", "grub2"])
        mock_get_mounted_devices.assert_called_once()
        mock_find_and_mount_root.assert_called_once()
        mock_get_vgs.assert_called_once()
        mock_check_mount_fstab_partitions.assert_called_once_with(
            "/tmp/tmp_dir", mountable_lvm_devs=['ext4'])

        self.assertEqual(result, ('/tmp/tmp_dir', '/dev/sdb1'))

    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    def test_dismount_os(self, mock_exec_cmd):
        root_dir = "/mnt/root_dir"
        mock_exec_cmd.side_effect = [
            None,
            (b"/dev/sda1 /mnt/root_dir/sub_dir type ext4\n"
             b"/dev/sda2 /mnt/root_dir/dev type ext4\n"
             b"/dev/sda3 /mnt/root_dir type ext4\n"),
            None,
            None,
            None,
        ]

        self.base_os_mount_tools.dismount_os(root_dir)

        mock_exec_cmd.assert_has_calls([
            mock.call("sudo fuser --kill --mount /mnt/root_dir || true"),
            mock.call("cat /proc/mounts"),
            mock.call("sudo umount /mnt/root_dir/sub_dir"),
            mock.call("mountpoint -q /mnt/root_dir/dev"
                      " && sudo umount /mnt/root_dir/dev"),
            mock.call(
                "mountpoint -q /mnt/root_dir && sudo umount /mnt/root_dir"),
        ])

    @mock.patch.object(base.utils, 'get_url_with_credentials')
    def test_set_proxy(self, mock_get_url_with_credentials):
        proxy_settings = {
            'url': "http://127.0.0.1:8080",
            'username': "admin",
            'password': 'Random-Password-123!',
            'no_proxy': ['cloudbase.it', '127.0.0.1']
        }
        self.base_os_mount_tools.set_proxy(proxy_settings)

        mock_get_url_with_credentials.assert_called_once_with(
            proxy_settings['url'], proxy_settings['username'],
            proxy_settings['password'])

        self.assertEqual(
            self.base_os_mount_tools._environment['no_proxy'],
            'cloudbase.it.127.0.0.1'
        )

    @mock.patch.object(base.utils, 'get_url_with_credentials')
    def test_set_proxy_no_url(self, mock_get_url_with_credentials):
        proxy_settings = {
            'username': "admin",
            'password': 'Random-Password-123!'
        }
        result = self.base_os_mount_tools.set_proxy(proxy_settings)

        self.assertIsNone(result)

        mock_get_url_with_credentials.assert_not_called()
