# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
import os
from unittest import mock

import ddt

from coriolis import exception
from coriolis.osmorphing import base
from coriolis.tests import test_base


class CoriolisTestException(Exception):
    pass


class BaseOSMorphingToolsTestBase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseOSMorphingTools class."""

    @mock.patch.object(base.BaseOSMorphingTools, '__abstractmethods__', set())
    @mock.patch.object(
        base.BaseOSMorphingTools, 'get_required_detected_os_info_fields'
    )
    def setUp(self, mock_get_required_fields):
        super(BaseOSMorphingToolsTestBase, self).setUp()
        mock_get_required_fields.return_value = [
            'distribution_name', 'release_version'
        ]
        self.detected_os_info = {
            'distribution_name': mock.sentinel.distribution_name,
            'release_version': mock.sentinel.release_version,
        }
        self.os_morphing_tools = base.BaseOSMorphingTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.os_root_device, mock.sentinel.hypervisor,
            mock.sentinel.event_manager, self.detected_os_info,
            mock.sentinel.osmorphing_parameters,
            mock.sentinel.operation_timeout)

    def test_get_required_detected_os_info_fields(self):
        self.assertRaises(
            NotImplementedError,
            base.BaseOSMorphingTools.get_required_detected_os_info_fields
        )

    @mock.patch.object(
        base.BaseOSMorphingTools, 'get_required_detected_os_info_fields'
    )
    def test_check_detected_os_info_parameters(self, mock_get_required_fields):
        mock_get_required_fields.return_value = [
            'distribution_name', 'release_version'
        ]
        result = base.BaseOSMorphingTools.check_detected_os_info_parameters(
            self.detected_os_info)

        mock_get_required_fields.assert_called_once_with()

        self.assertTrue(result)

    @mock.patch.object(
        base.BaseOSMorphingTools, 'get_required_detected_os_info_fields'
    )
    def test_check_detected_os_info_parameters_missing_os_info_fields(
            self, mock_get_required_fields):
        mock_get_required_fields.return_value = [
            'distribution_name', 'release_version'
        ]
        # Remove the release_version field in order to trigger the exception.
        self.detected_os_info.pop('release_version')
        self.assertRaises(
            exception.InvalidDetectedOSParams,
            base.BaseOSMorphingTools.check_detected_os_info_parameters,
            self.detected_os_info
        )
        mock_get_required_fields.assert_called_once_with()

    @mock.patch.object(
        base.BaseOSMorphingTools, 'get_required_detected_os_info_fields'
    )
    def test_check_detected_os_info_parameters_missing_extra_os_info_fields(
            self, mock_get_required_fields):
        # Add an extra field in the detected OS info in order to trigger the
        # exception.
        self.detected_os_info['extra_field'] = mock.sentinel.extra_field
        self.assertRaises(
            exception.InvalidDetectedOSParams,
            base.BaseOSMorphingTools.check_detected_os_info_parameters,
            self.detected_os_info
        )
        mock_get_required_fields.assert_called_once_with()

    def test_check_os_supported_not_implemented(self):
        self.assertRaises(
            NotImplementedError,
            base.BaseOSMorphingTools.check_os_supported,
            self.detected_os_info
        )

    def test_set_environment(self):
        self.os_morphing_tools.set_environment(mock.sentinel.environment)
        self.assertEqual(
            self.os_morphing_tools._environment, mock.sentinel.environment)


# This class is used to test the BaseLinuxOSMorphingTools class since it is
# abstract and cannot be instantiated directly.
class TestLinuxOSMorphingTools(base.BaseLinuxOSMorphingTools):
    def check_os_supported(self):
        pass

    def get_installed_packages(self):
        pass

    def install_packages(self):
        pass

    def set_net_config(self):
        pass

    def uninstall_packages(self):
        pass


@ddt.ddt
class BaseLinuxOSMorphingToolsTestBase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseLinuxOSMorphingTools class."""

    def setUp(self):
        super(BaseLinuxOSMorphingToolsTestBase, self).setUp()
        self.conn = mock.sentinel.conn
        self.os_root_dir = '/root'
        self.chroot_path = '/root/etc/resolv.conf'
        self.os_root_device = mock.sentinel.os_root_device
        self.hypervisor = mock.sentinel.hypervisor
        self.event_manager = mock.MagicMock()
        self.detected_os_info = {
            'distribution_name': mock.sentinel.distribution_name,
            'release_version': mock.sentinel.release_version,
            'os_type': mock.sentinel.os_type,
            'friendly_release_name': mock.sentinel.friendly_release_name,
        }
        self.osmorphing_parameters = mock.sentinel.osmorphing_parameters
        self.operation_timeout = mock.sentinel.operation_timeout
        self.os_morphing_tools = TestLinuxOSMorphingTools(
            self.conn, self.os_root_dir, self.os_root_device, self.hypervisor,
            self.event_manager, self.detected_os_info,
            self.osmorphing_parameters, self.operation_timeout)

    @ddt.data(
        (None, None, None, False),
        ("1.0", 2.0, None, False),
        ("2.0", 2.0, 2.0, True),
        ("3.0", 2.0, 2.5, False),
        ("2.5", 2.0, 3.0, True)
    )
    @ddt.unpack
    def test__version_supported_util(self, version, min_version, max_version,
                                     expected_result):
        result = self.os_morphing_tools._version_supported_util(
            version, min_version, max_version)
        self.assertEqual(result, expected_result)

    @ddt.data(
        (1.0, 2.0, ValueError),
    )
    @ddt.unpack
    def test__version_supported_util_exceptions(self, version, minimum,
                                                expected_exception):
        self.assertRaises(
            expected_exception,
            self.os_morphing_tools._version_supported_util, version, minimum)

    def test_version_supported_util_warnings_no_match(self):
        version = "no match"
        minimum = 1.0
        with self.assertLogs('coriolis.osmorphing.base', level=logging.WARN):
            result = base.BaseLinuxOSMorphingTools._version_supported_util(
                version, minimum)
        self.assertFalse(result)

    def test_get_packages(self):
        self.os_morphing_tools._packages = {
            None: [('pkg1', False), ('pkg2', True)],
            'hypervisor1': [('pkg3', False)],
            'hypervisor2': [('pkg4', True)]
        }
        self.os_morphing_tools._hypervisor = 'hypervisor1'

        add, remove = self.os_morphing_tools.get_packages()

        self.assertEqual(add, ['pkg1', 'pkg2', 'pkg3'])
        self.assertEqual(remove, ['pkg2', 'pkg4'])

    def test_get_packages_no_hypervisor(self):
        self.os_morphing_tools._packages = {
            None: [('pkg1', False), ('pkg2', True)]
        }
        self.os_morphing_tools._hypervisor = None

        add, remove = self.os_morphing_tools.get_packages()

        self.assertEqual(add, ['pkg1', 'pkg2'])
        self.assertEqual(remove, ['pkg2'])

    @mock.patch.object(base.utils, 'write_ssh_file')
    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test_run_user_script_empty_script(self, mock_exec_ssh_cmd,
                                          mock_write_ssh_file):
        result = self.os_morphing_tools.run_user_script('')
        self.assertIsNone(result)
        mock_write_ssh_file.assert_not_called()
        mock_exec_ssh_cmd.assert_not_called()

    @mock.patch.object(base.utils, 'write_ssh_file')
    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test_run_user_script(self, mock_exec_ssh_cmd, mock_write_ssh_file):
        user_script = 'echo "Hello, World!"'
        script_path = '/tmp/coriolis_user_script'

        self.os_morphing_tools.run_user_script(user_script)
        mock_write_ssh_file.assert_called_once_with(
            self.conn, script_path, user_script)
        mock_exec_ssh_cmd.assert_has_calls([
            mock.call(self.conn, "sudo chmod +x %s" % script_path,
                      get_pty=True),
            mock.call(self.conn, 'sudo "%s" "%s"' % (
                script_path, self.os_morphing_tools._os_root_dir),
                get_pty=True)])

    @mock.patch.object(base.utils, 'write_ssh_file')
    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test_run_user_script_with_exception(self, mock_exec_ssh_cmd,
                                            mock_write_ssh_file):
        user_script = 'echo "Hello, World!"'
        mock_write_ssh_file.side_effect = exception.CoriolisException

        self.assertRaises(
            exception.CoriolisException,
            self.os_morphing_tools.run_user_script, user_script)
        mock_exec_ssh_cmd.assert_not_called()

    @mock.patch.object(base.utils, 'write_ssh_file')
    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test_run_user_script_with_exception_on_chmod(self, mock_exec_ssh_cmd,
                                                     mock_write_ssh_file):
        user_script = 'echo "Hello, World!"'
        script_path = '/tmp/coriolis_user_script'

        mock_exec_ssh_cmd.side_effect = exception.CoriolisException

        self.assertRaises(
            exception.CoriolisException,
            self.os_morphing_tools.run_user_script, user_script)

        mock_write_ssh_file.assert_called_once_with(
            self.conn, script_path, user_script)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_copy_resolv_conf')
    def test_pre_packages_install(self, mock_copy_resolv_conf):
        self.os_morphing_tools.pre_packages_install(mock.sentinel.package_name)
        mock_copy_resolv_conf.assert_called_once_with()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_restore_resolv_conf')
    def test_post_packages_install(self, mock_restore_resolv_conf):
        self.os_morphing_tools.post_packages_install(
            mock.sentinel.package_name)
        mock_restore_resolv_conf.assert_called_once_with()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_copy_resolv_conf')
    def test_pre_packages_uninstall(self, mock_copy_resolv_conf):
        self.os_morphing_tools.pre_packages_uninstall(
            mock.sentinel.package_name)
        mock_copy_resolv_conf.assert_called_once_with()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_restore_resolv_conf')
    def test_post_packages_uninstall(self, mock_restore_resolv_conf):
        self.os_morphing_tools.post_packages_uninstall(
            mock.sentinel.package_name)
        mock_restore_resolv_conf.assert_called_once_with()

    def test_get_update_grub2_command(self):
        self.assertRaises(NotImplementedError,
                          self.os_morphing_tools.get_update_grub2_command)

    @mock.patch.object(base.utils, 'test_ssh_path')
    def test__test_path(self, mock_test_ssh_path):
        result = self.os_morphing_tools._test_path(self.chroot_path)

        mocked_full_path = os.path.join(
            self.os_morphing_tools._os_root_dir, self.chroot_path)

        mock_test_ssh_path.assert_called_once_with(
            self.os_morphing_tools._ssh, mocked_full_path)
        self.assertEqual(result, mock_test_ssh_path.return_value)

    @mock.patch.object(base.utils, 'read_ssh_file')
    def test__read_file(self, mock_read_ssh_file):
        result = self.os_morphing_tools._read_file(self.chroot_path)

        mocked_full_path = os.path.join(
            self.os_morphing_tools._os_root_dir, self.chroot_path)

        mock_read_ssh_file.assert_called_once_with(
            self.os_morphing_tools._ssh, mocked_full_path)
        self.assertEqual(result, mock_read_ssh_file.return_value)

    @mock.patch.object(base.utils, 'write_ssh_file')
    def test__write_file(self, mock_write_ssh_file):
        self.os_morphing_tools._write_file(
            self.chroot_path, mock.sentinel.content)

        mocked_full_path = os.path.join(
            self.os_morphing_tools._os_root_dir, self.chroot_path)

        mock_write_ssh_file.assert_called_once_with(
            self.os_morphing_tools._ssh, mocked_full_path,
            mock.sentinel.content)

    @mock.patch.object(base.utils, 'list_ssh_dir')
    def test__list_dir(self, mock_list_ssh_dir):
        result = self.os_morphing_tools._list_dir(self.chroot_path)

        mocked_full_path = os.path.join(
            self.os_morphing_tools._os_root_dir, self.chroot_path)

        mock_list_ssh_dir.assert_called_once_with(
            self.os_morphing_tools._ssh, mocked_full_path)
        self.assertEqual(result, mock_list_ssh_dir.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test__exec_cmd(self, mock_exec_ssh_cmd):
        result = self.os_morphing_tools._exec_cmd(
            mock.sentinel.cmd, timeout=120)

        mock_exec_ssh_cmd.assert_called_once_with(
            self.os_morphing_tools._ssh, mock.sentinel.cmd,
            environment=self.os_morphing_tools._environment, get_pty=True,
            timeout=120)

        self.assertEqual(result, mock_exec_ssh_cmd.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test__exec_cmd_without_timeout(self, mock_exec_ssh_cmd):
        result = self.os_morphing_tools._exec_cmd(mock.sentinel.cmd)

        mock_exec_ssh_cmd.assert_called_once_with(
            self.os_morphing_tools._ssh, mock.sentinel.cmd,
            environment=self.os_morphing_tools._environment, get_pty=True,
            timeout=self.os_morphing_tools._osmorphing_operation_timeout)
        self.assertEqual(result, mock_exec_ssh_cmd.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test__exec_cmd_with_exception(self, mock_exec_ssh_cmd):
        mock_exec_ssh_cmd.side_effect = exception.MinionMachineCommandTimeout()

        self.assertRaises(
            exception.OSMorphingSSHOperationTimeout,
            self.os_morphing_tools._exec_cmd, mock.sentinel.cmd)

    @mock.patch.object(base.utils, 'exec_ssh_cmd_chroot')
    def test__exec_cmd_chroot(self, mock_exec_ssh_cmd_chroot):
        result = self.os_morphing_tools._exec_cmd_chroot(
            mock.sentinel.cmd, timeout=120)

        mock_exec_ssh_cmd_chroot.assert_called_once_with(
            self.os_morphing_tools._ssh, self.os_morphing_tools._os_root_dir,
            mock.sentinel.cmd, environment=self.os_morphing_tools._environment,
            get_pty=True, timeout=120)
        self.assertEqual(result, mock_exec_ssh_cmd_chroot.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd_chroot')
    def test__exec_cmd_chroot_without_timeout(self, mock_exec_ssh_cmd_chroot):
        result = self.os_morphing_tools._exec_cmd_chroot(mock.sentinel.cmd)

        mock_exec_ssh_cmd_chroot.assert_called_once_with(
            self.os_morphing_tools._ssh, self.os_morphing_tools._os_root_dir,
            mock.sentinel.cmd, environment=self.os_morphing_tools._environment,
            get_pty=True,
            timeout=self.os_morphing_tools._osmorphing_operation_timeout)
        self.assertEqual(result, mock_exec_ssh_cmd_chroot.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd_chroot')
    def test__exec_cmd_chroot_with_exception(self, mock_exec_ssh_cmd_chroot):
        mock_exec_ssh_cmd_chroot.side_effect = [
            exception.MinionMachineCommandTimeout()]

        self.assertRaises(
            exception.OSMorphingSSHOperationTimeout,
            self.os_morphing_tools._exec_cmd_chroot, mock.sentinel.cmd)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__check_user_exists(self, mock_exec_cmd_chroot):
        result = self.os_morphing_tools._check_user_exists(
            mock.sentinel.username)

        mock_exec_cmd_chroot.assert_called_once_with(
            'id -u %s' % mock.sentinel.username)
        self.assertTrue(result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__check_user_exists_with_exception(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.side_effect = CoriolisTestException()

        result = self.os_morphing_tools._check_user_exists(
            mock.sentinel.username)
        self.assertFalse(result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.uuid, 'uuid4')
    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test__write_file_sudo(self, mock_exec_ssh_cmd, mock_uuid,
                              mock_exec_cmd, mock_write_file):
        self.os_morphing_tools._write_file_sudo(
            mock.sentinel.chroot_path, mock.sentinel.content)

        mock_write_file.assert_called_once_with(
            'tmp/%s' % mock_uuid.return_value, mock.sentinel.content)
        mock_exec_cmd.assert_has_calls([
            mock.call('cp /tmp/%s /%s' % (
                mock_uuid.return_value, mock.sentinel.chroot_path)),
            mock.call('rm /tmp/%s' % mock_uuid.return_value)])
        mock_exec_ssh_cmd.assert_called_once_with(
            self.os_morphing_tools._ssh, 'sudo sync',
            self.os_morphing_tools._environment, get_pty=True)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__enable_systemd_service(self, mock_exec_cmd_chroot):
        self.os_morphing_tools._enable_systemd_service(
            mock.sentinel.service_name)

        mock_exec_cmd_chroot.assert_called_once_with(
            'systemctl enable %s.service' % mock.sentinel.service_name)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__disable_systemd_service(self, mock_exec_cmd_chroot):
        self.os_morphing_tools._disable_systemd_service(
            mock.sentinel.service_name)

        mock_exec_cmd_chroot.assert_called_once_with(
            'systemctl disable %s.service' % mock.sentinel.service_name)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__disable_upstart_service(self, mock_exec_cmd_chroot):
        self.os_morphing_tools._disable_upstart_service(
            mock.sentinel.service_name)

        mock_exec_cmd_chroot.assert_called_once_with(
            'echo manual | tee /etc/init/%s.override' %
            mock.sentinel.service_name)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_config_file')
    def test__get_os_release(self, mock_read_config_file):
        result = self.os_morphing_tools._get_os_release()

        mock_read_config_file.assert_called_once_with(
            'etc/os-release', check_exists=True)
        self.assertEqual(result, mock_read_config_file.return_value)

    @mock.patch.object(base.utils, 'read_ssh_ini_config_file')
    def test__read_config_file(self, mock_read_ssh_ini):
        result = self.os_morphing_tools._read_config_file(
            self.chroot_path, check_exists=False)

        mocked_full_path = os.path.join(
            self.os_morphing_tools._os_root_dir, self.chroot_path)
        mock_read_ssh_ini.assert_called_once_with(
            self.os_morphing_tools._ssh, mocked_full_path, check_exists=False)
        self.assertEqual(result, mock_read_ssh_ini.return_value)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd')
    def test__copy_resolv_conf(self, mock_exec_cmd, mock_test_path):
        mocked_full_path = os.path.join(
            self.os_morphing_tools._os_root_dir, self.chroot_path)
        resolv_conf_path_old = "%s.old" % mocked_full_path
        mock_test_path.return_value = True

        self.os_morphing_tools._copy_resolv_conf()

        mock_test_path.assert_called_once_with(mocked_full_path)
        mock_exec_cmd.assert_has_calls([
            mock.call('sudo mv -f %s %s' % (
                mocked_full_path, resolv_conf_path_old)),
            mock.call('sudo cp -L --remove-destination /etc/resolv.conf %s' %
                      mocked_full_path)])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd')
    def test__restore_resolv_conf(self, mock_exec_cmd, mock_test_path):
        mocked_full_path = os.path.join(
            self.os_morphing_tools._os_root_dir, self.chroot_path)
        resolv_conf_path_old = "%s.old" % mocked_full_path
        mock_test_path.return_value = True

        self.os_morphing_tools._restore_resolv_conf()

        mock_test_path.assert_called_once_with(resolv_conf_path_old)
        mock_exec_cmd.assert_called_once_with(
            'sudo mv -f %s %s' % (resolv_conf_path_old, mocked_full_path))

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__replace_fstab_entries_device_prefix(
            self, mock_exec_cmd_chroot, mock_write_file, mock_read_file):
        fstab_chroot_path = "etc/fstab"
        current_prefix = "/dev/sd"
        new_prefix = "/dev/vd"

        mock_read_file.return_value = (
            b"/dev/sda1 / ext4 defaults 0 0\n"
            b"/dev/sdb1 /home ext4 defaults 0 0")

        self.os_morphing_tools._replace_fstab_entries_device_prefix(
            current_prefix, new_prefix)

        mock_read_file.assert_called_once_with(fstab_chroot_path)
        mock_exec_cmd_chroot.assert_called_once_with(
            "mv -f /%s /%s.bak" % (fstab_chroot_path, fstab_chroot_path))
        mock_write_file.assert_called_once_with(
            fstab_chroot_path,
            "/dev/vda1 / ext4 defaults 0 0\n/dev/vdb1 /home ext4 defaults 0 0")

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__set_selinux_autorelabel(self, mock_exec_cmd_chroot):
        self.os_morphing_tools._set_selinux_autorelabel()

        mock_exec_cmd_chroot.assert_called_once_with(
            'touch /.autorelabel')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__set_selinux_autorelabel_with_exception(self,
                                                     mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.side_effect = CoriolisTestException()

        with self.assertLogs('coriolis.osmorphing.base',
                             level=logging.WARNING):
            self.os_morphing_tools._set_selinux_autorelabel()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file')
    def test__configure_cloud_init_user_retention(
            self, mock_read_file, mock_test_path, mock_write_file_sudo,
            mock_exec_cmd_chroot):
        cloud_cfg_paths = ["/etc/cloud/cloud.cfg"]
        cloud_cfgs_dir = "/etc/cloud/cloud.cfg.d"

        mock_test_path.return_value = True
        mock_exec_cmd_chroot.return_value = b"10.cfg\n20.cfg"
        mock_read_file.return_value = (
            b"disable_root: true\n"
            b"ssh_pwauth: false\nusers: ['user1', 'user2']")
        self.os_morphing_tools._configure_cloud_init_user_retention()

        mock_exec_cmd_chroot.assert_has_calls([
            mock.call('ls -1 %s' % cloud_cfgs_dir),
            mock.call('cp %s %s.bak' % (
                cloud_cfg_paths[0], cloud_cfg_paths[0])),
            mock.call('cp %s/10.cfg %s/10.cfg.bak' % (
                cloud_cfgs_dir, cloud_cfgs_dir)),
            mock.call('cp %s/20.cfg %s/20.cfg.bak' % (
                cloud_cfgs_dir, cloud_cfgs_dir)),
        ])
        mock_test_path.assert_has_calls([
            mock.call('/etc/cloud/cloud.cfg.d'),
            mock.call('/etc/cloud/cloud.cfg'),
            mock.call('/etc/cloud/cloud.cfg.d/10.cfg'),
            mock.call('/etc/cloud/cloud.cfg.d/20.cfg'),
        ])

        mock_write_file_sudo.assert_has_calls([
            mock.call('/etc/cloud/cloud.cfg',
                      'disable_root: false\nssh_pwauth: true\nusers: null\n'),
            mock.call('/etc/cloud/cloud.cfg.d/10.cfg',
                      'disable_root: false\nssh_pwauth: true\nusers: null\n'),
            mock.call('/etc/cloud/cloud.cfg.d/20.cfg',
                      'disable_root: false\nssh_pwauth: true\nusers: null\n'),
        ])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test__configure_cloud_init_user_retention_path_not_found(
            self, mock_test_path, mock_exec_cmd_chroot, mock_write_file_sudo):
        mock_test_path.return_value = False

        with self.assertLogs('coriolis.osmorphing.base', level=logging.WARN):
            self.os_morphing_tools._configure_cloud_init_user_retention()

        mock_test_path.assert_has_calls([
            mock.call('/etc/cloud/cloud.cfg.d'),
            mock.call('/etc/cloud/cloud.cfg'),
        ])
        mock_exec_cmd_chroot.assert_not_called()
        mock_write_file_sudo.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test__configure_cloud_init_user_retention_exception(
            self, mock_test_path, mock_exec_cmd_chroot, mock_read_file,
            mock_write_file_sudo):
        mock_test_path.return_value = True
        mock_exec_cmd_chroot.return_value = b""
        mock_read_file.return_value = (
            b"disable_root: true"
            b"nssh_pwauth: false"
            b"users: ['user1', 'user2']")
        mock_write_file_sudo.side_effect = Exception()

        self.assertRaises(
            exception.CoriolisException,
            self.os_morphing_tools._configure_cloud_init_user_retention)

        mock_test_path.assert_has_calls([
            mock.call('/etc/cloud/cloud.cfg.d'),
            mock.call('/etc/cloud/cloud.cfg'),
        ])
        mock_exec_cmd_chroot.assert_has_calls([
            mock.call('ls -1 /etc/cloud/cloud.cfg.d'),
            mock.call('cp /etc/cloud/cloud.cfg /etc/cloud/cloud.cfg.bak'),
        ])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__test_path_chroot(self, mock_exec_cmd_chroot):
        path = "/tmp/test_path"
        mock_exec_cmd_chroot.return_value = b"1\n"

        result = self.os_morphing_tools._test_path_chroot(path)

        mock_exec_cmd_chroot.assert_called_once_with(
            '[ -f "%s" ] && echo 1 || echo 0' % path)
        self.assertTrue(result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__test_path_chroot_no_leading_slash(self, mock_exec_cmd_chroot):
        path = "tmp/test_path"
        mock_exec_cmd_chroot.return_value = b"1\n"

        result = self.os_morphing_tools._test_path_chroot(path)

        mock_exec_cmd_chroot.assert_called_once_with(
            '[ -f "/%s" ] && echo 1 || echo 0' % path)
        self.assertTrue(result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__read_file_sudo(self, mock_exec_cmd_chroot):
        chroot_path = "/tmp/test_path"

        result = self.os_morphing_tools._read_file_sudo(chroot_path)

        mock_exec_cmd_chroot.assert_called_once_with('cat %s' % chroot_path)
        self.assertEqual(result, mock_exec_cmd_chroot.return_value)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__read_file_sudo_no_leading_slash(self, mock_exec_cmd_chroot):
        chroot_path = "tmp/test_path"

        result = self.os_morphing_tools._read_file_sudo(chroot_path)

        mock_exec_cmd_chroot.assert_called_once_with('cat /%s' % chroot_path)
        self.assertEqual(result, mock_exec_cmd_chroot.return_value)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__read_grub_config_file_exists(self, mock_test_path_chroot,
                                           mock_read_file_sudo):
        config = mock.sentinel.config
        file_contents = b'key1="value1"\n#comment\nkey2="value2"\ninvalid_line'

        mock_test_path_chroot.return_value = True
        mock_read_file_sudo.return_value = file_contents

        result = self.os_morphing_tools._read_grub_config(config)

        mock_test_path_chroot.assert_called_once_with(config)
        mock_read_file_sudo.assert_called_once_with(config)
        self.assertEqual(result, {'key1': 'value1', 'key2': 'value2'})

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__read_grub_config_file_not_exists(
            self, mock_test_path_chroot, mock_read_file_sudo):
        mock_test_path_chroot.return_value = False

        self.assertRaises(IOError, self.os_morphing_tools._read_grub_config,
                          mock.sentinel.config)

        mock_read_file_sudo.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_grub_config')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__get_grub_config_obj_file_exists(
            self, mock_test_path_chroot, mock_exec_cmd_chroot,
            mock_read_grub_config):
        grub_conf = "/etc/default/grub"
        tmp_file = "/tmp/tmp_file"

        mock_test_path_chroot.return_value = True
        mock_exec_cmd_chroot.side_effect = [tmp_file.encode(), None]
        mock_read_grub_config.return_value = (
            mock_exec_cmd_chroot.return_value)

        result = self.os_morphing_tools._get_grub_config_obj(grub_conf)

        mock_test_path_chroot.assert_called_once_with(grub_conf)
        mock_exec_cmd_chroot.assert_has_calls([
            mock.call('mktemp'),
            mock.call('/bin/cp -fp %s %s' % (grub_conf, tmp_file))
        ])
        mock_read_grub_config.assert_called_once_with(tmp_file)

        expected_result = {
            'source': grub_conf,
            'location': tmp_file,
            'contents': mock_read_grub_config.return_value
        }

        self.assertEqual(result, expected_result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_grub_config')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__get_grub_config_obj_file_not_exists(
            self, mock_test_path_chroot, mock_exec_cmd_chroot,
            mock_read_grub_config):
        grub_conf = "/etc/default/grub"

        mock_test_path_chroot.return_value = False

        self.assertRaises(
            IOError, self.os_morphing_tools._get_grub_config_obj, grub_conf)

        mock_exec_cmd_chroot.assert_not_called()
        mock_read_grub_config.assert_not_called()

    def test__validate_grub_config_obj_not_dict(self):
        config_obj = "invalid config_obj"

        self.assertRaises(ValueError,
                          self.os_morphing_tools._validate_grub_config_obj,
                          config_obj)

    def test__validate_grub_config_obj_valid(self):
        config_obj = {
            'location': mock.sentinel.location,
            'source': mock.sentinel.source,
            'contents': mock.sentinel.contents
        }

        # Should not raise any exceptions
        self.os_morphing_tools._validate_grub_config_obj(config_obj)

    def test__validate_grub_config_obj_missing_keys(self):
        config_obj = {'location': mock.sentinel.location}

        self.assertRaises(ValueError,
                          self.os_morphing_tools._validate_grub_config_obj,
                          config_obj)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_set_grub_value_append(self, mock_exec_cmd_chroot,
                                   mock_read_file_sudo):
        option = 'option'
        value = 'value'
        config_obj = {
            'location': '/tmp/tmp_file',
            'source': '/etc/default/grub',
            'contents': {
                'GRUB_DEFAULT': '0'
            },
        }
        cfg = 'cfg'

        mock_read_file_sudo.return_value = cfg.encode()

        self.os_morphing_tools.set_grub_value(option, value, config_obj)

        mock_exec_cmd_chroot.assert_called_once_with(
            'sed -ie \'$a%s="%s"\' %s' % (
                option, value, config_obj['location'])
        )
        mock_read_file_sudo.assert_called_once_with(config_obj['location'])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_set_grub_value_replace(self, mock_exec_cmd_chroot,
                                    mock_read_file_sudo):
        option = 'option'
        value = 'value'
        config_obj = {
            'location': '/tmp/tmp_file',
            'source': '/etc/default/grub',
            'contents': {option: 'old_value'},
        }
        cfg = 'cfg'

        mock_read_file_sudo.return_value = cfg.encode()

        self.os_morphing_tools.set_grub_value(option, value, config_obj)

        mock_exec_cmd_chroot.assert_called_once_with(
            'sed -i \'s|^%s=.*|%s="%s"|g\' %s' % (option, option, value,
                                                  config_obj['location'])
        )
        mock_read_file_sudo.assert_called_once_with(config_obj['location'])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'set_grub_value')
    def test__set_grub2_cmdline_clobber(self, mock_set_grub_value):
        config_obj = {
            'contents': {
                'GRUB_CMDLINE_LINUX_DEFAULT': mock.sentinel.default,
                'GRUB_CMDLINE_LINUX': mock.sentinel.linux,
            },
        }
        options = ['option1', 'option2']

        self.os_morphing_tools._set_grub2_cmdline(config_obj, options,
                                                  clobber=True)

        mock_set_grub_value.assert_called_once_with(
            'GRUB_CMDLINE_LINUX', ' '.join(options), config_obj, replace=True)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'set_grub_value')
    def test__set_grub2_cmdline_add_options(self, mock_set_grub_value):
        config_obj = {
            'contents': {
                'GRUB_CMDLINE_LINUX_DEFAULT': 'quiet_default',
                'GRUB_CMDLINE_LINUX': 'quiet_linux',
            },
        }
        options = ['option1', 'option2']

        self.os_morphing_tools._set_grub2_cmdline(config_obj, options,
                                                  clobber=False)

        mock_set_grub_value.assert_called_once_with(
            'GRUB_CMDLINE_LINUX', 'quiet_linux option1 option2', config_obj,
            replace=True)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'set_grub_value')
    def test__set_grub2_cmdline_no_options_to_add(self, mock_set_grub_value):
        config_obj = {
            'contents': {
                'GRUB_CMDLINE_LINUX_DEFAULT': 'quiet_option1',
                'GRUB_CMDLINE_LINUX': 'quiet_option2',
            },
        }
        options = ['option1']

        self.os_morphing_tools._set_grub2_cmdline(config_obj, options,
                                                  clobber=False)
        mock_set_grub_value.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, 'get_update_grub2_command'
    )
    def test__execute_update_grub(self, mock_get_update_grub2_command,
                                  mock_exec_cmd_chroot):
        self.os_morphing_tools._execute_update_grub()

        mock_get_update_grub2_command.assert_called_once_with()
        mock_exec_cmd_chroot.assert_called_once_with(
            mock_get_update_grub2_command.return_value
        )

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_execute_update_grub')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__apply_grub2_config(self, mock_exec_cmd_chroot,
                                 mock_execute_update_grub):
        config_obj = {
            'location': mock.sentinel.location,
            'source': mock.sentinel.source,
            'contents': {
                'GRUB_DEFAULT': '0'
            },
        }

        self.os_morphing_tools._apply_grub2_config(config_obj,
                                                   execute_update_grub=True)

        mock_exec_cmd_chroot.assert_called_once_with(
            'mv -f %s %s' % (config_obj['location'], config_obj['source'])
        )
        mock_execute_update_grub.assert_called_once_with()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_execute_update_grub')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__apply_grub2_config_no_update_grub(self, mock_exec_cmd_chroot,
                                                mock_execute_update_grub):
        config_obj = {
            'location': mock.sentinel.location,
            'source': mock.sentinel.source,
            'contents': {
                'GRUB_DEFAULT': '0'
            },
        }

        self.os_morphing_tools._apply_grub2_config(config_obj,
                                                   execute_update_grub=False)

        mock_exec_cmd_chroot.assert_called_once_with(
            'mv -f %s %s' % (config_obj['location'], config_obj['source'])
        )
        mock_execute_update_grub.assert_not_called()

    def test__set_grub2_console_settings_invalid_parity(self):
        self.assertRaises(
            ValueError,
            self.os_morphing_tools._set_grub2_console_settings,
            parity='invalid_parity')

    def test__set_grub2_console_settings_invalid_consoles(self):
        self.assertRaises(
            ValueError, self.os_morphing_tools._set_grub2_console_settings,
            consoles='invalid_consoles')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_apply_grub2_config')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_set_grub2_cmdline')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'set_grub_value')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_get_grub_config_obj')
    def test__set_grub2_console_settings_all_params(
            self, mock_get_grub_config_obj, mock_set_grub_value,
            mock_set_grub2_cmdline, mock_apply_grub2_config):
        consoles = ['tty0', 'ttyS0']
        speed = 9600
        parity = 'odd'
        grub_conf = '/etc/default/grub'

        config_obj = {'location': grub_conf}
        mock_get_grub_config_obj.return_value = config_obj

        serial_cmd = base.GRUB2_SERIAL % (speed, parity)

        self.os_morphing_tools._set_grub2_console_settings(
            consoles, speed, parity, grub_conf,
            execute_update_grub=False)

        mock_get_grub_config_obj.assert_called_once_with(grub_conf)
        mock_set_grub_value.assert_called_once_with(
            'GRUB_SERIAL_COMMAND', serial_cmd, config_obj)
        mock_set_grub2_cmdline.assert_called_once_with(
            config_obj, ['console=tty0', 'console=ttyS0'])
        mock_apply_grub2_config.assert_called_once_with(
            config_obj, False)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_apply_grub2_config')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_set_grub2_cmdline')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'set_grub_value')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_get_grub_config_obj')
    def test__set_grub2_console_settings_default_params(
            self, mock_get_grub_config_obj, mock_set_grub_value,
            mock_set_grub2_cmdline, mock_apply_grub2_config):
        grub_conf = '/etc/default/grub'

        config_obj = {'location': grub_conf}
        mock_get_grub_config_obj.return_value = config_obj

        self.os_morphing_tools._set_grub2_console_settings(
            grub_conf=grub_conf, execute_update_grub=True)

        mock_get_grub_config_obj.assert_called_once_with(grub_conf)
        mock_set_grub_value.assert_called_once_with(
            'GRUB_SERIAL_COMMAND',
            'serial --word=8 --stop=1 --speed=115200 --parity=no --unit=0',
            config_obj)
        mock_set_grub2_cmdline.assert_called_once_with(
            config_obj, ['console=tty0', 'console=ttyS0'])
        mock_apply_grub2_config.assert_called_once_with(config_obj, True)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file_sudo')
    def test__add_net_udev_rules(self, mock_write_file_sudo, mock_test_path):
        mock_test_path.return_value = False
        net_ifaces_info = [
            ("eth0", "AA:BB:CC:DD:EE:FF"),
            ("eth1", "FF:EE:DD:CC:BB:AA")
        ]
        content = (
            'SUBSYSTEM=="net", ACTION=="add", DRIVERS=="?*", '
            'ATTR{address}=="aa:bb:cc:dd:ee:ff", NAME="eth0"\n'
            'SUBSYSTEM=="net", ACTION=="add", DRIVERS=="?*", '
            'ATTR{address}=="ff:ee:dd:cc:bb:aa", NAME="eth1"\n'
        )

        self.os_morphing_tools._add_net_udev_rules(net_ifaces_info)

        mock_write_file_sudo.assert_called_once_with(
            "etc/udev/rules.d/70-persistent-net.rules", content
        )

    @ddt.data(
        # (nics_info, expected_net_ifaces)
        (
            [
                {"mac_address": "AA:BB:CC:DD:EE:FF",
                 "ip_addresses": ["192.168.1.20"]},
                {"mac_address": "00:11:22:33:44:55",
                 "ip_addresses": ["192.168.1.11"]},
                {"mac_address": "11:22:33:44:55:66",
                 "ip_addresses": ["192.168.1.31"]},
                {"mac_address": None,
                 "ip_addresses": ["192.168.1.10"]},
                {"mac_address": "FF:FF:FF:FF:FF:FF",
                 "ip_addresses": ["192.168.1.10", "192.168.1.20"]}
            ],
            {
                "eth0": "FF:FF:FF:FF:FF:FF",
                "eth1": "FF:FF:FF:FF:FF:FF",
                "eth2": "11:22:33:44:55:66"
            }
        ),
    )
    @ddt.unpack
    def test__setup_network_preservation(self, nics_info, expected_net_ifaces):
        class FakeNetPreserver:
            def __init__(self, tool):
                self.tool = tool
                self.interface_info = {}

            def parse_network(self):
                self.interface_info = {
                    "eth0": {"mac_address": "00:11:22:33:44:55",
                             "ip_addresses": ["192.168.1.10"]},
                    "eth1": {"mac_address": "AA:BB:CC:DD:EE:FF",
                             "ip_addresses": ["192.168.1.20"]},
                    "eth2": {"mac_address": "11:22:33:44:55:66",
                             "ip_addresses": ["192.168.1.30"]},
                }

        with mock.patch(
            "coriolis.osmorphing.netpreserver.factory.get_net_preserver",
                return_value=FakeNetPreserver) as mock_get_np:

            self.os_morphing_tools._add_net_udev_rules = mock.MagicMock()

            self.os_morphing_tools._setup_network_preservation(nics_info)

            result_net_ifaces = dict(
                self.os_morphing_tools._add_net_udev_rules.call_args[0][0])

            mock_get_np.assert_called_once_with(self.os_morphing_tools)
            self.os_morphing_tools._add_net_udev_rules.assert_called_once()
            self.assertEqual(result_net_ifaces, expected_net_ifaces)
