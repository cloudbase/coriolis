# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
import shlex
from unittest import mock

import ddt
from packaging.version import Version

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
        environment = {'http_proxy': 'http://10.0.0.1:3128'}

        self.os_morphing_tools.set_environment(environment)

        self.assertEqual(
            {'http_proxy': 'http://10.0.0.1:3128'},
            self.os_morphing_tools._environment)

    def test_set_environment_preserves_constructor_variables(self):
        """Variables declared by the constructor survive set_environment."""
        self.os_morphing_tools._environment['DEBIAN_FRONTEND'] = (
            'noninteractive')

        self.os_morphing_tools.set_environment(
            {'http_proxy': 'http://10.0.0.1:3128'})

        self.assertEqual(
            {'DEBIAN_FRONTEND': 'noninteractive',
             'http_proxy': 'http://10.0.0.1:3128'},
            self.os_morphing_tools._environment)


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
        self.joined_chroot_path = '/root/root/etc/resolv.conf'
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

    @ddt.data(
        ("8", 8),
        ("8.0", 8),
        ("8.10", 8),
        ("9", 9),
        ("9.4", 9),
        ("10", 10),
        ("10.0", 10),
        ("10.10", 10),
    )
    @ddt.unpack
    def test__parse_version_util(self, version, expected_major):
        result = self.os_morphing_tools._parse_version_util(version)
        self.assertIsInstance(result, Version)
        self.assertEqual(result.major, expected_major)

    @ddt.data(
        "",
        None,
        "abc",
        "x10",
    )
    def test__parse_version_util_invalid(self, version):
        self.assertRaises(
            ValueError,
            self.os_morphing_tools._parse_version_util, version)

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
                      get_pty=False),
            mock.call(self.conn, 'sudo "%s" "%s"' % (
                script_path, self.os_morphing_tools._os_root_dir),
                get_pty=False)])

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

    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_run_scheduled_grub2_update')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_restore_resolv_conf')
    def test_post_packages_install(self, mock_restore_resolv_conf,
                                   mock_run_scheduled_grub2_update):
        self.os_morphing_tools.post_packages_install(
            mock.sentinel.package_name)
        mock_run_scheduled_grub2_update.assert_called_once_with()
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

        mock_test_ssh_path.assert_called_once_with(
            self.os_morphing_tools._ssh, self.joined_chroot_path)
        self.assertEqual(result, mock_test_ssh_path.return_value)

    @mock.patch.object(base.utils, 'read_ssh_file')
    def test__read_file(self, mock_read_ssh_file):
        result = self.os_morphing_tools._read_file(self.chroot_path)

        mock_read_ssh_file.assert_called_once_with(
            self.os_morphing_tools._ssh, self.joined_chroot_path)
        self.assertEqual(result, mock_read_ssh_file.return_value)

    @mock.patch.object(base.utils, 'write_ssh_file')
    def test__write_file(self, mock_write_ssh_file):
        self.os_morphing_tools._write_file(
            self.chroot_path, mock.sentinel.content)

        mock_write_ssh_file.assert_called_once_with(
            self.os_morphing_tools._ssh, self.joined_chroot_path,
            mock.sentinel.content)

    @mock.patch.object(base.utils, 'list_ssh_dir')
    def test__list_dir(self, mock_list_ssh_dir):
        result = self.os_morphing_tools._list_dir(self.chroot_path)

        mock_list_ssh_dir.assert_called_once_with(
            self.os_morphing_tools._ssh, self.joined_chroot_path)
        self.assertEqual(result, mock_list_ssh_dir.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test__exec_cmd(self, mock_exec_ssh_cmd):
        result = self.os_morphing_tools._exec_cmd(
            mock.sentinel.cmd, timeout=120)

        mock_exec_ssh_cmd.assert_called_once_with(
            self.os_morphing_tools._ssh, mock.sentinel.cmd,
            environment=self.os_morphing_tools._environment, get_pty=False,
            timeout=120)

        self.assertEqual(result, mock_exec_ssh_cmd.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test__exec_cmd_without_timeout(self, mock_exec_ssh_cmd):
        result = self.os_morphing_tools._exec_cmd(mock.sentinel.cmd)

        mock_exec_ssh_cmd.assert_called_once_with(
            self.os_morphing_tools._ssh, mock.sentinel.cmd,
            environment=self.os_morphing_tools._environment, get_pty=False,
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
            get_pty=False, timeout=120)
        self.assertEqual(result, mock_exec_ssh_cmd_chroot.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd_chroot')
    def test__exec_cmd_chroot_without_timeout(self, mock_exec_ssh_cmd_chroot):
        result = self.os_morphing_tools._exec_cmd_chroot(mock.sentinel.cmd)

        mock_exec_ssh_cmd_chroot.assert_called_once_with(
            self.os_morphing_tools._ssh, self.os_morphing_tools._os_root_dir,
            mock.sentinel.cmd, environment=self.os_morphing_tools._environment,
            get_pty=False,
            timeout=self.os_morphing_tools._osmorphing_operation_timeout)
        self.assertEqual(result, mock_exec_ssh_cmd_chroot.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd_chroot')
    def test__exec_cmd_chroot_with_exception(self, mock_exec_ssh_cmd_chroot):
        mock_exec_ssh_cmd_chroot.side_effect = [
            exception.MinionMachineCommandTimeout()]

        self.assertRaises(
            exception.OSMorphingSSHOperationTimeout,
            self.os_morphing_tools._exec_cmd_chroot, mock.sentinel.cmd)

    @mock.patch.object(base.utils, 'exec_ssh_cmd_chroot')
    def test__exec_cmd_chroot_environment(self, mock_exec_ssh_cmd_chroot):
        """The tools' environment is handed over to the chrooted command."""
        environment = {
            'http_proxy': 'http://10.0.0.1:3128',
            'DEBIAN_FRONTEND': 'noninteractive'}
        self.os_morphing_tools._environment = environment

        self.os_morphing_tools._exec_cmd_chroot(mock.sentinel.cmd)

        mock_exec_ssh_cmd_chroot.assert_called_once_with(
            self.os_morphing_tools._ssh, self.os_morphing_tools._os_root_dir,
            mock.sentinel.cmd, environment=environment, get_pty=False,
            timeout=self.os_morphing_tools._osmorphing_operation_timeout)

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
            self.os_morphing_tools._environment, get_pty=False)

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

        mock_read_ssh_ini.assert_called_once_with(
            self.os_morphing_tools._ssh, self.joined_chroot_path,
            check_exists=False)
        self.assertEqual(result, mock_read_ssh_ini.return_value)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file_sudo')
    def test__read_config_file_sudo(
            self, mock_read_file_sudo, mock_test_path_chroot):
        mock_test_path_chroot.return_value = True
        mock_read_file_sudo.return_value = '[connection]\ntype=ethernet'

        result = self.os_morphing_tools._read_config_file_sudo(
            self.chroot_path)

        mock_read_file_sudo.assert_called_once_with(self.chroot_path)
        self.assertEqual(result, {'type': 'ethernet'})

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file_sudo')
    def test__read_config_file_none(
            self, mock_read_file_sudo, mock_test_path_chroot):
        mock_test_path_chroot.return_value = False

        result = self.os_morphing_tools._read_config_file_sudo(
            self.chroot_path)

        mock_read_file_sudo.assert_not_called()
        self.assertEqual(result, {})

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file_sudo')
    def test__read_config_file_sudo_raises(
            self, mock_read_file_sudo, mock_test_path_chroot):
        mock_test_path_chroot.return_value = False

        self.assertRaises(
            IOError,
            self.os_morphing_tools._read_config_file_sudo,
            self.chroot_path,
            check_exists=True)

        mock_read_file_sudo.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_list_dir')
    def test__get_net_config_files(self, mock_list_dir):
        network_scripts_path = (
            base.BaseLinuxOSMorphingTools._NETWORK_SCRIPTS_PATH)
        mock_list_dir.return_value = ['ifcfg-eth0', 'ifcfg-lo', 'other-file']

        result = self.os_morphing_tools._get_net_config_files(
            network_scripts_path)

        mock_list_dir.assert_called_once_with(network_scripts_path)
        self.assertEqual(
            result,
            [
                'etc/sysconfig/network-scripts/ifcfg-eth0',
                'etc/sysconfig/network-scripts/ifcfg-lo',
            ])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_config_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_get_net_config_files')
    def test__get_ifcfgs_by_type(self, mock_get_net_config_files,
                                 mock_read_config_file_sudo):
        network_scripts_path = (
            base.BaseLinuxOSMorphingTools._NETWORK_SCRIPTS_PATH)
        mock_get_net_config_files.return_value = [mock.sentinel.ifcfg_file]
        mock_read_config_file_sudo.side_effect = [{"TYPE": "Ethernet"}]

        result = self.os_morphing_tools._get_ifcfgs_by_type(
            "Ethernet", network_scripts_path)

        mock_get_net_config_files.assert_called_once_with(network_scripts_path)
        mock_read_config_file_sudo.assert_called_once_with(
            mock.sentinel.ifcfg_file)
        self.assertEqual(
            result, [(mock.sentinel.ifcfg_file, {"TYPE": "Ethernet"})])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_config_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_get_net_config_files')
    def test__get_ifcfgs_by_type_default_type(self, mock_get_net_config_files,
                                              mock_read_config_file_sudo):
        network_scripts_path = (
            base.BaseLinuxOSMorphingTools._NETWORK_SCRIPTS_PATH)
        mock_get_net_config_files.return_value = [mock.sentinel.ifcfg_file]
        mock_read_config_file_sudo.side_effect = [{}]

        result = self.os_morphing_tools._get_ifcfgs_by_type(
            "Ethernet", network_scripts_path)

        self.assertEqual(
            result, [(mock.sentinel.ifcfg_file, {"TYPE": "Ethernet"})])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_list_dir')
    def test__get_nmconnection_files(self, mock_list_dir):
        network_scripts_path = (
            base.BaseLinuxOSMorphingTools._NM_CONNECTIONS_PATH)
        mock_list_dir.return_value = [
            'eth0.nmconnection', 'eth1.nmconnection', 'other-file']

        result = self.os_morphing_tools._get_nmconnection_files(
            network_scripts_path)

        mock_list_dir.assert_called_once_with(network_scripts_path)
        self.assertEqual(
            result,
            [
                'etc/NetworkManager/system-connections/eth0.nmconnection',
                'etc/NetworkManager/system-connections/eth1.nmconnection',
            ])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_config_file_sudo')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_get_nmconnection_files')
    def test__get_keyfiles_by_type(self, mock_get_nmconnection_files,
                                   mock_read_config_file_sudo):
        network_scripts_path = (
            base.BaseLinuxOSMorphingTools._NM_CONNECTIONS_PATH)
        mock_get_nmconnection_files.return_value = [mock.sentinel.nmconn_file]
        mock_read_config_file_sudo.side_effect = [{"type": "ethernet"}]

        result = self.os_morphing_tools._get_keyfiles_by_type(
            "ethernet", network_scripts_path)

        mock_get_nmconnection_files.assert_called_once_with(
            network_scripts_path)
        mock_read_config_file_sudo.assert_called_once_with(
            mock.sentinel.nmconn_file)
        self.assertEqual(
            result, [(mock.sentinel.nmconn_file, {"type": "ethernet"})])

    def test__get_ifcfg_nm_controlled_default(self):
        # No minimum version configured -> never NetworkManager-controlled.
        self.assertIsNone(
            self.os_morphing_tools._IFCFG_NM_CONTROLLED_MIN_VERSION)

        self.assertEqual(
            "no", self.os_morphing_tools._get_ifcfg_nm_controlled())

    def test__get_ifcfg_nm_controlled_below_minimum(self):
        self.os_morphing_tools._IFCFG_NM_CONTROLLED_MIN_VERSION = 8
        self.os_morphing_tools._version = "7"

        self.assertEqual(
            "no", self.os_morphing_tools._get_ifcfg_nm_controlled())

    def test__get_ifcfg_nm_controlled_at_or_above_minimum(self):
        self.os_morphing_tools._IFCFG_NM_CONTROLLED_MIN_VERSION = 8
        self.os_morphing_tools._version = "9"

        self.assertEqual(
            "yes", self.os_morphing_tools._get_ifcfg_nm_controlled())

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__backup_nmconnection_files_explicit_list(
            self, mock_exec_cmd_chroot):
        nmconnection_files = [
            'etc/NetworkManager/system-connections/eth0.nmconnection',
            'etc/NetworkManager/system-connections/eth1.nmconnection']

        with self.assertLogs('coriolis.osmorphing.base', level=logging.DEBUG):
            self.os_morphing_tools._backup_nmconnection_files(
                nmconnection_files)

        mock_exec_cmd_chroot.assert_has_calls([
            mock.call(
                'mv "etc/NetworkManager/system-connections/eth0.nmconnection" '
                '"etc/NetworkManager/system-connections/eth0.nmconnection.bak"'
            ),
            mock.call(
                'mv "etc/NetworkManager/system-connections/eth1.nmconnection" '
                '"etc/NetworkManager/system-connections/eth1.nmconnection.bak"'
            ),
        ])

    @mock.patch.object(
        base.BaseLinuxOSMorphingTools,
        '_get_existing_ethernet_nmconnection_files')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__backup_nmconnection_files_fetches_files(
            self, mock_exec_cmd_chroot,
            mock_get_existing_ethernet_nmconnection_files):
        mock_get_existing_ethernet_nmconnection_files.return_value = [
            'etc/NetworkManager/system-connections/eth0.nmconnection']

        with self.assertLogs('coriolis.osmorphing.base', level=logging.DEBUG):
            self.os_morphing_tools._backup_nmconnection_files()

        mock_get_existing_ethernet_nmconnection_files.assert_called_once_with()
        mock_exec_cmd_chroot.assert_called_once_with(
            'mv "etc/NetworkManager/system-connections/eth0.nmconnection" '
            '"etc/NetworkManager/system-connections/eth0.nmconnection.bak"')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_get_ifcfgs_by_type')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test__backup_ethernet_ifcfg_configs(
            self, mock_test_path, mock_exec_cmd_chroot,
            mock_get_ifcfgs_by_type):
        mock_test_path.return_value = True
        mock_get_ifcfgs_by_type.return_value = [
            ("etc/sysconfig/network-scripts/ifcfg-eth0", {}),
            # ifcfg-lo must be skipped.
            ("etc/sysconfig/network-scripts/ifcfg-lo", {}),
        ]

        with self.assertLogs('coriolis.osmorphing.base', level=logging.DEBUG):
            self.os_morphing_tools._backup_ethernet_ifcfg_configs()

        mock_exec_cmd_chroot.assert_called_once_with(
            'mv "etc/sysconfig/network-scripts/ifcfg-eth0" '
            '"etc/sysconfig/network-scripts/ifcfg-eth0.bak"')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_get_ifcfgs_by_type')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test__backup_ethernet_ifcfg_configs_no_dir(
            self, mock_test_path, mock_exec_cmd_chroot,
            mock_get_ifcfgs_by_type):
        mock_test_path.return_value = False

        self.os_morphing_tools._backup_ethernet_ifcfg_configs()

        mock_get_ifcfgs_by_type.assert_not_called()
        mock_exec_cmd_chroot.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file_sudo')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_backup_ethernet_ifcfg_configs')
    def test__write_nic_configs(
            self, mock_backup_ethernet_ifcfg_configs, mock_write_file_sudo):
        nics_info = [{'name': 'eth0'}, {'name': 'eth1'}]

        self.os_morphing_tools._write_nic_configs(nics_info)

        mock_backup_ethernet_ifcfg_configs.assert_called_once_with()
        mock_write_file_sudo.assert_has_calls([
            mock.call(
                "etc/sysconfig/network-scripts/ifcfg-eth0",
                base.IFCFG_TEMPLATE % {
                    "device_name": "eth0",
                    "nm_controlled": "no",
                }),
            mock.call(
                "etc/sysconfig/network-scripts/ifcfg-eth1",
                base.IFCFG_TEMPLATE % {
                    "device_name": "eth1",
                    "nm_controlled": "no",
                }),
        ])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file_sudo')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_backup_ethernet_ifcfg_configs')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_backup_nmconnection_files')
    def test__write_nmconnection_configs(
            self, mock_backup_nmconnection_files,
            mock_backup_ethernet_ifcfg_configs, mock_write_file_sudo,
            mock_exec_cmd_chroot):
        nics_info = [{'name': 'eth0'}]
        nmconnection_files = [
            'etc/NetworkManager/system-connections/eth0.nmconnection']

        self.os_morphing_tools._write_nmconnection_configs(
            nics_info, nmconnection_files)

        mock_backup_nmconnection_files.assert_called_once_with(
            nmconnection_files)
        mock_backup_ethernet_ifcfg_configs.assert_called_once_with()
        mock_write_file_sudo.assert_called_once()
        args, _ = mock_write_file_sudo.call_args
        self.assertEqual(
            args[0],
            "etc/NetworkManager/system-connections/eth0.nmconnection")
        self.assertIn("[connection]", args[1])
        self.assertIn("interface-name=eth0", args[1])
        mock_exec_cmd_chroot.assert_called_once_with(
            "chmod 600 /etc/NetworkManager/system-connections/"
            "eth0.nmconnection")

    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_backup_nmconnection_files')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_backup_ethernet_ifcfg_configs')
    def test__write_nmconnection_configs_no_nics(
            self, mock_backup_ethernet_ifcfg_configs,
            mock_backup_nmconnection_files):
        self.os_morphing_tools._write_nmconnection_configs(None, None)

        mock_backup_nmconnection_files.assert_not_called()
        mock_backup_ethernet_ifcfg_configs.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd')
    def test__copy_resolv_conf(self, mock_exec_cmd, mock_test_path):
        mocked_resolv_conf = "etc/resolv.conf"
        self.os_morphing_tools._os_root_dir = "/root"
        mocked_full_path = "/root/etc/resolv.conf"
        mocked_full_path_old = "/root/etc/resolv.conf.old"
        mock_test_path.return_value = True

        self.os_morphing_tools._copy_resolv_conf()

        mock_test_path.assert_called_once_with(mocked_resolv_conf)
        mock_exec_cmd.assert_has_calls([
            mock.call('sudo mv -f %s %s' % (
                mocked_full_path, mocked_full_path_old)),
            mock.call('sudo cp -L --remove-destination /etc/resolv.conf %s' %
                      mocked_full_path)])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd')
    def test__restore_resolv_conf(self, mock_exec_cmd, mock_test_path):
        mocked_resolv_conf_old = "etc/resolv.conf.old"
        self.os_morphing_tools._os_root_dir = "/root"
        mocked_full_path = "/root/etc/resolv.conf"
        mocked_full_path_old = "/root/etc/resolv.conf.old"
        mock_test_path.return_value = True

        self.os_morphing_tools._restore_resolv_conf()

        mock_test_path.assert_called_once_with(mocked_resolv_conf_old)
        mock_exec_cmd.assert_called_once_with(
            'sudo mv -f %s %s' % (mocked_full_path_old, mocked_full_path))

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

    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_write_file_sudo")
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_exec_cmd_chroot")
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_test_path")
    def test__write_cloud_init_mods_config(
            self, mock__test_path, mock__exec_cmd_chroot,
            mock__write_file_sudo):
        mock__test_path.return_value = True
        cloud_cfg = {
            "mock_key1": {"mock_key2": "mock_value1"},
            "mock_key3": "mock_value2"}

        self.os_morphing_tools._write_cloud_init_mods_config(cloud_cfg)

        mock__exec_cmd_chroot.assert_not_called()
        mock__write_file_sudo.assert_called_once_with(
            "/etc/cloud/cloud.cfg.d/99_coriolis.cfg",
            'mock_key1:\n  mock_key2: mock_value1\nmock_key3: mock_value2\n')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_write_file_sudo")
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_exec_cmd_chroot")
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_test_path")
    def test__write_cloud_init_mods_config_no_directory(
            self, mock__test_path, mock__exec_cmd_chroot,
            mock__write_file_sudo):
        mock__test_path.return_value = False
        cloud_cfg = {}

        self.os_morphing_tools._write_cloud_init_mods_config(cloud_cfg)

        mock__exec_cmd_chroot.assert_called_once_with(
            "mkdir -p /etc/cloud/cloud.cfg.d")
        mock__write_file_sudo.assert_called_once_with(
            "/etc/cloud/cloud.cfg.d/99_coriolis.cfg",
            '{}\n')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_exec_cmd_chroot")
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_test_path")
    def test__disable_installer_cloud_config(
            self, mock__test_path, mock__exec_cmd_chroot):
        mock__test_path.return_value = True

        self.os_morphing_tools._disable_installer_cloud_config()

        mock__exec_cmd_chroot.assert_called_once_with(
            "mv /etc/cloud/cloud.cfg.d/99-installer.cfg "
            "/etc/cloud/cloud.cfg.d/99-installer.cfg.bak")

    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_exec_cmd_chroot")
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_test_path")
    def test__disable_installer_cloud_config_no_file(
            self, mock__test_path, mock__exec_cmd_chroot):
        mock__test_path.return_value = False

        self.os_morphing_tools._disable_installer_cloud_config()

        mock__exec_cmd_chroot.assert_not_called()

    @ddt.data(
        ((False, False, False), "", [], False),
        ((True, True, False), "", [
            "rm /etc/cloud/cloud-init.disabled",
            "sed -i '/cloud-init=disabled/d' /etc/systemd/system.conf",
        ], False),
        ((False, False, True),
         'GRUB_CMDLINE_LINUX="console=ttyS0 cloud-init=disabled"',
         ["sed -i '/cloud-init=disabled/d' /etc/default/grub"],
         True),
        ((False, False, True),
         'GRUB_CMDLINE_LINUX="console=ttyS0"',
         [],
         False),
    )
    @ddt.unpack
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_read_file_sudo")
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_schedule_grub2_update")
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_exec_cmd_chroot")
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_test_path")
    def test__ensure_cloud_init_not_disabled(
            self, test_path_results, grub_defaults_contents, expected_cmds,
            updates_grub, mock__test_path, mock__exec_cmd_chroot,
            mock__schedule_grub2_update, mock__read_file_sudo):
        mock__test_path.side_effect = test_path_results
        mock__read_file_sudo.return_value = grub_defaults_contents

        self.os_morphing_tools._ensure_cloud_init_not_disabled()

        called_cmds = [
            call.args[0] for call in mock__exec_cmd_chroot.call_args_list]
        self.assertEqual(called_cmds, expected_cmds)
        if updates_grub:
            mock__schedule_grub2_update.assert_called_once()
        else:
            mock__schedule_grub2_update.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_exec_cmd_chroot")
    def test__reset_cloud_init_run(self, mock__exec_cmd_chroot):
        self.os_morphing_tools._reset_cloud_init_run()

        mock__exec_cmd_chroot.assert_called_once_with(
            "cloud-init clean --logs")

    @ddt.data(
        (False, None, None),
        (True, "other_modules:\n  - not_update_etc_hosts\n"
         "cloud_init_modules:\n  - set_hostname\n  - update_etc_hosts\n",
         ["set_hostname", "update_etc_hosts"]),
        (True, "other_modules:\n  - update_etc_hosts\n", [])
    )
    @ddt.unpack
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_read_file_sudo")
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_test_path")
    def test__get_cloud_init_modules(
            self, test_path_result, file_content,
            expected_result, mock__test_path, mock__read_file_sudo):
        mock__test_path.return_value = test_path_result
        mock__read_file_sudo.return_value = file_content

        result = self.os_morphing_tools._get_cloud_init_modules()

        self.assertEqual(result, expected_result)

    @ddt.data(
        (False, None, base.DEFAULT_CLOUD_USER),
        (True, "system_info:\n  default_user:\n    name: mock_user\n",
         "mock_user"),
        (True, "{}", base.DEFAULT_CLOUD_USER),
    )
    @ddt.unpack
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_read_file_sudo")
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_test_path")
    def test__get_default_cloud_user(
            self, test_path_result, file_content, expected_user,
            mock__test_path, mock__read_file_sudo):
        mock__test_path.return_value = test_path_result
        mock__read_file_sudo.return_value = file_content

        result = self.os_morphing_tools._get_default_cloud_user()

        self.assertEqual(result, expected_user)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_exec_cmd_chroot")
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_check_user_exists")
    @mock.patch.object(base.BaseLinuxOSMorphingTools,
                       "_get_default_cloud_user")
    def test__create_cloudinit_user(
            self, mock__get_default_cloud_user,
            mock__check_user_exists, mock__exec_cmd_chroot):
        mock__get_default_cloud_user.return_value = "mock_user"
        mock__check_user_exists.return_value = False

        self.os_morphing_tools._create_cloudinit_user()

        mock__exec_cmd_chroot.assert_called_once_with("useradd mock_user")

    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_exec_cmd_chroot")
    @mock.patch.object(base.BaseLinuxOSMorphingTools, "_check_user_exists")
    @mock.patch.object(base.BaseLinuxOSMorphingTools,
                       "_get_default_cloud_user")
    def test__create_cloudinit_user_already_exists(
            self, mock__get_default_cloud_user,
            mock__check_user_exists, mock__exec_cmd_chroot):
        mock__get_default_cloud_user.return_value = "mock_user"
        mock__check_user_exists.return_value = True

        self.os_morphing_tools._create_cloudinit_user()

        mock__exec_cmd_chroot.assert_not_called()

    @ddt.data(
        (
            None,
            ["vim"],
            {},
            False,
            [],
            None,
            False
        ),
        (
            ["Oracle"],
            ["cloud-init"],
            {"retain_user_credentials": True, "set_dhcp": False},
            False,
            ["set_hostname"],
            {
                "disable_root": False,
                "ssh_pwauth": True,
                "users": [],
                "network": {"config": "disabled"},
                "datasource_list": ["Oracle"]
            },
            True
        ),
        (
            [],
            ["cloud-init", "vim"],
            {"retain_user_credentials": False, "set_dhcp": True},
            True,
            [],
            {},
            False
        ),
        (
            [],
            ["cloud-init", "vim"],
            {"retain_user_credentials": False, "set_dhcp": False},
            True,
            ["update_etc_hosts", "set_hostname", "write_files"],
            {
                "network": {"config": "disabled"},
                "cloud_init_modules": ["set_hostname", "write_files"]
            },
            False
        ),
        (
            ["ConfigDrive", "OpenStack"],
            ["cloud-init", "vim"],
            {"retain_user_credentials": False, "set_dhcp": True},
            True,
            [],
            {
                "datasource_list": ["ConfigDrive", "OpenStack"]
            },
            False
        ),
    )
    @ddt.unpack
    @mock.patch.object(base.BaseLinuxOSMorphingTools,
                       '_enable_systemd_service')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_has_systemd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools,
                       '_get_cloud_init_modules')
    @mock.patch.object(base.BaseLinuxOSMorphingTools,
                       '_write_cloud_init_mods_config')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_create_cloudinit_user')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_reset_cloud_init_run')
    @mock.patch.object(base.BaseLinuxOSMorphingTools,
                       '_ensure_cloud_init_not_disabled')
    @mock.patch.object(base.BaseLinuxOSMorphingTools,
                       '_disable_installer_cloud_config')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'get_packages')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'datasource_list',
                       new_callable=mock.PropertyMock)
    def test__configure_cloud_init(
            self, datasource_list, returned_packages, osmorphing_params,
            creates_cloudinit_user, cloud_init_modules, expected_result,
            has_systemd_chroot, mock_datasource_list, mock_get_packages,
            mock__disable_installer_cloud_config,
            mock__ensure_cloud_init_not_disabled, mock__reset_cloud_init_run,
            mock__create_cloudinit_user, mock__write_cloud_init_mods_config,
            mock__get_cloud_init_modules,
            mock__has_systemd_chroot, mock__enable_systemd_service
    ):
        if datasource_list is not None:
            mock_datasource_list.return_value = datasource_list
        mock_get_packages.return_value = returned_packages
        self.os_morphing_tools._osmorphing_parameters = osmorphing_params
        mock__has_systemd_chroot.return_value = has_systemd_chroot
        mock__get_cloud_init_modules.return_value = cloud_init_modules

        self.os_morphing_tools._configure_cloud_init()

        if expected_result is not None:
            mock__ensure_cloud_init_not_disabled.assert_called_once()
            mock__reset_cloud_init_run.assert_called_once()
            mock__write_cloud_init_mods_config.assert_called_once_with(
                expected_result)
            if creates_cloudinit_user:
                mock__create_cloudinit_user.assert_called_once()
            else:
                mock__create_cloudinit_user.assert_not_called()
            if has_systemd_chroot:
                mock__enable_systemd_service.assert_called_once_with(
                    "cloud-init")
            else:
                mock__enable_systemd_service.assert_not_called()
        else:
            mock__disable_installer_cloud_config.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__test_path_chroot(self, mock_exec_cmd_chroot):
        path = "/tmp/test_path"
        mock_exec_cmd_chroot.return_value = "1\n"

        result = self.os_morphing_tools._test_path_chroot(path)

        mock_exec_cmd_chroot.assert_called_once_with(
            '[ -f "%s" ] && echo 1 || echo 0' % path)
        self.assertTrue(result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__test_path_chroot_no_leading_slash(self, mock_exec_cmd_chroot):
        path = "tmp/test_path"
        mock_exec_cmd_chroot.return_value = "1\n"

        result = self.os_morphing_tools._test_path_chroot(path)

        mock_exec_cmd_chroot.assert_called_once_with(
            '[ -f "/%s" ] && echo 1 || echo 0' % path)
        self.assertTrue(result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__read_file_sudo(self, mock_exec_cmd_chroot):
        chroot_path = "/tmp/test_path"

        result = self.os_morphing_tools._read_file_sudo(chroot_path)

        mock_exec_cmd_chroot.assert_called_once_with('cat "%s"' % chroot_path)
        self.assertEqual(result, mock_exec_cmd_chroot.return_value)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__read_file_sudo_no_leading_slash(self, mock_exec_cmd_chroot):
        chroot_path = "tmp/test_path"

        result = self.os_morphing_tools._read_file_sudo(chroot_path)

        mock_exec_cmd_chroot.assert_called_once_with('cat "/%s"' % chroot_path)
        self.assertEqual(result, mock_exec_cmd_chroot.return_value)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__read_grub_config_file_exists(self, mock_test_path_chroot,
                                           mock_read_file_sudo):
        config = mock.sentinel.config
        file_contents = 'key1="value1"\n#comment\nkey2="value2"\ninvalid_line'

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
        mock_exec_cmd_chroot.side_effect = [tmp_file, None]
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

        mock_read_file_sudo.return_value = cfg

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

        mock_read_file_sudo.return_value = cfg

        self.os_morphing_tools.set_grub_value(option, value, config_obj)

        mock_exec_cmd_chroot.assert_called_once_with(
            'sed -i \'s|^%s=.*|%s="%s"|g\' %s' % (option, option, value,
                                                  config_obj['location'])
        )
        mock_read_file_sudo.assert_called_once_with(config_obj['location'])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file_sudo')
    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test_set_grub_value_serial_command_is_a_single_sed_argument(
            self, mock_exec_ssh_cmd, mock_read_file_sudo):
        """Regression test for the GRUB_SERIAL_COMMAND quoting bug.

        The appended value holds spaces, double quotes and '--word=8'-style
        text. It must reach sed as one single argument; previously it was
        word-split into separate sed options and failed with
        "sed: unrecognized option '--word=8'".
        """
        self.os_morphing_tools._environment = {
            'http_proxy': 'http://10.0.0.1:3128'}
        self.os_morphing_tools._os_root_dir = '/tmp/tmp.q15QdW45qE'
        serial_cmd = base.GRUB2_SERIAL % (115200, "no")
        config_obj = {
            'location': '/tmp/tmp.OIK95wgYUb',
            'source': '/etc/default/grub',
            'contents': {'GRUB_DEFAULT': '0'},
        }

        self.os_morphing_tools.set_grub_value(
            'GRUB_SERIAL_COMMAND', serial_cmd, config_obj)

        argv = shlex.split(mock_exec_ssh_cmd.call_args[0][1])
        self.assertEqual(
            ['sudo', 'env', 'http_proxy=http://10.0.0.1:3128', 'chroot',
             '/tmp/tmp.q15QdW45qE', 'sed', '-ie',
             '$aGRUB_SERIAL_COMMAND="%s"' % serial_cmd,
             '/tmp/tmp.OIK95wgYUb'],
            argv)
        # None of the GRUB value may end up as a standalone sed option.
        self.assertNotIn('--word=8', argv)
        self.assertEqual([], [a for a in argv if a.startswith('--')])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_set_grub_value_with_embedded_quotes(self, mock_exec_cmd_chroot,
                                                 mock_read_file_sudo):
        """Values holding quotes must not break out of the sed script."""
        config_obj = {
            'location': '/tmp/tmp_file',
            'source': '/etc/default/grub',
            'contents': {'GRUB_DEFAULT': '0'},
        }

        self.os_morphing_tools.set_grub_value(
            'GRUB_CMDLINE_LINUX', "quiet 'splash'", config_obj)

        self.assertEqual(
            ['sed', '-ie', '$aGRUB_CMDLINE_LINUX="quiet \'splash\'"',
             '/tmp/tmp_file'],
            shlex.split(mock_exec_cmd_chroot.call_args[0][0]))

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

    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_get_grub_default_conf',
        return_value=None,
    )
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, 'get_update_grub2_command'
    )
    def test__execute_update_grub(
            self,
            mock_get_update_grub2_command,
            mock_exec_cmd_chroot,
            _mock_get_grub_default_conf):
        self.os_morphing_tools._execute_update_grub()

        mock_get_update_grub2_command.assert_called_once_with()
        mock_exec_cmd_chroot.assert_called_once_with(
            mock_get_update_grub2_command.return_value
        )

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__get_grub_default_conf_exists(self, mock_test_path_chroot):
        mock_test_path_chroot.return_value = True

        result = self.os_morphing_tools._get_grub_default_conf()

        mock_test_path_chroot.assert_called_once_with('/etc/default/grub')
        self.assertEqual(result, '/etc/default/grub')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__get_grub_default_conf_missing(self, mock_test_path_chroot):
        mock_test_path_chroot.return_value = False

        result = self.os_morphing_tools._get_grub_default_conf()

        mock_test_path_chroot.assert_called_once_with('/etc/default/grub')
        self.assertIsNone(result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__set_grub_os_prober_setting_remove(
            self, mock_exec_cmd_chroot):
        grub_conf = '/etc/default/grub'

        self.os_morphing_tools._set_grub_os_prober_setting(grub_conf, None)

        mock_exec_cmd_chroot.assert_called_once_with(
            "sed -i '/^GRUB_DISABLE_OS_PROBER=/d' %s" % grub_conf)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_apply_grub2_config')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'set_grub_value')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_get_grub_config_obj')
    def test__set_grub_os_prober_setting_append(
            self, mock_get_grub_config_obj, mock_set_grub_value,
            mock_apply_grub2_config):
        grub_conf = '/etc/default/grub'
        config_obj = {
            'location': mock.sentinel.location,
            'source': grub_conf,
            'contents': {},
        }
        mock_get_grub_config_obj.return_value = config_obj

        self.os_morphing_tools._set_grub_os_prober_setting(
            grub_conf, 'true')

        mock_get_grub_config_obj.assert_called_once_with(grub_conf)
        mock_set_grub_value.assert_called_once_with(
            'GRUB_DISABLE_OS_PROBER', 'true', config_obj, replace=False)
        mock_apply_grub2_config.assert_called_once_with(
            config_obj, execute_update_grub=False)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_apply_grub2_config')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'set_grub_value')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_get_grub_config_obj')
    def test__set_grub_os_prober_setting_replace(
            self, mock_get_grub_config_obj, mock_set_grub_value,
            mock_apply_grub2_config):
        grub_conf = '/etc/default/grub'
        config_obj = {
            'location': mock.sentinel.location,
            'source': grub_conf,
            'contents': {'GRUB_DISABLE_OS_PROBER': 'false'},
        }
        mock_get_grub_config_obj.return_value = config_obj

        self.os_morphing_tools._set_grub_os_prober_setting(
            grub_conf, 'true')

        mock_get_grub_config_obj.assert_called_once_with(grub_conf)
        mock_set_grub_value.assert_called_once_with(
            'GRUB_DISABLE_OS_PROBER', 'true', config_obj, replace=True)
        mock_apply_grub2_config.assert_called_once_with(
            config_obj, execute_update_grub=False)

    @ddt.data(
        ({}, None),
        ({'GRUB_DISABLE_OS_PROBER': 'false'}, 'false'),
        ({'GRUB_DISABLE_OS_PROBER': 'true'}, 'true'),
    )
    @ddt.unpack
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_set_grub_os_prober_setting')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, 'get_update_grub2_command')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_grub_config')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_get_grub_default_conf',
        return_value='/etc/default/grub',
    )
    def test__execute_update_grub_toggles_os_prober(
            self,
            grub_contents,
            expected_restore_value,
            _mock_get_grub_default_conf,
            mock_read_grub_config,
            mock_get_update_grub2_command,
            mock_exec_cmd_chroot,
            mock_set_grub_os_prober_setting):
        mock_read_grub_config.return_value = grub_contents

        self.os_morphing_tools._execute_update_grub()

        mock_read_grub_config.assert_called_once_with('/etc/default/grub')
        mock_set_grub_os_prober_setting.assert_has_calls([
            mock.call('/etc/default/grub', 'true'),
            mock.call('/etc/default/grub', expected_restore_value),
        ])
        mock_get_update_grub2_command.assert_called_once_with()
        mock_exec_cmd_chroot.assert_called_once_with(
            mock_get_update_grub2_command.return_value)

    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_set_grub_os_prober_setting')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, 'get_update_grub2_command')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_grub_config')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_get_grub_default_conf',
        return_value='/etc/default/grub',
    )
    def test__execute_update_grub_restores_os_prober_on_failure(
            self,
            _mock_get_grub_default_conf,
            mock_read_grub_config,
            mock_get_update_grub2_command,
            mock_exec_cmd_chroot,
            mock_set_grub_os_prober_setting):
        mock_read_grub_config.return_value = {
            'GRUB_DISABLE_OS_PROBER': 'false',
        }
        mock_exec_cmd_chroot.side_effect = CoriolisTestException()

        self.assertRaises(
            CoriolisTestException,
            self.os_morphing_tools._execute_update_grub)

        mock_set_grub_os_prober_setting.assert_has_calls([
            mock.call('/etc/default/grub', 'true'),
            mock.call('/etc/default/grub', 'false'),
        ])
        mock_get_update_grub2_command.assert_called_once_with()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_execute_update_grub')
    def test__schedule_grub2_update(self, mock_execute_update_grub):
        self.assertFalse(self.os_morphing_tools._grub2_update_scheduled)

        self.os_morphing_tools._schedule_grub2_update()

        self.assertTrue(self.os_morphing_tools._grub2_update_scheduled)
        # Scheduling must not run the (slow) update command itself.
        mock_execute_update_grub.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_execute_update_grub')
    def test__run_scheduled_grub2_update_when_scheduled(
            self, mock_execute_update_grub):
        self.os_morphing_tools._grub2_update_scheduled = True

        self.os_morphing_tools._run_scheduled_grub2_update()

        mock_execute_update_grub.assert_called_once_with()
        # The pending flag must be cleared so subsequent calls are no-ops.
        self.assertFalse(self.os_morphing_tools._grub2_update_scheduled)

        # Running the update again must be a no-op.
        self.os_morphing_tools._run_scheduled_grub2_update()
        # Check that the update command was called only once.
        self.assertEqual(1, mock_execute_update_grub.call_count)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_execute_update_grub')
    def test__run_scheduled_grub2_update_when_not_scheduled(
            self, mock_execute_update_grub):
        self.os_morphing_tools._grub2_update_scheduled = False

        self.os_morphing_tools._run_scheduled_grub2_update()

        mock_execute_update_grub.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_schedule_grub2_update')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__apply_grub2_config(self, mock_exec_cmd_chroot,
                                 mock_schedule_grub2_update):
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
        mock_schedule_grub2_update.assert_called_once_with()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_schedule_grub2_update')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__apply_grub2_config_no_update_grub(self, mock_exec_cmd_chroot,
                                                mock_schedule_grub2_update):
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
        mock_schedule_grub2_update.assert_not_called()

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
        net_ifaces_info = {
            "eth0": "AA:BB:CC:DD:EE:FF",
            "eth1": "FF:EE:DD:CC:BB:AA"
        }
        content = (
            'SUBSYSTEM=="net", ACTION=="add", DRIVERS=="?*", '
            'ATTR{address}=="aa:bb:cc:dd:ee:ff", NAME="eth0"\n'
            'SUBSYSTEM=="net", ACTION=="add", DRIVERS=="?*", '
            'ATTR{address}=="ff:ee:dd:cc:bb:aa", NAME="eth1"\n'
        )

        self.os_morphing_tools._add_net_udev_rules(net_ifaces_info)

        mock_write_file_sudo.assert_called_once_with(
            "etc/udev/rules.d/99-coriolis-net.rules", content
        )

    @ddt.data(
        # (nics_info, interface_info, expected_net_ifaces)
        (
            [
                {"mac_address": None,
                 "ip_addresses": []}
            ],
            {
                "eth0": {"mac_address": None,
                         "ip_addresses": []},
            },
            {}
        ),
        (
            [
                {"mac_address": "00:11:22:33:44:55",
                 "ip_addresses": ["192.168.1.10"]},
            ],
            {
                "eth0": {"mac_address": None,
                         "ip_addresses": None},
            },
            {}
        ),
        (
            [
                {"mac_address": "00:11:22:33:44:55",
                 "ip_addresses": ["192.168.1.10"]},
            ],
            {
                "eth0": {"mac_address": "00:11:22:33:44:55",
                         "ip_addresses": []},
            },
            {
                "eth0": "00:11:22:33:44:55"
            }
        ),
        (
            [
                {"mac_address": "00:11:22:33:44:55",
                 "ip_addresses": ["192.168.1.10"]},
            ],
            {
                "eth0": {"mac_address": None,
                         "ip_addresses": ["192.168.1.10"]},
            },
            {
                "eth0": "00:11:22:33:44:55"
            }
        ),
        (
            [
                {"mac_address": "00:11:22:33:44:55",
                 "ip_addresses": ["192.168.1.10"]},
            ],
            {
                "eth0": {"mac_address": None,
                         "ip_addresses": ["192.168.1.20", "192.168.1.10"]},
            },
            {
                "eth0": "00:11:22:33:44:55"
            }
        ),
        (
            [
                {"mac_address": "00:11:22:33:44:55",
                 "ip_addresses": ["192.168.1.20", "192.168.1.10"]},
            ],
            {
                "eth0": {"mac_address": None,
                         "ip_addresses": ["192.168.1.30", "192.168.1.10"]},
            },
            {
                "eth0": "00:11:22:33:44:55"
            }
        ),
        (
            [
                {"mac_address": "AA:BB:CC:DD:EE:FF",
                 "ip_addresses": ["192.168.4.10"]},
            ],
            {
                "eth0": {"mac_address": "00:11:22:33:44:55",
                         "ip_addresses": ["192.168.1.10"]},
                "eth1": {"mac_address": "AA:BB:CC:DD:EE:FF",
                         "ip_addresses": ["192.168.2.20"]},
                "eth2": {"mac_address": "11:22:33:44:55:66",
                         "ip_addresses": ["192.168.3.30"]},
            },
            {
                "eth1": "AA:BB:CC:DD:EE:FF"
            }
        ),
        (
            [
                {"mac_address": None,
                 "ip_addresses": ["192.168.3.10"]},
                {"mac_address": "AA:BB:CC:DD:EE:FF",
                 "ip_addresses": []},
                {"mac_address": "00:11:22:33:44:55",
                 "ip_addresses": ["192.168.1.11"]},
                {"mac_address": "FF:FF:FF:FF:FF:FF",
                 "ip_addresses": ["192.168.2.10", "192.168.2.20"]},
            ],
            {
                "eth3": {"mac_address": None,
                         "ip_addresses": ["192.168.3.10"]},
                "eth0": {"mac_address": "00:11:22:33:44:55",
                         "ip_addresses": ["192.168.1.10"]},
                "eth1": {"mac_address": "AA:BB:CC:DD:EE:FF",
                         "ip_addresses": ["192.168.1.20"]},
                "eth2": {"mac_address": "11:22:33:44:55:66",
                         "ip_addresses": ["192.168.2.20"]},
            },
            {
                "eth0": "00:11:22:33:44:55",
                "eth1": "AA:BB:CC:DD:EE:FF",
                "eth2": "FF:FF:FF:FF:FF:FF",
            }
        ),
        # MAC changed but the network config contains an explicit MAC
        # assignment. An exception is expected since Coriolis cannot
        # modify Linux network config files at the moment.
        (
            [
                {"mac_address": "00:11:22:33:44:55",
                 "new_mac_address": "ff:aa:11:22:33:44",
                 "ip_addresses": ["192.168.1.10"]},
            ],
            {
                "eth0": {"mac_address": "00:11:22:33:44:55",
                         "ip_addresses": []},
            },
            exception.CoriolisException,
        ),
        # The MAC address changed but the network config files did not
        # contain explicit MAC assignments, we can rely on the udev rules
        # that preserve the interface names. The new MAC will be returned.
        (
            [
                {"mac_address": "00:11:22:33:44:55",
                 "new_mac_address": "ff:aa:11:22:33:44",
                 "ip_addresses": ["192.168.1.10"]},
            ],
            {
                "eth0": {"mac_address": None,
                         "ip_addresses": ["192.168.1.10"]},
            },
            {
                "eth0": "ff:aa:11:22:33:44",
            }
        ),
    )
    @ddt.unpack
    def test__setup_network_preservation(
        self, nics_info, interface_info, expected_net_ifaces):
        class FakeNetPreserver:
            def __init__(self, tool):
                self.tool = tool
                self.interface_info = {}

            def parse_network(self):
                self.interface_info = interface_info

        with mock.patch(
            "coriolis.osmorphing.netpreserver.factory.get_net_preserver",
                return_value=FakeNetPreserver) as mock_get_np:

            self.os_morphing_tools._add_net_udev_rules = mock.MagicMock()

            if isinstance(expected_net_ifaces, type) and issubclass(
                    expected_net_ifaces, Exception):
                self.assertRaises(
                    expected_net_ifaces,
                    self.os_morphing_tools._setup_network_preservation,
                    nics_info,
                )
            else:
                with self.assertLogs(
                    'coriolis.osmorphing.base', level=logging.INFO):
                    self.os_morphing_tools._setup_network_preservation(
                        nics_info)

                result_net_ifaces = dict(
                    self.os_morphing_tools._add_net_udev_rules.call_args[0][0])

                mock_get_np.assert_called_once_with(self.os_morphing_tools)
                self.os_morphing_tools._add_net_udev_rules.assert_called_once()
                self.assertEqual(expected_net_ifaces, result_net_ifaces)
