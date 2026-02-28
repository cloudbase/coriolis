# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing.osmount import suse
from coriolis.tests import test_base


class BaseSUSEOSMountToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the SUSEOSMountTools class."""

    @mock.patch.object(suse.base.BaseSSHOSMountTools, '_connect')
    def setUp(self, mock_connect):
        super(BaseSUSEOSMountToolsTestCase, self).setUp()
        self.ssh = mock.MagicMock()

        self.tools = suse.SUSEOSMountTools(
            self.ssh, mock.sentinel.event_manager,
            mock.sentinel.ignore_devices,
            mock.sentinel.operation_timeout)

        mock_connect.assert_called_once_with()

        self.tools._ssh = self.ssh

    @mock.patch.object(suse.utils, 'get_linux_os_info')
    def test_check_os(self, mock_get_linux_os_info):
        mock_get_linux_os_info.return_value = ['sles']

        result = self.tools.check_os()
        self.assertTrue(result)

    @mock.patch.object(suse.utils, 'get_linux_os_info')
    def test_check_os_opensuse_leap(self, mock_get_linux_os_info):
        mock_get_linux_os_info.return_value = ['opensuse-leap']

        result = self.tools.check_os()
        self.assertTrue(result)

    @mock.patch.object(suse.utils, 'get_linux_os_info')
    def test_check_os_opensuse_tumbleweed(self, mock_get_linux_os_info):
        mock_get_linux_os_info.return_value = ['opensuse-tumbleweed']

        result = self.tools.check_os()
        self.assertTrue(result)

    @mock.patch.object(suse.utils, 'get_linux_os_info')
    def test_check_os_not_suse(self, mock_get_linux_os_info):
        mock_get_linux_os_info.return_value = ['ubuntu']

        result = self.tools.check_os()
        self.assertIsNone(result)

    @mock.patch.object(suse.utils, 'retry_on_error')
    @mock.patch.object(suse.base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(suse.base.BaseSSHOSMountTools, 'setup')
    def test_setup_lvm2_not_installed(
            self, mock_setup, mock_exec_cmd, mock_retry_on_error):
        mock_retry_on_error.return_value = lambda f: f
        mock_exec_cmd.side_effect = [
            Exception("not installed"),
            None, None, None]
        result = self.tools.setup()
        self.assertIsNone(result)

        mock_setup.assert_called_once_with()
        mock_retry_on_error.assert_called_once_with(
            max_attempts=10, sleep_seconds=30)
        mock_exec_cmd.assert_has_calls([
            mock.call("rpm -q lvm2"),
            mock.call(
                "sudo -E zypper --non-interactive install lvm2"),
            mock.call("sudo modprobe dm-mod"),
            mock.call("sudo rm -f /etc/lvm/devices/system.devices")
        ])

    @mock.patch.object(suse.utils, 'retry_on_error')
    @mock.patch.object(suse.base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(suse.base.BaseSSHOSMountTools, 'setup')
    def test_setup_lvm2_already_installed(
            self, mock_setup, mock_exec_cmd, mock_retry_on_error):
        result = self.tools.setup()
        self.assertIsNone(result)

        mock_setup.assert_called_once_with()
        mock_retry_on_error.assert_not_called()
        mock_exec_cmd.assert_has_calls([
            mock.call("rpm -q lvm2"),
            mock.call("sudo modprobe dm-mod"),
            mock.call("sudo rm -f /etc/lvm/devices/system.devices")
        ])

    @mock.patch.object(suse.base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(suse.utils, 'restart_service')
    @mock.patch.object(suse.utils, 'test_ssh_path', return_value=True)
    def test__allow_ssh_env_vars(
            self, mock_test_ssh_path, mock_restart_service, mock_exec_cmd):
        result = self.tools._allow_ssh_env_vars()
        self.assertTrue(result)

        mock_test_ssh_path.assert_called_once_with(
            self.ssh, suse.SSHD_CONFIG_PATH)
        mock_exec_cmd.assert_called_once_with(
            'sudo sed -i -e "\\$aAcceptEnv *" %s' % suse.SSHD_CONFIG_PATH)
        mock_restart_service.assert_called_once_with(self.ssh, "sshd")

    @mock.patch.object(suse.base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(suse.utils, 'restart_service')
    @mock.patch.object(suse.utils, 'test_ssh_path', return_value=False)
    def test__allow_ssh_env_vars_usr_etc(
            self, mock_test_ssh_path, mock_restart_service, mock_exec_cmd):
        result = self.tools._allow_ssh_env_vars()
        self.assertTrue(result)

        mock_test_ssh_path.assert_called_once_with(
            self.ssh, suse.SSHD_CONFIG_PATH)
        mock_exec_cmd.assert_has_calls([
            mock.call(
                "sudo cp %s %s" % (
                    suse.USR_SSHD_CONFIG_PATH, suse.SSHD_CONFIG_PATH)),
            mock.call(
                'sudo sed -i -e "\\$aAcceptEnv *" %s' %
                suse.SSHD_CONFIG_PATH)])
        mock_restart_service.assert_called_once_with(self.ssh, "sshd")
