# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing.osmount import ubuntu
from coriolis.tests import test_base


class UbuntuOSMountToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the UbuntuOSMountTools class."""

    @mock.patch.object(ubuntu.base.BaseSSHOSMountTools, '_connect')
    def setUp(self, mock_connect):
        super(UbuntuOSMountToolsTestCase, self).setUp()
        self.ssh = mock.MagicMock()

        self.tools = ubuntu.UbuntuOSMountTools(
            self.ssh, mock.sentinel.event_manager,
            mock.sentinel.ignore_devices, mock.sentinel.operation_timeout)

        mock_connect.assert_called_once_with()

        self.tools._ssh = self.ssh

    @mock.patch.object(ubuntu.utils, 'get_linux_os_info')
    def test_check_os(self, mock_get_linux_os_info):
        mock_get_linux_os_info.return_value = ['Ubuntu']

        result = self.tools.check_os()
        self.assertTrue(result)

    @mock.patch.object(ubuntu.base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(ubuntu.base.BaseSSHOSMountTools, 'setup')
    def test_setup(self, mock_setup, mock_exec_cmd):
        result = self.tools.setup()
        self.assertIsNone(result)

        mock_setup.assert_called_once_with()
        mock_exec_cmd.assert_has_calls([
            mock.call("sudo -E apt-get update -y"),
            mock.call("sudo -E apt-get -o DPkg::Lock::Timeout=600 "
                      "install lvm2 psmisc -y"),
            mock.call("sudo modprobe dm-mod")
        ])

    @mock.patch.object(ubuntu.base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(ubuntu.utils, 'restart_service')
    def test__allow_ssh_env_vars(self, mock_restart_service, mock_exec_cmd):
        result = self.tools._allow_ssh_env_vars()
        self.assertTrue(result)

        mock_exec_cmd.assert_called_once_with(
            'sudo sed -i -e "\$aAcceptEnv *" /etc/ssh/sshd_config')
        mock_restart_service.assert_called_once_with(self.ssh, "sshd")
