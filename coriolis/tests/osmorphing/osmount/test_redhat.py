# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing.osmount import redhat
from coriolis.tests import test_base


class BaseRedHatOSMountToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the RedHatOSMountTools class."""

    @mock.patch.object(redhat.base.BaseSSHOSMountTools, '_connect')
    def setUp(self, mock_connect):
        super(BaseRedHatOSMountToolsTestCase, self).setUp()
        self.ssh = mock.MagicMock()

        self.tools = redhat.RedHatOSMountTools(
            self.ssh, mock.sentinel.event_manager,
            mock.sentinel.ignore_devices,
            mock.sentinel.operation_timeout)

        mock_connect.assert_called_once_with()

        self.tools._ssh = self.ssh

    @mock.patch.object(redhat.utils, 'get_linux_os_info')
    def test_check_os(self, mock_get_linux_os_info):
        mock_get_linux_os_info.return_value = ['RedHatEnterpriseServer']

        result = self.tools.check_os()
        self.assertTrue(result)

    @mock.patch.object(redhat.base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(redhat.base.BaseSSHOSMountTools, 'setup')
    def test_setup(self, mock_setup, mock_exec_cmd):
        result = self.tools.setup()
        self.assertIsNone(result)

        mock_setup.assert_called_once_with()
        mock_exec_cmd.assert_has_calls([
            mock.call("sudo -E yum install -y lvm2 psmisc"),
            mock.call("sudo modprobe dm-mod")
        ])

    @mock.patch.object(redhat.base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(redhat.utils, 'restart_service')
    def test__allow_ssh_env_vars(self, mock_restart_service, mock_exec_cmd):
        result = self.tools._allow_ssh_env_vars()
        self.assertTrue(result)

        mock_exec_cmd.assert_called_once_with(
            'sudo sed -i -e "\$aAcceptEnv *" /etc/ssh/sshd_config')
        mock_restart_service.assert_called_once_with(self.ssh, "sshd")
