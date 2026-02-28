# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import exception
from coriolis.osmorphing.osmount import base
from coriolis.osmorphing.osmount import factory
from coriolis.tests import test_base


class GetOsMountToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the get_os_mount_tools function."""

    def test_get_os_mount_tools_unsupported_os_type(self):
        self.assertRaises(
            exception.CoriolisException, factory.get_os_mount_tools,
            "unsupported_os", mock.sentinel.connection_info,
            mock.sentinel.event_manager, mock.sentinel.ignore_devices,
            mock.sentinel.operation_timeout)

    @mock.patch.object(base.BaseSSHOSMountTools, '_connect')
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(factory.ubuntu.UbuntuOSMountTools, 'check_os',
                       return_value=False)
    @mock.patch.object(factory.redhat.RedHatOSMountTools, 'check_os',
                       return_value=False)
    @mock.patch.object(factory.suse.SUSEOSMountTools, 'check_os',
                       return_value=False)
    @mock.patch.object(factory.windows.WindowsMountTools, 'check_os',
                       return_value=False)
    def test_get_os_mount_tools_no_os_found(
            self, mock_windows_check, mock_suse_check, mock_redhat_check,
            mock_ubuntu_check, mock_exec_cmd, mock_connect):
        mock_exec_cmd.return_value = ("Ubuntu", "")
        self.assertRaises(
            exception.CoriolisException, factory.get_os_mount_tools,
            factory.constants.OS_TYPE_LINUX, mock_connect,
            mock.sentinel.event_manager, mock.sentinel.ignore_devices,
            mock.sentinel.operation_timeout)

        mock_redhat_check.assert_called_once_with()
        mock_ubuntu_check.assert_called_once_with()
        mock_suse_check.assert_called_once_with()
        mock_windows_check.assert_not_called()

    @mock.patch.object(base.BaseSSHOSMountTools, '_connect')
    @mock.patch.object(base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(factory.ubuntu.UbuntuOSMountTools, 'check_os',
                       return_value=True)
    @mock.patch.object(factory.redhat.RedHatOSMountTools, 'check_os',
                       return_value=False)
    @mock.patch.object(factory.suse.SUSEOSMountTools, 'check_os',
                       return_value=False)
    @mock.patch.object(factory.windows.WindowsMountTools, 'check_os',
                       return_value=False)
    def test_get_os_mount_tools_os_found(
            self, mock_windows_check, mock_suse_check, mock_redhat_check,
            mock_ubuntu_check, mock_exec_cmd, mock_connect):
        mock_exec_cmd.return_value = ("Ubuntu", "")
        tools = factory.get_os_mount_tools(
            factory.constants.OS_TYPE_LINUX, mock_connect,
            mock.sentinel.event_manager, mock.sentinel.ignore_devices,
            mock.sentinel.operation_timeout)
        self.assertIsInstance(tools, factory.ubuntu.UbuntuOSMountTools)

        mock_ubuntu_check.assert_called_once_with()
        mock_redhat_check.assert_not_called()
        mock_suse_check.assert_not_called()
        mock_windows_check.assert_not_called()
