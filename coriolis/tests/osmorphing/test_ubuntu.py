# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

from coriolis.osmorphing import base
from coriolis.osmorphing import ubuntu
from coriolis.tests import test_base


class BaseUbuntuMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseUbuntuMorphingTools class."""

    def setUp(self):
        super(BaseUbuntuMorphingToolsTestCase, self).setUp()
        self.detected_os_info = {
            'os_type': 'linux',
            'distribution_name': 'Ubuntu',
            'release_version': '20.04',
            'friendly_release_name': mock.sentinel.friendly_release_name,
        }
        self.event_manager = mock.MagicMock()
        self.os_root_dir = '/root'
        self.morphing_tools = ubuntu.BaseUbuntuMorphingTools(
            mock.sentinel.conn, self.os_root_dir,
            mock.sentinel.os_root_dev, mock.sentinel.hypervisor,
            self.event_manager, self.detected_os_info,
            mock.sentinel.osmorphing_parameters,
            mock.sentinel.operation_timeout)

    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_version_supported_util'
    )
    def test_check_os_supported_not_supported(
            self, mock_version_supported_util):
        self.detected_os_info['distribution_name'] = 'unsupported'

        result = self.morphing_tools.check_os_supported(self.detected_os_info)

        mock_version_supported_util.assert_not_called()

        self.assertFalse(result)

    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_version_supported_util'
    )
    def test_check_os_supported_lts_release(self, mock_version_supported_util):
        result = self.morphing_tools.check_os_supported(self.detected_os_info)

        mock_version_supported_util.assert_called_once_with(
            self.detected_os_info['release_version'], minimum=12.04,
            maximum=12.04)

        self.assertTrue(result)

    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_version_supported_util'
    )
    def test_check_os_supported_non_lts_release(
            self, mock_version_supported_util):
        self.detected_os_info['release_version'] = '21.10'
        mock_version_supported_util.return_value = False

        result = self.morphing_tools.check_os_supported(self.detected_os_info)

        mock_version_supported_util.assert_called_with(
            self.detected_os_info['release_version'], minimum=22.04)

        self.assertEqual(result, mock_version_supported_util.return_value)

    @mock.patch('yaml.dump')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_list_dir')
    def test__set_netplan_ethernet_configs(
            self, mock_list_dir, mock_read_file, mock_write_file_sudo,
            mock_exec_cmd, mock_yaml_dump):
        mock_list_dir.return_value = ['file1', 'file2.yaml']
        mock_read_file.return_value = (
            b'network: {version: 2, ethernets: {eth0: {dhcp4: true}}}')
        nics_info = [
            {'name': 'eth0', 'mac_address': '00:00:00:00:00:00'},
            {'name': 'eth1', 'mac_address': '00:00:00:00:00:01'}]

        self.morphing_tools._set_netplan_ethernet_configs(
            nics_info, dhcp=True, iface_name_prefix='test')

        mock_exec_cmd.assert_called_once_with(
            "sudo cp '%s/etc/netplan/file2.yaml' "
            "'%s/etc/netplan/file2.yaml.bak'" % (
                self.os_root_dir, self.os_root_dir))
        mock_read_file.assert_called_once_with('etc/netplan/file2.yaml')
        mock_write_file_sudo.assert_called_once_with(
            'etc/netplan/file2.yaml', mock_yaml_dump.return_value)

    @mock.patch('yaml.load')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_list_dir')
    def test__set_netplan_ethernet_configs_no_network_data(
            self, mock_list_dir, mock_read_file, mock_write_file_sudo,
            mock_exec_cmd, mock_yaml_load):
        mock_yaml_load.return_value = {}
        mock_list_dir.return_value = ['file1.yaml']
        mock_read_file.return_value = b''

        with self.assertLogs('coriolis.osmorphing.ubuntu',
                             level=logging.DEBUG):
            self.morphing_tools._set_netplan_ethernet_configs(
                mock.sentinel.nics_info)

        mock_read_file.assert_called_once_with('etc/netplan/file1.yaml')
        mock_exec_cmd.assert_called_once_with(
            "sudo cp '%s/etc/netplan/file1.yaml' "
            "'%s/etc/netplan/file1.yaml.bak'" % (
                self.os_root_dir, self.os_root_dir))
        mock_write_file_sudo.assert_not_called()

    @mock.patch('yaml.dump')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_list_dir')
    def test__set_netplan_ethernet_configs_incompatible_version(
            self, mock_list_dir, mock_read_file, mock_write_file_sudo,
            mock_exec_cmd, mock_yaml_dump):
        mock_list_dir.return_value = ['file1.yaml']
        mock_read_file.return_value = b'network: {version: 4}'

        with self.assertLogs('coriolis.osmorphing.ubuntu',
                             level=logging.DEBUG):
            self.morphing_tools._set_netplan_ethernet_configs(
                mock.sentinel.nics_info)

        mock_read_file.assert_called_once_with('etc/netplan/file1.yaml')
        mock_exec_cmd.assert_called_once_with(
            "sudo cp '%s/etc/netplan/file1.yaml' "
            "'%s/etc/netplan/file1.yaml.bak'" % (
                self.os_root_dir, self.os_root_dir))
        mock_write_file_sudo.assert_not_called()

    @mock.patch('yaml.load')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_list_dir')
    def test__set_netplan_ethernet_configs_no_ethernet_configs(
            self, mock_list_dir, mock_read_file, mock_write_file_sudo,
            mock_exec_cmd, mock_yaml_load):
        mock_yaml_load.return_value = {
            "network": {"version": 2, "ethernets": {}}}
        mock_list_dir.return_value = ['file1.yaml']
        mock_read_file.return_value = b'file1 content'

        nics_info = [{'name': 'eth0', 'mac_address': '00:00:00:00:00:00'}]

        with self.assertLogs('coriolis.osmorphing.ubuntu',
                             level=logging.DEBUG):
            self.morphing_tools._set_netplan_ethernet_configs(nics_info)

        expected_yaml_content = (
            'network:\n  ethernets: {}\n  version: 2\n'
        )
        mock_read_file.assert_called_once_with('etc/netplan/file1.yaml')
        mock_exec_cmd.assert_called_once_with(
            "sudo cp '%s/etc/netplan/file1.yaml' "
            "'%s/etc/netplan/file1.yaml.bak'" % (
                self.os_root_dir, self.os_root_dir))
        mock_write_file_sudo.assert_called_once_with(
            'etc/netplan/file1.yaml', expected_yaml_content)
