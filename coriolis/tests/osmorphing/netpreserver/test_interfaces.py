# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing import debian
from coriolis.osmorphing.netpreserver import interfaces
from coriolis.tests import test_base


class InterfacesNetPreserverTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the InterfacesNetPreserver class."""

    def setUp(self):
        super(InterfacesNetPreserverTestCase, self).setUp()
        self.event_manager = mock.MagicMock()
        self.detected_os_info = {
            'os_type': 'linux',
            "release_version": '10',
            'distribution_name': debian.DEBIAN_DISTRO_IDENTIFIER,
            'friendly_release_name': mock.sentinel.friendly_release_name,
        }
        self.netpreserver = interfaces.InterfacesNetPreserver(
            debian.BaseDebianMorphingTools(
                mock.sentinel.conn,
                mock.sentinel.os_root_dir,
                mock.sentinel.os_root_dir,
                mock.sentinel.hypervisor,
                self.event_manager,
                self.detected_os_info,
                mock.sentinel.osmorphing_parameters,
                mock.sentinel.operation_timeout
            )
        )

    @mock.patch.object(debian.BaseDebianMorphingTools, '_list_dir')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_test_path')
    def test_check_net_preserver_true(self, mock_test_path, mock_list_dir):
        mock_test_path.return_value = True
        mock_list_dir.return_value = ["interfaces"]

        result = self.netpreserver.check_net_preserver()

        mock_test_path.assert_called_once_with(self.netpreserver.ifaces_file)
        mock_list_dir.assert_called_once_with('etc/network')
        self.assertTrue(result)

    @mock.patch.object(debian.BaseDebianMorphingTools, '_list_dir')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_test_path')
    def test_check_net_preserver_false_no_file(self, mock_test_path,
                                               mock_list_dir):
        mock_test_path.return_value = False
        mock_list_dir.return_value = []

        result = self.netpreserver.check_net_preserver()

        self.assertFalse(result)

    @mock.patch.object(debian.BaseDebianMorphingTools, '_read_file_sudo')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_test_path')
    def test_parse_network_basic(self, mock_test_path, mock_exec_cmd_chroot,
                                 mock_read_file_sudo):
        interfaces_content = (
            "iface eth0 inet static\n"
            "hwaddress ether 00:11:22:33:44:55\n"
            "address 192.168.1.10/24\n"
            "\n"
            "iface eth1 inet dhcp\n"
            "address 192.168.1.20/24\n"
        )
        mock_read_file_sudo.return_value = interfaces_content.encode()
        mock_test_path.return_value = True
        mock_exec_cmd_chroot.return_value = ""

        self.netpreserver.interface_info = {}
        self.netpreserver.parse_network()

        expected_info = {
            "eth0": {
                "mac_address": "00:11:22:33:44:55",
                "ip_addresses": ["192.168.1.10"]
            },
            "eth1": {
                "mac_address": "",
                "ip_addresses": ["192.168.1.20"]
            }
        }
        self.assertEqual(self.netpreserver.interface_info, expected_info)

    @mock.patch.object(debian.BaseDebianMorphingTools, '_read_file_sudo')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_test_path')
    def test_parse_network_with_source(self, mock_test_path,
                                       mock_exec_cmd_chroot,
                                       mock_read_file_sudo):
        main_content = (
            "iface eth0 inet static\n"
            "hwaddress ether 00:11:22:33:44:55\n"
            "address 192.168.1.10/24\n"
            "source /etc/network/interfaces.d/\n"
        )
        extra_file = "extra_iface"
        extra_content = (
            "iface eth1 inet dhcp\n"
            "address 192.168.1.20/24\n"
        )

        mock_test_path.return_value = True
        mock_exec_cmd_chroot.return_value = extra_file
        mock_read_file_sudo.side_effect = (
            main_content.encode(), extra_content.encode()
        )

        self.netpreserver.parse_network()

        expected_info = {
            "eth0": {
                "mac_address": "00:11:22:33:44:55",
                "ip_addresses": ["192.168.1.10"]
            },
            "eth1": {
                "mac_address": "",
                "ip_addresses": ["192.168.1.20"]
            }
        }
        self.assertEqual(self.netpreserver.interface_info, expected_info)

    @mock.patch.object(debian.BaseDebianMorphingTools, '_read_file_sudo')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_test_path')
    def test_parse_network_with_trailing_hwaddress(self, mock_test_path,
                                                   mock_exec_cmd_chroot,
                                                   mock_read_file_sudo):

        interfaces_content = (
            "hwaddress ether 00:11:22:33:44:55\n"
        )
        mock_read_file_sudo.return_value = interfaces_content.encode()
        mock_test_path.return_value = True
        mock_exec_cmd_chroot.return_value = ""

        with self.assertLogs(level='WARNING'):
            self.netpreserver.parse_network()

        self.assertEqual(self.netpreserver.interface_info, {})
