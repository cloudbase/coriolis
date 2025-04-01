# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing import debian
from coriolis.osmorphing.netpreserver import netplan
from coriolis.tests import test_base


class NetplanNetPreserverTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the NetplanNetPreserver class."""

    def setUp(self):
        super(NetplanNetPreserverTestCase, self).setUp()
        self.event_manager = mock.MagicMock()
        self.detected_os_info = {
            'os_type': 'linux',
            "release_version": '10',
            'distribution_name': debian.DEBIAN_DISTRO_IDENTIFIER,
            'friendly_release_name': mock.sentinel.friendly_release_name,
        }
        self.netpreserver = netplan.NetplanNetPreserver(
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
        mock_list_dir.return_value = ["01-netplan.yaml", "notconfig.txt",
                                      "02-other.yml"]

        result = self.netpreserver.check_net_preserver()

        mock_test_path.assert_called_once_with(self.netpreserver.netplan_base)
        mock_list_dir.assert_called_once_with(self.netpreserver.netplan_base)
        self.assertTrue(result)

    @mock.patch.object(debian.BaseDebianMorphingTools, '_list_dir')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_test_path')
    def test_check_net_preserver_false_no_file(self, mock_test_path,
                                               mock_list_dir):
        mock_test_path.return_value = True
        mock_list_dir.return_value = ["config.txt", "README.md"]

        result = self.netpreserver.check_net_preserver()

        self.assertFalse(result)

    @mock.patch.object(debian.BaseDebianMorphingTools, '_list_dir')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_test_path')
    def test_check_net_preserver_false_no_directory(self, mock_test_path,
                                                    mock_list_dir):
        mock_test_path.return_value = False
        mock_list_dir.return_value = []

        result = self.netpreserver.check_net_preserver()

        self.assertFalse(result)

    @mock.patch.object(debian.BaseDebianMorphingTools, '_read_file_sudo')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_list_dir')
    def test_parse_network(self, mock_list_dir, mock_read_file_sudo):
        mock_list_dir.return_value = ["01-netplan.yaml"]
        yaml_content = """
            network:
            ethernets:
                eth0:
                    match:
                        macaddress: "00:11:22:33:44:55"
                    addresses:
                        - "192.168.1.10/24"
                eth1:
                    addresses:
                        - "192.168.1.20/24"
                        - "192.168.1.21/24"
                eth2:
                    match:
                        macaddress: "AA:BB:CC:DD:EE:FF"
                eth4:
                    addresses:
                        - "192.168.1.31/24":
                eth3:
                    addresses:
                        - "192.168.1.30/24":
                            lifetime: 0
                            label: "eth3"
                eth5:
                    addresses:
                        - "192.168.1.32/24"
                        - {}
                eth6:
                    addresses:
                        - {}

        """
        mock_read_file_sudo.return_value = yaml_content.lstrip().encode()

        self.netpreserver.parse_network()

        expected_info = {
            "eth0": {
                "mac_address": "00:11:22:33:44:55",
                "ip_addresses": ["192.168.1.10"]
            },
            "eth1": {
                "mac_address": "",
                "ip_addresses": ["192.168.1.20", "192.168.1.21"]
            },
            "eth2": {
                "mac_address": "AA:BB:CC:DD:EE:FF",
                "ip_addresses": []
            },
            "eth3": {
                "mac_address": "",
                "ip_addresses": ["192.168.1.30"]
            },
            "eth4": {
                "mac_address": "",
                "ip_addresses": ["192.168.1.31"]
            },
            "eth5": {
                "mac_address": "",
                "ip_addresses": ["192.168.1.32"]
            },
            "eth6": {
                "mac_address": "",
                "ip_addresses": []
            }
        }

        self.assertEqual(self.netpreserver.interface_info, expected_info)

    @mock.patch.object(debian.BaseDebianMorphingTools, '_read_file_sudo')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_list_dir')
    def test_parse_network_invalid_file(self, mock_list_dir,
                                        mock_read_file_sudo):
        mock_list_dir.return_value = ["01-netplan.yaml"]
        yaml_content = """
            network:
            ethernets:
                eth0:
                        match:
                    macaddress: "00:11:22:33:44:55"
                    addresses:
                        - "192.168.1.10/24"
        """

        mock_read_file_sudo.return_value = yaml_content.lstrip().encode()

        with self.assertLogs(level='WARNING'):
            self.netpreserver.parse_network()

        expected_info = {}

        self.assertEqual(self.netpreserver.interface_info, expected_info)
