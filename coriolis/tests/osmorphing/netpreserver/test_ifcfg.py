# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing import base
from coriolis.osmorphing.netpreserver import ifcfg
from coriolis.osmorphing import redhat
from coriolis.tests import test_base


class CoriolisTestException(Exception):
    pass


class IfcfgNetPreserverTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the IfcfgNetPreserver class."""

    def setUp(self):
        super(IfcfgNetPreserverTestCase, self).setUp()
        self.event_manager = mock.MagicMock()
        self.detected_os_info = {
            'os_type': 'linux',
            'distribution_name': redhat.RED_HAT_DISTRO_IDENTIFIER,
            'release_version': '6',
            'friendly_release_name': mock.sentinel.friendly_release_name,
        }
        self.netpreserver = ifcfg.IfcfgNetPreserver(
            redhat.BaseRedHatMorphingTools(
                mock.sentinel.conn, mock.sentinel.os_root_dir,
                mock.sentinel.os_root_dir, mock.sentinel.hypervisor,
                self.event_manager, self.detected_os_info,
                mock.sentinel.osmorphing_parameters,
                mock.sentinel.operation_timeout))

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_config_file')
    @mock.patch.object(ifcfg.IfcfgNetPreserver, '_get_net_config_files')
    def test_get_ifcfgs_by_type(self, mock_get_net_config_files,
                                mock_read_config_file):
        mock_get_net_config_files.return_value = [mock.sentinel.ifcfg_file]
        mock_read_config_file.side_effect = [{"TYPE": "Ethernet"}]

        result = self.netpreserver._get_ifcfgs_by_type(
            "Ethernet", self.netpreserver.network_scripts_path)

        mock_read_config_file.assert_called_once_with(mock.sentinel.ifcfg_file)
        mock_get_net_config_files.assert_called_once_with(
            self.netpreserver.network_scripts_path)

        self.assertEqual(
            result, [(mock.sentinel.ifcfg_file, {"TYPE": "Ethernet"})])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_list_dir')
    def test_get_net_config_files(self, mock_list_dir):
        mock_list_dir.return_value = ['ifcfg-eth0', 'ifcfg-lo', 'other-file']

        result = self.netpreserver._get_net_config_files(
            self.netpreserver.network_scripts_path)

        expected_result = [
            'etc/sysconfig/network-scripts/ifcfg-eth0',
            'etc/sysconfig/network-scripts/ifcfg-lo'
        ]

        mock_list_dir.assert_called_once_with(
            self.netpreserver.network_scripts_path)

        self.assertEqual(result, expected_result)

    @mock.patch.object(ifcfg.IfcfgNetPreserver, '_get_ifcfgs_by_type')
    @mock.patch.object(ifcfg.IfcfgNetPreserver, '_get_net_config_files')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test_check_net_preserver_True(self, mock_test_path,
                                      mock_get_net_config_files,
                                      mock_get_ifcfgs_by_type) -> None:

        mock_test_path.return_value = True
        mock_get_net_config_files.return_value = [
            "etc/sysconfig/network-scripts/ifcfg-eth0",
            "etc/sysconfig/network-scripts/ifcfg-lo"
        ]
        mock_get_ifcfgs_by_type.return_value = [
            {
                "TYPE": "Ethernet",
                "DEVICE": "eth0",
                "HWADDR": "00:11:22:33:44:55",
                "IPADDR": "192.168.1.10"
            },
            {
                "TYPE": "Ethernet",
                "HWADDR": "AA:BB:CC:DD:EE:FF",
                "IPADDR": "192.168.1.11"
            }]

        result = self.netpreserver.check_net_preserver()

        self.assertTrue(result)

    @mock.patch.object(ifcfg.IfcfgNetPreserver, '_get_ifcfgs_by_type')
    @mock.patch.object(ifcfg.IfcfgNetPreserver, '_get_net_config_files')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test_check_net_preserver_no_ifcfg_files(self, mock_test_path,
                                                mock_get_net_config_files,
                                                mock_get_ifcfgs_by_type):
        mock_test_path.return_value = True
        mock_get_net_config_files.return_value = [
            "etc/sysconfig/network-scripts/ifcfg-eth0",
            "etc/sysconfig/network-scripts/ifcfg-lo"
        ]
        mock_get_ifcfgs_by_type.return_value = []

        result = self.netpreserver.check_net_preserver()

        self.assertFalse(result)

    @mock.patch.object(ifcfg.IfcfgNetPreserver, '_get_ifcfgs_by_type')
    def test_parse_network(self, mock_get_ifcfgs_by_type):
        ifcfg_file_with_device = "etc/sysconfig/network-scripts/ifcfg-eth0"
        ifcfg_with_device = {
            "TYPE": "Ethernet",
            "DEVICE": "eth0",
            "HWADDR": "00:11:22:33:44:55",
            "IPADDR": "192.168.1.10"
        }
        ifcfg_file_without_device = "etc/sysconfig/network-scripts/ifcfg-eth1"
        ifcfg_without_device = {
            "TYPE": "Ethernet",
            "HWADDR": "AA:BB:CC:DD:EE:FF",
            "IPADDR": "192.168.1.11"
        }
        ifcfg_file_without_hwaddr = "etc/sysconfig/network-scripts/ifcfg-eth2"
        ifcfg_without_hwaddr = {
            "TYPE": "Ethernet",
            "DEVICE": "eth2",
            "IPADDR": "192.168.1.12"
        }

        mock_get_ifcfgs_by_type.return_value = [
            (ifcfg_file_with_device, ifcfg_with_device),
            (ifcfg_file_without_device, ifcfg_without_device),
            (ifcfg_file_without_hwaddr, ifcfg_without_hwaddr)
        ]

        self.netpreserver.parse_network()

        expected_info = {
            "eth0": {
                "mac_address": "00:11:22:33:44:55",
                "ip_addresses": ["192.168.1.10"]
            },
            "eth1": {
                "mac_address": "AA:BB:CC:DD:EE:FF",
                "ip_addresses": ["192.168.1.11"]
            },
            "eth2": {
                "mac_address": None,
                "ip_addresses": ["192.168.1.12"],
            }
        }

        self.assertEqual(self.netpreserver.interface_info, expected_info)
