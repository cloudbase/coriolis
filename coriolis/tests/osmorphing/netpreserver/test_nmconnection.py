# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing import base
from coriolis.osmorphing.netpreserver import nmconnection
from coriolis.osmorphing import redhat
from coriolis.tests import test_base


class NmconnectionNetPreserverTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the NmconnectionNetPreserver class."""

    def setUp(self):
        super(NmconnectionNetPreserverTestCase, self).setUp()
        self.event_manager = mock.MagicMock()
        self.detected_os_info = {
            'os_type': 'linux',
            'distribution_name': redhat.RED_HAT_DISTRO_IDENTIFIER,
            'release_version': '6',
            'friendly_release_name': mock.sentinel.friendly_release_name,
        }
        self.netpreserver = nmconnection.NmconnectionNetPreserver(
            redhat.BaseRedHatMorphingTools(
                mock.sentinel.conn, mock.sentinel.os_root_dir,
                mock.sentinel.os_root_dir, mock.sentinel.hypervisor,
                self.event_manager, self.detected_os_info,
                mock.sentinel.osmorphing_parameters,
                mock.sentinel.operation_timeout)
        )

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_get_keyfiles_by_type')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_get_nmconnection_files')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test_check_net_preserver_True(self, mock_test_path,
                                      mock_get_nmconnection_files,
                                      mock_get_keyfiles_by_type):
        mock_test_path.return_value = True
        mock_get_nmconnection_files.return_value = [
            'etc/NetworkManager/system-connections/eth0.nmconnection']
        mock_get_keyfiles_by_type.return_value = [
            (mock.sentinel.nmconn_file, {"type": "ethernet"})]

        result = self.netpreserver.check_net_preserver()

        self.assertTrue(result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_get_keyfiles_by_type')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_get_nmconnection_files')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test_check_net_preserver_no_ethernet_files(
            self, mock_test_path, mock_get_nmconnection_files,
            mock_get_keyfiles_by_type):
        mock_test_path.return_value = True
        mock_get_nmconnection_files.return_value = [
            'etc/NetworkManager/system-connections/vpn.nmconnection']
        mock_get_keyfiles_by_type.return_value = []

        result = self.netpreserver.check_net_preserver()

        self.assertFalse(result)

    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_get_nmconnection_files')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test_check_net_preserver_no_files(self, mock_test_path,
                                          mock_get_nmconnection_files):
        mock_test_path.return_value = True
        mock_get_nmconnection_files.return_value = []

        result = self.netpreserver.check_net_preserver()

        self.assertFalse(result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test_check_net_preserver_no_dir(self, mock_test_path):
        mock_test_path.return_value = False

        result = self.netpreserver.check_net_preserver()

        self.assertFalse(result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_get_keyfiles_by_type')
    def test_parse_network(self, mock_get_keyfiles_by_type):
        nmconnection_path = base.BaseLinuxOSMorphingTools._NM_CONNECTIONS_PATH
        nmconn_file_with_id = (
            nmconnection_path + "/eth0.nmconnection"
        )
        nmconn_with_id = {
            "id": "eth0",
            "mac-address": "00:11:22:33:44:55",
            "address1": "192.168.1.10/24"
        }
        nmconn_file_without_id = (
            nmconnection_path + "/eth1.nmconnection"
        )
        nmconn_without_id = {
            "mac-address": "AA:BB:CC:DD:EE:FF",
            "Address2": "192.168.1.20/24, 192.168.1.21/24"
        }
        nmconn_file_without_mac_address = (
            nmconnection_path + "/eth2.nmconnection"
        )
        nmconn_without_mac_address = {
            "id": "eth2",
            "address1": "192.168.1.30/24",
        }
        nmconn_file_without_mac_address_ip_address = (
            nmconnection_path + "/eth3.nmconnection"
        )
        nmconn_without_mac_address_ip_address = {
            "id": "id_eth3",
            "address": "192.168.1.40/24",
        }
        nmconn_file_with_space = (
            nmconnection_path + "/System ethx.nmconnection"
        )
        nmconn_without_mac_address_and_id = {
            "address1": "192.168.1.50/24",
        }
        nmconn_with_interface_name = {
            "interface-name": "eth4",
            "id": "id_eth4",
            "address": "192.168.1.60/24",
        }
        mock_get_keyfiles_by_type.return_value = [
            (nmconn_file_with_id, nmconn_with_id),
            (nmconn_file_without_id, nmconn_without_id),
            (nmconn_file_without_mac_address, nmconn_without_mac_address),
            (nmconn_file_without_mac_address_ip_address,
             nmconn_without_mac_address_ip_address),
            (nmconn_file_with_space, nmconn_without_mac_address_and_id),
            (nmconn_file_with_space, nmconn_with_interface_name)
        ]

        self.netpreserver.interface_info = {}

        self.netpreserver.parse_network()

        expected_info = {
            "eth0": {
                "mac_address": "00:11:22:33:44:55",
                "ip_addresses": ["192.168.1.10"]
            },
            "eth1": {
                "mac_address": "AA:BB:CC:DD:EE:FF",
                "ip_addresses": ["192.168.1.20", "192.168.1.21"]
            },
            "eth2": {
                "mac_address": None,
                "ip_addresses": ["192.168.1.30"]
            },
            "id_eth3": {
                "mac_address": None,
                "ip_addresses": ["192.168.1.40"]
            },
            "System_ethx": {
                "mac_address": None,
                "ip_addresses": ["192.168.1.50"]
            },
            "eth4": {
                "mac_address": None,
                "ip_addresses": ["192.168.1.60"]
            }
        }

        self.assertEqual(expected_info, self.netpreserver.interface_info)
