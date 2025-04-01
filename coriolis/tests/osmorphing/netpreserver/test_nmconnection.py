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

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_list_dir')
    def test_get_nmconnection_files(self, mock_list_dir):
        mock_list_dir.return_value = [
            'eth0.nmconnection', 'eth1.nmconnection', 'other-file']
        result = self.netpreserver._get_nmconnection_files(
            self.netpreserver.nmconnection_file)
        expected_result = [
            'etc/NetworkManager/system-connections/eth0.nmconnection',
            'etc/NetworkManager/system-connections/eth1.nmconnection'
        ]
        mock_list_dir.assert_called_once_with(
            self.netpreserver.nmconnection_file)
        self.assertEqual(result, expected_result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_config_file')
    @mock.patch.object(nmconnection.NmconnectionNetPreserver,
                       '_get_nmconnection_files')
    def test_get_keyfiles_by_type(self, mock_get_nmconnection_files,
                                  mock_read_config_file):
        mock_get_nmconnection_files.return_value = [mock.sentinel.nmconn_file]
        mock_read_config_file.side_effect = [{"type": "ethernet"}]

        result = self.netpreserver._get_keyfiles_by_type(
            "ethernet", self.netpreserver.nmconnection_file)

        mock_get_nmconnection_files.assert_called_once_with(
            self.netpreserver.nmconnection_file)
        mock_read_config_file.assert_called_once_with(
            mock.sentinel.nmconn_file)
        self.assertEqual(result, [(mock.sentinel.nmconn_file,
                                   {"type": "ethernet"})])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    @mock.patch.object(nmconnection.NmconnectionNetPreserver,
                       '_get_nmconnection_files')
    @mock.patch.object(nmconnection.NmconnectionNetPreserver,
                       '_get_keyfiles_by_type')
    def test_check_net_preserver_True(self, mock_get_keyfiles_by_type,
                                      mock_get_nmconnection_files,
                                      mock_test_path):
        mock_test_path.return_value = True
        mock_get_nmconnection_files.return_value = ["eth0.nmconnection",
                                                    "eth1.nmconnection"]
        mock_get_keyfiles_by_type.return_value = [
            (mock.sentinel.nmconn_file, {"type": "ethernet",
                                         "connection": {"id": "eth0"}})
        ]

        result = self.netpreserver.check_net_preserver()

        self.assertTrue(result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    @mock.patch.object(nmconnection.NmconnectionNetPreserver,
                       '_get_nmconnection_files')
    def test_check_net_preserver_no_files(self, mock_get_nmconnection_files,
                                          mock_test_path):
        mock_test_path.return_value = True
        mock_get_nmconnection_files.return_value = []

        result = self.netpreserver.check_net_preserver()

        self.assertFalse(result)

    @mock.patch.object(nmconnection.NmconnectionNetPreserver,
                       '_get_keyfiles_by_type')
    def test_parse_network(self, mock_get_keyfiles_by_type):
        nmconn_file_with_id = (
            self.netpreserver.nmconnection_file + "/eth0.nmconnection"
        )
        nmconn_with_id = {
            "connection": {"id": "eth0"},
            "mac-address": "00:11:22:33:44:55",
            "address1": "192.168.1.10/24"
        }
        nmconn_file_without_id = (
            self.netpreserver.nmconnection_file + "/eth1.nmconnection"
        )
        nmconn_without_id = {
            "mac-address": "AA:BB:CC:DD:EE:FF",
            "Address2": "192.168.1.20/24, 192.168.1.21/24"
        }
        nmconn_file_without_mac_address = (
            self.netpreserver.nmconnection_file + "/eth2.nmconnection"
        )
        nmconn_without_mac_address = {
            "connection": {"id": "eth2"},
            "address1": "192.168.1.30/24",
        }
        nmconn_file_without_mac_address_ip_address = (
            self.netpreserver.nmconnection_file + "/eth3.nmconnection"
        )
        nmconn_without_mac_address_ip_address = {
            "connection": {"id": "eth3"},
        }
        mock_get_keyfiles_by_type.return_value = [
            (nmconn_file_with_id, nmconn_with_id),
            (nmconn_file_without_id, nmconn_without_id),
            (nmconn_file_without_mac_address, nmconn_without_mac_address),
            (nmconn_file_without_mac_address_ip_address,
             nmconn_without_mac_address_ip_address)
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
            "eth3": {
                "mac_address": None,
                "ip_addresses": []
            }
        }

        self.assertEqual(self.netpreserver.interface_info, expected_info)
