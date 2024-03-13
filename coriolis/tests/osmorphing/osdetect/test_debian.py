# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import debian
from coriolis.tests import test_base


class DebianOSDetectToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the DebianOSDetectTools class."""

    def setUp(self):
        super(DebianOSDetectToolsTestCase, self).setUp()
        self.debian_os_detect_tools = debian.DebianOSDetectTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.operation_timeout)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_config_file')
    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_file')
    def test_detect_os(self, mock_read_file, mock_read_config_file,
                       mock_test_path):
        mock_test_path.return_value = True
        mock_read_config_file.return_value = {
            'DISTRIB_ID': 'Debian',
            'DISTRIB_RELEASE': '10'
        }

        expected_info = {
            "os_type": debian.constants.OS_TYPE_LINUX,
            "distribution_name": debian.DEBIAN_DISTRO_IDENTIFIER,
            "release_version": '10',
            "friendly_release_name": "Debian Linux 10"
        }

        result = self.debian_os_detect_tools.detect_os()

        mock_test_path.assert_called_once_with("etc/lsb-release")
        mock_read_config_file.assert_called_once_with("etc/lsb-release")
        mock_read_file.assert_not_called()

        self.assertEqual(result, expected_info)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_config_file')
    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_file')
    def test_detect_os_debian_version(
            self, mock_read_file, mock_read_config_file, mock_test_path):
        mock_test_path.side_effect = [False, True]
        mock_read_file.return_value = b"10\n"

        expected_info = {
            "os_type": debian.constants.OS_TYPE_LINUX,
            "distribution_name": debian.DEBIAN_DISTRO_IDENTIFIER,
            "release_version": '10',
            "friendly_release_name": "Debian Linux 10"
        }

        result = self.debian_os_detect_tools.detect_os()

        mock_read_config_file.assert_not_called()
        mock_test_path.assert_has_calls([
            mock.call("etc/lsb-release"),
            mock.call("etc/debian_version")
        ])
        mock_read_file.assert_called_once_with("etc/debian_version")

        self.assertEqual(result, expected_info)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_config_file')
    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_file')
    @mock.patch.object(base.BaseLinuxOSDetectTools, '_test_path')
    def test_detect_os_no_release(self, mock_test_path, mock_read_file,
                                  mock_read_config_file):
        mock_test_path.return_value = False

        result = self.debian_os_detect_tools.detect_os()

        self.assertEqual(result, {})

        mock_test_path.assert_has_calls([
            mock.call("etc/lsb-release"),
            mock.call("etc/debian_version")
        ])
        mock_read_file.assert_not_called()
        mock_read_config_file.assert_not_called()
