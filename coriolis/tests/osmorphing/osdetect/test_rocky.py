# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import rocky
from coriolis.tests import test_base


class RockyLinuxOSDetectToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the RockyLinuxOSDetectTools class."""

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_file')
    def test_detect_os(self, mock_read_file, mock_test_path):
        mock_test_path.return_value = True
        mock_read_file.return_value = b"Rocky Linux release 8.4"

        expected_info = {
            "os_type": rocky.constants.OS_TYPE_LINUX,
            "distribution_name": rocky.ROCKY_LINUX_DISTRO_IDENTIFIER,
            "release_version": '8.4',
            "friendly_release_name": "Rocky Linux Version 8.4"
        }

        rocky_os_detect_tools = rocky.RockyLinuxOSDetectTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.operation_timeout)

        result = rocky_os_detect_tools.detect_os()
        mock_test_path.assert_called_once_with("etc/redhat-release")
        mock_read_file.assert_called_once_with("etc/redhat-release")

        self.assertEqual(result, expected_info)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_file')
    def test_detect_os_no_rocky(self, mock_read_file, mock_test_path):
        mock_test_path.return_value = True
        mock_read_file.return_value = b"CentOS Linux release 8.4"

        with self.assertLogs('coriolis.osmorphing.osdetect.rocky',
                             level="DEBUG"):
            rocky_os_detect_tools = rocky.RockyLinuxOSDetectTools(
                mock.sentinel.conn, mock.sentinel.os_root_dir,
                mock.sentinel.operation_timeout)
            result = rocky_os_detect_tools.detect_os()

            self.assertEqual(result, {})

        mock_test_path.assert_called_once_with("etc/redhat-release")
        mock_read_file.assert_called_once_with("etc/redhat-release")
