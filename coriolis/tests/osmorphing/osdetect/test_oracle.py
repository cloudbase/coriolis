# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import oracle
from coriolis.tests import test_base


class OracleOSDetectToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the OracleOSDetectTools class."""

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_file')
    def test_detect_os(self, mock_read_file, mock_test_path):
        mock_test_path.return_value = True
        mock_read_file.return_value = b"Oracle Linux release 8.4"

        expected_info = {
            "os_type": oracle.constants.OS_TYPE_LINUX,
            "distribution_name": oracle.ORACLE_DISTRO_IDENTIFIER,
            "release_version": '8.4',
            "friendly_release_name": "Oracle Linux Version 8.4"
        }

        oracle_os_detect_tools = oracle.OracleOSDetectTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.operation_timeout)

        result = oracle_os_detect_tools.detect_os()
        mock_test_path.assert_called_once_with("etc/oracle-release")
        mock_read_file.assert_called_once_with("etc/oracle-release")

        self.assertEqual(result, expected_info)
