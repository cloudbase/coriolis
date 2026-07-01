# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import constants
from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import oracle
from coriolis.tests import test_base


class OracleOSDetectToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the OracleOSDetectTools class."""

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "ol",
            "VERSION_ID": "8.4",
            "NAME": "Oracle Linux Server",
        }

        expected_info = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": oracle.ORACLE_DISTRO_IDENTIFIER,
            "release_version": '8.4',
            "friendly_release_name": "Oracle Linux Version 8.4"
        }

        oracle_os_detect_tools = oracle.OracleOSDetectTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.operation_timeout)

        result = oracle_os_detect_tools.detect_os()
        mock_get_os_release.assert_called_once_with()

        self.assertEqual(result, expected_info)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_not_oracle(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "rhel",
            "VERSION_ID": "9.8",
            "NAME": "Red Hat Enterprise Linux",
        }

        oracle_os_detect_tools = oracle.OracleOSDetectTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.operation_timeout)

        result = oracle_os_detect_tools.detect_os()

        self.assertEqual(result, {})
