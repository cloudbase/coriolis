# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
import os
from unittest import mock

import ddt

from coriolis import exception
from coriolis.osmorphing.osdetect import windows
from coriolis.tests import test_base


WIN_VERSION_PS_OUTPUT = """
CurrentVersion            : 6.3
CurrentMajorVersionNumber : 10
CurrentMinorVersionNumber : 0
CurrentBuildNumber        : 20348
InstallationType          : Server
ProductName               : Windows Server 2022 Datacenter Evaluation
EditionID                 : ServerDatacenterEval
"""

WIN_VERSION_PS_OUTPUT_MISSING_FIELDS = """
CurrentMinorVersionNumber : 0
CurrentBuildNumber        : 20348
InstallationType          : Server
ProductName               : Windows Server 2022 Datacenter Evaluation
EditionID                 : ServerDatacenterEval
"""

WIN_VERSION_PS_OUTPUT_MISSING_MAJOR_VERSION = """
CurrentVersion            : 6.3
CurrentMinorVersionNumber : 0
CurrentBuildNumber        : 20348
InstallationType          : Server
ProductName               : Windows Server 2022 Datacenter Evaluation
EditionID                 : ServerDatacenterEval
"""


@ddt.ddt
class WindowsOSDetectToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the WindowsOSDetectTools class."""

    def setUp(self):
        super(WindowsOSDetectToolsTestCase, self).setUp()
        self.conn = mock.MagicMock()
        self.os_root_dir = 'C:\\'
        self.conn.EOL = '\n'

        self.windows_os_detect_tools = windows.WindowsOSDetectTools(
            self.conn, self.os_root_dir, mock.sentinel.operation_timeout)

    def test_returned_detected_os_info_fields(self):
        expected_base_fields = [
            "os_type",
            "distribution_name",
            "release_version",
            "friendly_release_name",
            "version_number",
            "edition_id",
            "installation_type",
            "product_name"
        ]

        result = (
            windows.WindowsOSDetectTools.returned_detected_os_info_fields()
        )

        self.assertEqual(result, expected_base_fields)

    def test__load_registry_hive(self):
        self.windows_os_detect_tools._load_registry_hive(
            mock.sentinel.subkey, mock.sentinel.path)

        self.conn.exec_command.assert_called_once_with(
            "reg.exe", ["load", mock.sentinel.subkey, mock.sentinel.path])

    def test__unload_registry_hive(self):
        self.windows_os_detect_tools._unload_registry_hive(
            mock.sentinel.subkey)

        self.conn.exec_command.assert_called_once_with(
            "reg.exe", ["unload", mock.sentinel.subkey])

    def test__get_ps_fl_value(self):
        result = self.windows_os_detect_tools._get_ps_fl_value(
            WIN_VERSION_PS_OUTPUT, 'CurrentVersion')

        self.assertEqual(result, '6.3')

    @ddt.data(
        {
            "ps_output": WIN_VERSION_PS_OUTPUT,
            "expected_result": (
                windows.version.LooseVersion("10.0.20348"),
                "ServerDatacenterEval",
                "Server",
                "Windows Server 2022 Datacenter Evaluation"
            ),
        },
        {
            "ps_output": WIN_VERSION_PS_OUTPUT_MISSING_MAJOR_VERSION,
            "expected_result": (
                "6.3.20348",
                "ServerDatacenterEval",
                "Server",
                "Windows Server 2022 Datacenter Evaluation"
            )
        }
    )
    @mock.patch.object(windows.WindowsOSDetectTools, '_load_registry_hive')
    @mock.patch.object(windows.WindowsOSDetectTools, '_unload_registry_hive')
    @mock.patch.object(windows.uuid, 'uuid4')
    def test__get_image_version_info(
            self, data, mock_uuid4, mock_unload_registry_hive,
            mock_load_registry_hive):
        self.conn.exec_ps_command.return_value = (
            data["ps_output"].replace('\n', os.linesep)
        )

        result = self.windows_os_detect_tools._get_image_version_info()

        mock_load_registry_hive.assert_called_once_with(
            "HKLM\\%s" % mock_uuid4.return_value,
            "%sWindows\\System32\\config\\SOFTWARE" % self.os_root_dir)

        mock_unload_registry_hive.assert_called_once_with(
            "HKLM\\%s" % mock_uuid4.return_value)

        self.assertEqual(result, data["expected_result"])

    @mock.patch.object(windows.WindowsOSDetectTools, '_load_registry_hive')
    @mock.patch.object(windows.WindowsOSDetectTools, '_unload_registry_hive')
    def test__get_image_version_info_with_exception(
            self, mock_unload_registry_hive,
            mock_load_registry_hive):
        self.conn.exec_ps_command.return_value = (
            WIN_VERSION_PS_OUTPUT_MISSING_FIELDS.replace('\n', os.linesep))

        mock_unload_registry_hive.assert_not_called()
        mock_load_registry_hive.assert_not_called()

        self.assertRaises(
            exception.CoriolisException,
            self.windows_os_detect_tools._get_image_version_info
        )

    @ddt.data(
        {
            'version_number': mock.sentinel.version_number,
            'edition_id': 'server',
            'installation_type': mock.sentinel.installation_type,
            'product_name': mock.sentinel.product_name,
            'distribution_name': windows.WINDOWS_SERVER_IDENTIFIER,
        },
        {
            'version_number': mock.sentinel.version_number,
            'edition_id': 'client',
            'installation_type': mock.sentinel.installation_type,
            'product_name': mock.sentinel.product_name,
            'distribution_name': windows.WINDOWS_CLIENT_IDENTIFIER,
        }
    )
    @mock.patch.object(windows.WindowsOSDetectTools, '_get_image_version_info')
    def test_detect_os(self, data, mock_get_image_version_info):
        mock_get_image_version_info.return_value = (
            data['version_number'],
            data['edition_id'],
            data['installation_type'],
            data['product_name']
        )

        expected_result = {
            "version_number": data['version_number'],
            "edition_id": data['edition_id'],
            "installation_type": data['installation_type'],
            "product_name": data['product_name'],
            "os_type": windows.constants.OS_TYPE_WINDOWS,
            "distribution_name": data['distribution_name'],
            "release_version": data['product_name'],
            "friendly_release_name": "Windows %s" % data['product_name']
        }

        with self.assertLogs('coriolis.osmorphing.osdetect.windows',
                             level=logging.DEBUG):
            result = self.windows_os_detect_tools.detect_os()

            self.assertEqual(result, expected_result)

    @mock.patch.object(windows.WindowsOSDetectTools, '_get_image_version_info')
    def test_detect_os_with_exception(self, mock_get_image_version_info):
        mock_get_image_version_info.side_effect = exception.CoriolisException

        with self.assertLogs('coriolis.osmorphing.osdetect.windows',
                             level=logging.DEBUG):
            self.assertRaises(exception.CoriolisException,
                              self.windows_os_detect_tools.detect_os)

        mock_get_image_version_info.assert_called_once_with()
