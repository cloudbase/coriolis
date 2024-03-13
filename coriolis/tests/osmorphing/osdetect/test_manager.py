# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import exception
from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import manager
from coriolis.tests import test_base


class MockOSDetectTool(base.BaseOSDetectTools):
    def returned_detected_os_info_fields(self):
        return ['os_type', 'os_version']

    def detect_os(self):
        return {
            'os_type': 'mock_os_type',
            'os_version': 'mock_os_version'
        }


class MockOSDetectToolNoInfo(base.BaseOSDetectTools):
    def returned_detected_os_info_fields(cls):
        pass

    def detect_os(self):
        pass


class MockOSDetectToolMissingFields(MockOSDetectTool):
    def returned_detected_os_info_fields(self):
        return ['os_type', 'os_version', 'extra_field']

    def detect_os(self):
        return {
            'os_type': 'mock_os_type',
            'os_version': 'mock_os_version'
        }


class MockOSDetectToolExtraFields(MockOSDetectTool):
    def returned_detected_os_info_fields(self):
        return ['os_type', 'os_version']

    def detect_os(self):
        return {
            'os_type': 'mock_os_type',
            'os_version': 'mock_os_version',
            'extra_field': 'extra_value'
        }


class ManagerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis manager module."""

    def setUp(self):
        super(ManagerTestCase, self).setUp()
        self.os_type = "linux"
        self.os_root_dir = "/root"
        self.conn = mock.MagicMock()

    def test__check_custom_os_detect_tools(self):
        # Create a mock object that is an instance of BaseOSDetectTools
        mock_os_detect_tool = mock.MagicMock(spec=base.BaseOSDetectTools)

        result = manager._check_custom_os_detect_tools([mock_os_detect_tool])

        self.assertTrue(result)

    def test_check_custom_os_detect_tools_not_list(self):
        self.assertRaises(exception.InvalidCustomOSDetectTools,
                          manager._check_custom_os_detect_tools, "not a list")

    def test_check_custom_os_detect_tools_invalid_type(self):
        self.assertRaises(exception.InvalidCustomOSDetectTools,
                          manager._check_custom_os_detect_tools, [object()])

    @mock.patch.object(manager, '_check_custom_os_detect_tools')
    def test_detect_os_custom_tools(self, mock_check_custom_tools):
        mock_os_detect_tool = MockOSDetectTool

        result = manager.detect_os(
            self.conn, self.os_type, self.os_root_dir,
            mock.sentinel.operation_timeout,
            tools_environment=mock.sentinel.tools_environment,
            custom_os_detect_tools=[mock_os_detect_tool])

        mock_check_custom_tools.assert_called_once_with([mock_os_detect_tool])

        expected_result = {
            'os_type': 'mock_os_type',
            'os_version': 'mock_os_version'
        }

        self.assertEqual(result, expected_result)

    @mock.patch.object(manager, '_check_custom_os_detect_tools')
    def test_detect_os_windows(self, mock_check_custom_tools):
        mock_os_detect_tool = MockOSDetectTool
        self.os_type = 'windows'

        result = manager.detect_os(
            self.conn, self.os_type, self.os_root_dir,
            mock.sentinel.operation_timeout,
            custom_os_detect_tools=[mock_os_detect_tool])

        mock_check_custom_tools.assert_called_once_with([mock_os_detect_tool])
        expected_result = {
            'os_type': 'mock_os_type',
            'os_version': 'mock_os_version'
        }

        self.assertEqual(result, expected_result)

    def test_detect_os_invalid_os_type(self):
        self.os_type = 'invalid_os_type'

        self.assertRaises(exception.OSDetectToolsNotFound, manager.detect_os,
                          self.conn, self.os_type, self.os_root_dir,
                          mock.sentinel.operation_timeout)

    @mock.patch.object(manager, '_check_custom_os_detect_tools')
    @mock.patch.object(manager.rocky.RockyLinuxOSDetectTools, 'detect_os')
    @mock.patch.object(manager.redhat.RedHatOSDetectTools, 'detect_os')
    @mock.patch.object(manager.centos.CentOSOSDetectTools, 'detect_os')
    @mock.patch.object(manager.oracle.OracleOSDetectTools, 'detect_os')
    def test_detect_os_no_detected_info(
            self, mock_oracle_detect_os, mock_centos_detect_os,
            mock_redhat_detect_os, mock_rocky_detect_os,
            mock_check_custom_tools):
        mock_rocky_detect_os.return_value = None
        mock_redhat_detect_os.return_value = None
        mock_centos_detect_os.return_value = None
        mock_oracle_detect_os.return_value = None
        mock_os_detect_tool = MockOSDetectToolNoInfo

        self.assertRaises(exception.OSDetectToolsNotFound, manager.detect_os,
                          self.conn, self.os_type, self.os_root_dir,
                          mock.sentinel.operation_timeout,
                          custom_os_detect_tools=[mock_os_detect_tool])
        mock_check_custom_tools.assert_called_once_with([mock_os_detect_tool])

    @mock.patch.object(manager, '_check_custom_os_detect_tools')
    @mock.patch.object(manager.rocky.RockyLinuxOSDetectTools, 'detect_os')
    @mock.patch.object(manager.redhat.RedHatOSDetectTools, 'detect_os')
    @mock.patch.object(manager.centos.CentOSOSDetectTools, 'detect_os')
    @mock.patch.object(manager.oracle.OracleOSDetectTools, 'detect_os')
    def test_detect_os_invalid_detected_info(
            self, mock_oracle_detect_os, mock_centos_detect_os,
            mock_redhat_detect_os, mock_rocky_detect_os,
            mock_check_custom_tools):
        mock_rocky_detect_os.return_value = None
        mock_redhat_detect_os.return_value = None
        mock_centos_detect_os.return_value = None
        mock_oracle_detect_os.return_value = "invalid_detected_info"
        mock_os_detect_tool = MockOSDetectToolNoInfo

        self.assertRaises(exception.InvalidDetectedOSParams, manager.detect_os,
                          self.conn, self.os_type, self.os_root_dir,
                          mock.sentinel.operation_timeout,
                          custom_os_detect_tools=[mock_os_detect_tool])
        mock_check_custom_tools.assert_called_once_with([mock_os_detect_tool])

    @mock.patch.object(manager, '_check_custom_os_detect_tools')
    @mock.patch.object(manager.rocky.RockyLinuxOSDetectTools, 'detect_os')
    @mock.patch.object(manager.redhat.RedHatOSDetectTools, 'detect_os')
    @mock.patch.object(manager.centos.CentOSOSDetectTools, 'detect_os')
    @mock.patch.object(manager.oracle.OracleOSDetectTools, 'detect_os')
    def test_detect_os_missing_detected_info_fields(
            self, mock_oracle_detect_os, mock_centos_detect_os,
            mock_redhat_detect_os, mock_rocky_detect_os,
            mock_check_custom_tools):
        mock_rocky_detect_os.return_value = None
        mock_redhat_detect_os.return_value = None
        mock_centos_detect_os.return_value = None
        mock_oracle_detect_os.return_value = None
        mock_os_detect_tool = MockOSDetectToolMissingFields

        self.assertRaises(exception.InvalidDetectedOSParams, manager.detect_os,
                          self.conn, self.os_type, self.os_root_dir,
                          mock.sentinel.operation_timeout,
                          custom_os_detect_tools=[mock_os_detect_tool])
        mock_check_custom_tools.assert_called_once_with([mock_os_detect_tool])

    @mock.patch.object(manager, '_check_custom_os_detect_tools')
    @mock.patch.object(manager.rocky.RockyLinuxOSDetectTools, 'detect_os')
    @mock.patch.object(manager.redhat.RedHatOSDetectTools, 'detect_os')
    @mock.patch.object(manager.centos.CentOSOSDetectTools, 'detect_os')
    @mock.patch.object(manager.oracle.OracleOSDetectTools, 'detect_os')
    def test_detect_os_extra_detected_info_fields(
            self, mock_oracle_detect_os, mock_centos_detect_os,
            mock_redhat_detect_os, mock_rocky_detect_os,
            mock_check_custom_tools):
        mock_rocky_detect_os.return_value = None
        mock_redhat_detect_os.return_value = None
        mock_centos_detect_os.return_value = None
        mock_oracle_detect_os.return_value = None
        mock_os_detect_tool = MockOSDetectToolExtraFields

        self.assertRaises(exception.InvalidDetectedOSParams, manager.detect_os,
                          self.conn, self.os_type, self.os_root_dir,
                          mock.sentinel.operation_timeout,
                          custom_os_detect_tools=[mock_os_detect_tool])
        mock_check_custom_tools.assert_called_once_with([mock_os_detect_tool])
