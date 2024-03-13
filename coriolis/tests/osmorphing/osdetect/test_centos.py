# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import centos
from coriolis.tests import test_base


class CentOSOSDetectToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the CentOSOSDetectTools class."""

    def setUp(self):
        super(CentOSOSDetectToolsTestCase, self).setUp()
        self.centos_os_detect_tools = centos.CentOSOSDetectTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.operation_timeout)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_file')
    def test_detect_os(self, mock_read_file, mock_test_path):
        mock_test_path.return_value = True
        mock_read_file.return_value = b"CentOS Linux release 7.9 (Core)"

        expected_info = {
            "os_type": centos.constants.OS_TYPE_LINUX,
            "distribution_name": centos.CENTOS_DISTRO_IDENTIFIER,
            "release_version": '7.9',
            "friendly_release_name": "%s Version %s" % (
                centos.CENTOS_DISTRO_IDENTIFIER, '7.9')
        }

        result = self.centos_os_detect_tools.detect_os()

        mock_test_path.assert_called_once_with("etc/redhat-release")
        mock_read_file.assert_called_once_with("etc/redhat-release")

        self.assertEqual(result, expected_info)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_file')
    def test_detect_os_centos_stream(self, mock_read_file, mock_test_path):
        mock_test_path.return_value = True
        mock_read_file.return_value = b"CentOS Stream release 8.3"

        expected_info = {
            "os_type": centos.constants.OS_TYPE_LINUX,
            "distribution_name": centos.CENTOS_STREAM_DISTRO_IDENTIFIER,
            "release_version": '8.3',
            "friendly_release_name": "%s Version %s" % (
                centos.CENTOS_STREAM_DISTRO_IDENTIFIER, '8.3')
        }

        result = self.centos_os_detect_tools.detect_os()

        mock_test_path.assert_called_once_with("etc/redhat-release")
        mock_read_file.assert_called_once_with("etc/redhat-release")

        self.assertEqual(result, expected_info)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_file')
    def test_detect_os_not_centos(self, mock_read_file, mock_test_path):
        mock_test_path.return_value = True
        mock_read_file.return_value = b"dummy release 8.3"

        with self.assertLogs('coriolis.osmorphing.osdetect.centos',
                             level=logging.DEBUG):
            result = self.centos_os_detect_tools.detect_os()

            self.assertEqual(result, {})

        mock_test_path.assert_called_once_with("etc/redhat-release")
        mock_read_file.assert_called_once_with("etc/redhat-release")
