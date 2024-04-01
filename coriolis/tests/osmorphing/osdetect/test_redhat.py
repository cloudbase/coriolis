# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import redhat
from coriolis.tests import test_base


class RedHatOSDetectToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the RedHatOSDetectTools class."""

    def setUp(self):
        super(RedHatOSDetectToolsTestCase, self).setUp()
        self.redhat_os_detect_tools = redhat.RedHatOSDetectTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.operation_timeout)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_file')
    def test_detect_os(self, mock_read_file, mock_test_path):
        mock_test_path.return_value = True
        mock_read_file.return_value = (
            b"Red Hat Enterprise Linux release 8.4 (Ootpa)")

        expected_info = {
            "os_type": redhat.constants.OS_TYPE_LINUX,
            "distribution_name": redhat.RED_HAT_DISTRO_IDENTIFIER,
            "release_version": '8.4',
            "friendly_release_name": "%s Version %s" % (
                redhat.RED_HAT_DISTRO_IDENTIFIER, '8.4')
        }

        result = self.redhat_os_detect_tools.detect_os()

        mock_test_path.assert_called_once_with("etc/redhat-release")
        mock_read_file.assert_called_once_with("etc/redhat-release")

        self.assertEqual(result, expected_info)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_test_path')
    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_file')
    def test_detect_os_no_redhat(self, mock_read_file, mock_test_path):
        mock_test_path.return_value = True
        mock_read_file.return_value = b"CentOS Linux release 8.4 (Ootpa)"

        with self.assertLogs('coriolis.osmorphing.osdetect.redhat',
                             level=logging.DEBUG):
            result = self.redhat_os_detect_tools.detect_os()

            self.assertEqual(result, {})

        mock_test_path.assert_called_once_with("etc/redhat-release")
        mock_read_file.assert_called_once_with("etc/redhat-release")
