# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import constants
from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import rocky
from coriolis.tests import test_base


class RockyLinuxOSDetectToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the RockyLinuxOSDetectTools class."""

    def setUp(self):
        super(RockyLinuxOSDetectToolsTestCase, self).setUp()
        self.rocky_os_detect_tools = rocky.RockyLinuxOSDetectTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.operation_timeout)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "rocky",
            "VERSION_ID": "8.4",
            "NAME": "Rocky Linux",
        }

        expected_info = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": rocky.ROCKY_LINUX_DISTRO_IDENTIFIER,
            "release_version": '8.4',
            "friendly_release_name": "Rocky Linux Version 8.4"
        }

        result = self.rocky_os_detect_tools.detect_os()
        mock_get_os_release.assert_called_once_with()

        self.assertEqual(result, expected_info)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_rocky_10(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "rocky",
            "VERSION_ID": "10.1",
            "NAME": "Rocky Linux",
            "PRETTY_NAME": "Rocky Linux 10.1 (Red Quartz)",
        }

        expected_info = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": rocky.ROCKY_LINUX_DISTRO_IDENTIFIER,
            "release_version": '10.1',
            "friendly_release_name": "Rocky Linux Version 10.1"
        }

        result = self.rocky_os_detect_tools.detect_os()

        self.assertEqual(result, expected_info)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_no_rocky(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "centos",
            "VERSION_ID": "8.4",
            "NAME": "CentOS Linux",
        }

        result = self.rocky_os_detect_tools.detect_os()

        self.assertEqual(result, {})
        mock_get_os_release.assert_called_once_with()
