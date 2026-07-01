# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import constants
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

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "rhel",
            "VERSION_ID": "8.4",
            "NAME": "Red Hat Enterprise Linux",
        }

        expected_info = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": redhat.RED_HAT_DISTRO_IDENTIFIER,
            "release_version": '8.4',
            "friendly_release_name": "%s Version %s" % (
                redhat.RED_HAT_DISTRO_IDENTIFIER, '8.4')
        }

        result = self.redhat_os_detect_tools.detect_os()

        mock_get_os_release.assert_called_once_with()

        self.assertEqual(result, expected_info)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_no_redhat(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "centos",
            "VERSION_ID": "8.4",
            "NAME": "CentOS Linux",
        }

        result = self.redhat_os_detect_tools.detect_os()

        self.assertEqual(result, {})
        mock_get_os_release.assert_called_once_with()
