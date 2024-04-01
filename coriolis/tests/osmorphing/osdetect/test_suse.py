# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import suse
from coriolis.tests import test_base


class SUSEOSDetectToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the SUSEOSDetectTools class."""

    def setUp(self):
        super(SUSEOSDetectToolsTestCase, self).setUp()

        self.suse_os_detect_tools = suse.SUSEOSDetectTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.operation_timeout)

    @mock.patch.object(
        base.BaseLinuxOSDetectTools, 'returned_detected_os_info_fields')
    def test_returned_detected_os_info_fields(self,
                                              mock_detected_os_info_fields):
        mock_detected_os_info_fields.return_value = [
            "os_type", "distribution_name"]

        result = suse.SUSEOSDetectTools.returned_detected_os_info_fields()

        expected_fields = ["os_type", "distribution_name",
                           suse.DETECTED_SUSE_RELEASE_FIELD_NAME]

        self.assertEqual(result, expected_fields)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_sles(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "NAME": "SLES",
            "VERSION_ID": suse.constants.OS_TYPE_UNKNOWN
        }

        expected_info = {
            "os_type": suse.constants.OS_TYPE_LINUX,
            "distribution_name": suse.SLES_DISTRO_IDENTIFIER,
            suse.DETECTED_SUSE_RELEASE_FIELD_NAME: "SLES",
            "release_version": suse.constants.OS_TYPE_UNKNOWN,
            "friendly_release_name": "SLES %s" % suse.constants.OS_TYPE_UNKNOWN
        }

        result = self.suse_os_detect_tools.detect_os()

        self.assertEqual(result, expected_info)

        mock_get_os_release.assert_called_once()

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_opensuse_tumbleweed(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "NAME": "openSUSE tumbleweed",
            "VERSION_ID": suse.constants.OS_TYPE_UNKNOWN
        }

        expected_info = {
            "os_type": suse.constants.OS_TYPE_LINUX,
            "distribution_name": suse.OPENSUSE_DISTRO_IDENTIFIER,
            suse.DETECTED_SUSE_RELEASE_FIELD_NAME: "openSUSE tumbleweed",
            "release_version": suse.OPENSUSE_TUMBLEWEED_VERSION_IDENTIFIER,
            "friendly_release_name": "openSUSE tumbleweed"
        }

        result = self.suse_os_detect_tools.detect_os()

        self.assertEqual(result, expected_info)

        mock_get_os_release.assert_called_once()

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_opensuse(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "NAME": "openSUSE test",
            "VERSION_ID": "15.3"
        }

        expected_info = {
            "os_type": suse.constants.OS_TYPE_LINUX,
            "distribution_name": suse.OPENSUSE_DISTRO_IDENTIFIER,
            suse.DETECTED_SUSE_RELEASE_FIELD_NAME: "openSUSE test",
            "release_version": "15.3",
            "friendly_release_name": "openSUSE 15.3"
        }

        result = self.suse_os_detect_tools.detect_os()

        self.assertEqual(result, expected_info)

        mock_get_os_release.assert_called_once()
