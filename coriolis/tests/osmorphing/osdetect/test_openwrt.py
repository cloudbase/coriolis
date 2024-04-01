# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import openwrt
from coriolis.tests import test_base


class OpenWRTOSDetectToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for OpenWRTOSDetectTools class."""

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_config_file')
    def test_detect_os(self, mock_read_config_file):
        mock_read_config_file.return_value = {
            "DISTRIB_ID": "OpenWrt",
            "DISTRIB_DESCRIPTION": "OpenWrt Description",
            "DISTRIB_RELEASE": mock.sentinel.version
        }

        expected_info = {
            "os_type": openwrt.constants.OS_TYPE_LINUX,
            "distribution_name": openwrt.OPENWRT_DISTRO_IDENTIFIER,
            "release_version": mock.sentinel.version,
            "friendly_release_name": "OpenWrt Description Version %s" % (
                mock.sentinel.version)
        }

        openwrt_os_detect_tools = openwrt.OpenWRTOSDetectTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.operation_timeout)

        result = openwrt_os_detect_tools.detect_os()

        mock_read_config_file.assert_called_once_with(
            "etc/openwrt_release", check_exists=True)

        self.assertEqual(result, expected_info)
