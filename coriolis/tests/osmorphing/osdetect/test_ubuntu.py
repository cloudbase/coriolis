# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import ubuntu
from coriolis.tests import test_base


class UbuntuOSDetectToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the UbuntuOSDetectTools class."""

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_config_file')
    def test_detect_os(self, mock_read_config_file):
        mock_read_config_file.return_value = {
            "DISTRIB_ID": "Ubuntu",
            "DISTRIB_RELEASE": mock.sentinel.version
        }

        expected_info = {
            "os_type": ubuntu.constants.OS_TYPE_LINUX,
            "distribution_name": ubuntu.UBUNTU_DISTRO_IDENTIFIER,
            "release_version": mock.sentinel.version,
            "friendly_release_name": "Ubuntu %s" % (
                mock.sentinel.version)
        }

        ubuntu_os_detect_tools = ubuntu.UbuntuOSDetectTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.operation_timeout)

        result = ubuntu_os_detect_tools.detect_os()

        mock_read_config_file.assert_called_once_with(
            "etc/lsb-release", check_exists=True)

        self.assertEqual(result, expected_info)
