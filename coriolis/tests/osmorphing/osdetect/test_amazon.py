# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.


from unittest import mock

from coriolis.osmorphing.osdetect import amazon
from coriolis.osmorphing.osdetect import base
from coriolis.tests import test_base


class AmazonLinuxOSDetectToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for AmazonLinuxOSDetectTools class."""

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "amzn",
            "VERSION": mock.sentinel.version,
            "NAME": "Amazon Linux"
        }

        expected_info = {
            "os_type": amazon.constants.OS_TYPE_LINUX,
            "distribution_name": amazon.AMAZON_DISTRO_NAME,
            "release_version": mock.sentinel.version,
            "friendly_release_name": "Amazon Linux %s" % mock.sentinel.version
        }

        amazon_os_detect_tools = amazon.AmazonLinuxOSDetectTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.operation_timeout)

        result = amazon_os_detect_tools.detect_os()

        mock_get_os_release.assert_called_once_with()

        self.assertEqual(result, expected_info)
