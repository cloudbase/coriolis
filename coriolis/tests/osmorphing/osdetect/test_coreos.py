# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import coreos
from coriolis.tests import test_base


class CoreOSOSDetectToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the CoreOSOSDetectTools class."""

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "coreos",
            "VERSION_ID": mock.sentinel.version
        }

        expected_info = {
            "os_type": coreos.constants.OS_TYPE_LINUX,
            "distribution_name": coreos.COREOS_DISTRO_IDENTIFIER,
            "release_version": mock.sentinel.version,
            "friendly_release_name": "CoreOS Linux %s" % mock.sentinel.version
        }

        coreos_os_detect_tools = coreos.CoreOSOSDetectTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.operation_timeout)

        result = coreos_os_detect_tools.detect_os()

        mock_get_os_release.assert_called_once_with()

        self.assertEqual(result, expected_info)
