# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import constants
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

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "centos",
            "VERSION_ID": "7.9",
            "NAME": "CentOS Linux",
        }

        expected_info = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": centos.CENTOS_DISTRO_IDENTIFIER,
            "release_version": '7.9',
            "friendly_release_name": "%s Version %s" % (
                centos.CENTOS_DISTRO_IDENTIFIER, '7.9')
        }

        result = self.centos_os_detect_tools.detect_os()

        mock_get_os_release.assert_called_once_with()

        self.assertEqual(result, expected_info)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_centos_stream(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "centos",
            "VERSION_ID": "8.3",
            "NAME": "CentOS Stream",
        }

        expected_info = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": centos.CENTOS_STREAM_DISTRO_IDENTIFIER,
            "release_version": '8.3',
            "friendly_release_name": "%s Version %s" % (
                centos.CENTOS_STREAM_DISTRO_IDENTIFIER, '8.3')
        }

        result = self.centos_os_detect_tools.detect_os()

        mock_get_os_release.assert_called_once_with()

        self.assertEqual(result, expected_info)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_centos_stream_10(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "centos",
            "VERSION_ID": "10",
            "NAME": "CentOS Stream",
        }

        expected_info = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": centos.CENTOS_STREAM_DISTRO_IDENTIFIER,
            "release_version": '10',
            "friendly_release_name": "%s Version %s" % (
                centos.CENTOS_STREAM_DISTRO_IDENTIFIER, '10')
        }

        result = self.centos_os_detect_tools.detect_os()

        self.assertEqual(result, expected_info)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_almalinux(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "almalinux",
            "VERSION_ID": "9.4",
            "NAME": "AlmaLinux",
            "ID_LIKE": "rhel centos fedora",
        }

        expected_info = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": centos.CENTOS_DISTRO_IDENTIFIER,
            "release_version": '9.4',
            "friendly_release_name": "%s Version %s" % (
                centos.CENTOS_DISTRO_IDENTIFIER, '9.4')
        }

        result = self.centos_os_detect_tools.detect_os()

        self.assertEqual(result, expected_info)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_not_centos(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "rocky",
            "VERSION_ID": "8.3",
            "NAME": "Rocky Linux",
        }

        result = self.centos_os_detect_tools.detect_os()

        self.assertEqual(result, {})
        mock_get_os_release.assert_called_once_with()
