# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

import ddt

from coriolis import exception
from coriolis.osmorphing import base
from coriolis.osmorphing import centos
from coriolis.tests import test_base


@ddt.ddt
class BaseCentOSMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseCentOSMorphingTools class."""

    def setUp(self):
        super(BaseCentOSMorphingToolsTestCase, self).setUp()
        self.detected_os_info = {
            'os_type': 'linux',
            'distribution_name': centos.CENTOS_DISTRO_IDENTIFIER,
            'release_version': '7',
            'friendly_release_name': mock.sentinel.friendly_release_name,
        }
        self.enable_repos = ['repo1', 'repo2']
        self.morphing_tools = centos.BaseCentOSMorphingTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.os_root_dir, mock.sentinel.hypervisor,
            mock.sentinel.event_manager, self.detected_os_info,
            mock.sentinel.osmorphing_parameters)

    def test_check_os_supported(self):
        detected_os_info = {
            "distribution_name": centos.CENTOS_DISTRO_IDENTIFIER,
            "release_version": "6"
        }

        result = centos.BaseCentOSMorphingTools.check_os_supported(
            detected_os_info)

        self.assertTrue(result)

    def test_check_os_not_supported(self):
        detected_os_info = {
            "distribution_name": 'unsupported',
        }
        result = centos.BaseCentOSMorphingTools.check_os_supported(
            detected_os_info)

        self.assertFalse(result)

    @ddt.data(
        # CentOS 7 and earlier use yum-config-manager.
        ('6', 'yum-config-manager --enable'),
        ('7', 'yum-config-manager --enable'),
        # CentOS 8+ uses dnf config-manager.
        ('8', 'dnf config-manager --set-enabled'),
        ('9', 'dnf config-manager --set-enabled'),
    )
    @ddt.unpack
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_enable_repos(self, version, expected_cmd, mock_exec_cmd_chroot):
        self.morphing_tools._version = version

        self.morphing_tools.enable_repos(self.enable_repos)

        mock_exec_cmd_chroot.assert_has_calls([
            mock.call("%s repo1" % expected_cmd),
            mock.call("%s repo2" % expected_cmd),
        ])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_enable_repos_empty(self, mock_exec_cmd_chroot):
        self.morphing_tools.enable_repos([])

        mock_exec_cmd_chroot.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_enable_repos_with_exception(self, mock_exec_cmd_chroot):
        self.morphing_tools._version = '7'
        mock_exec_cmd_chroot.side_effect = exception.CoriolisException()

        with self.assertLogs(
                'coriolis.osmorphing.centos', level=logging.WARN):
            self.morphing_tools.enable_repos(['repo1'])

        mock_exec_cmd_chroot.assert_called_once_with(
            "yum-config-manager --enable repo1")
