# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

from coriolis import exception
from coriolis.osmorphing import base, rocky
from coriolis.tests import test_base


class BaseRockyLinuxMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseRockyLinuxMorphingTools class."""

    def setUp(self):
        super(BaseRockyLinuxMorphingToolsTestCase, self).setUp()
        self.detected_os_info = {
            'os_type': 'linux',
            'distribution_name': rocky.ROCKY_LINUX_DISTRO_IDENTIFIER,
            'release_version': '8',
            'friendly_release_name': mock.sentinel.friendly_release_name,
        }
        self.enable_repos = ['repo1', 'repo2']
        self.morphing_tools = rocky.BaseRockyLinuxMorphingTools(
            mock.sentinel.conn,
            mock.sentinel.os_root_dir,
            mock.sentinel.os_root_dir,
            mock.sentinel.hypervisor,
            mock.sentinel.event_manager,
            self.detected_os_info,
            mock.sentinel.osmorphing_parameters,
        )

    def test_check_os_supported(self):
        detected_os_info = {
            "distribution_name": rocky.ROCKY_LINUX_DISTRO_IDENTIFIER,
            "release_version": "8",
        }
        result = rocky.BaseRockyLinuxMorphingTools.check_os_supported(detected_os_info)

        self.assertTrue(result)

    def test_check_os_not_supported(self):
        detected_os_info = {
            "distribution_name": "unsupported",
        }
        result = rocky.BaseRockyLinuxMorphingTools.check_os_supported(detected_os_info)

        self.assertFalse(result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_enable_repos(self, mock_exec_cmd_chroot):
        self.morphing_tools.enable_repos(self.enable_repos)

        mock_exec_cmd_chroot.assert_has_calls(
            [
                mock.call("dnf config-manager --set-enabled repo1"),
                mock.call("dnf config-manager --set-enabled repo2"),
            ]
        )

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_enable_repos_empty(self, mock_exec_cmd_chroot):
        self.morphing_tools.enable_repos([])

        mock_exec_cmd_chroot.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_enable_repos_with_exception(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.side_effect = exception.CoriolisException()

        with self.assertLogs('coriolis.osmorphing.rocky', level=logging.WARN):
            self.morphing_tools.enable_repos(['repo1'])

        mock_exec_cmd_chroot.assert_called_once_with(
            "dnf config-manager --set-enabled repo1"
        )
