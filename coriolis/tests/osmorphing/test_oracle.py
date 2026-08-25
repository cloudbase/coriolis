# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

import ddt

from coriolis import exception
from coriolis.osmorphing import base, oracle
from coriolis.tests import test_base


@ddt.ddt
class BaseOracleMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test case for the BaseOracleMorphingTools class."""

    def setUp(self):
        super(BaseOracleMorphingToolsTestCase, self).setUp()
        self.detected_os_info = {
            'os_type': 'linux',
            'distribution_name': oracle.ORACLE_DISTRO_IDENTIFIER,
            'release_version': '7',
            'friendly_release_name': mock.sentinel.friendly_release_name,
        }
        self.enable_repos = ['repo1', 'repo2']
        self.oracle_morphing_tools = oracle.BaseOracleMorphingTools(
            mock.sentinel.conn,
            mock.sentinel.os_root_dir,
            mock.sentinel.os_root_dir,
            mock.sentinel.hypervisor,
            mock.sentinel.event_manager,
            self.detected_os_info,
            mock.sentinel.osmorphing_parameters,
        )

    @ddt.data(
        ('7', True),
        ('8', True),
        ('9', True),
        ('9.4', True),
        ('10', True),
        ('10.0', True),
        ('11', True),
        ('5', False),
        ('abc', False),
    )
    @ddt.unpack
    def test_check_os_supported(self, release_version, expected):
        self.detected_os_info['release_version'] = release_version
        result = oracle.BaseOracleMorphingTools.check_os_supported(
            self.detected_os_info
        )

        self.assertEqual(expected, result)

    def test_check_os_not_supported(self):
        self.detected_os_info['distribution_name'] = 'unsupported'

        result = oracle.BaseOracleMorphingTools.check_os_supported(
            self.detected_os_info
        )

        self.assertFalse(result)

    @ddt.data(
        # OL7 and earlier use yum-config-manager.
        ('7', 'yum-config-manager --enable'),
        # OL8+ uses dnf config-manager.
        ('8', 'dnf config-manager --set-enabled'),
        ('9', 'dnf config-manager --set-enabled'),
        ('10', 'dnf config-manager --set-enabled'),
    )
    @ddt.unpack
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_enable_repos(self, version, expected_cmd, mock_exec_cmd_chroot):
        self.oracle_morphing_tools._version = version

        self.oracle_morphing_tools.enable_repos(self.enable_repos)

        mock_exec_cmd_chroot.assert_has_calls(
            [
                mock.call("%s repo1" % expected_cmd),
                mock.call("%s repo2" % expected_cmd),
            ]
        )

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_enable_repos_empty(self, mock_exec_cmd_chroot):
        self.oracle_morphing_tools.enable_repos([])

        mock_exec_cmd_chroot.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_enable_repos_with_exception(self, mock_exec_cmd_chroot):
        self.oracle_morphing_tools._version = '7'
        mock_exec_cmd_chroot.side_effect = exception.CoriolisException()

        with self.assertLogs('coriolis.osmorphing.oracle', level=logging.WARN):
            self.oracle_morphing_tools.enable_repos(['repo1'])

        mock_exec_cmd_chroot.assert_called_once_with(
            "yum-config-manager --enable repo1"
        )
