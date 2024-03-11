# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing import base
from coriolis.osmorphing import oracle
from coriolis.osmorphing.osdetect import oracle as oracle_detect
from coriolis.osmorphing import redhat
from coriolis.tests import test_base


class BaseOracleMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test case for the BaseOracleMorphingTools class."""

    def setUp(self):
        super(BaseOracleMorphingToolsTestCase, self).setUp()
        self.detected_os_info = {
            'os_type': 'linux',
            'distribution_name': oracle_detect.ORACLE_DISTRO_IDENTIFIER,
            'release_version': '6',
            'friendly_release_name': mock.sentinel.friendly_release_name,
        }
        self.oracle_morphing_tools = oracle.BaseOracleMorphingTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.os_root_dir, mock.sentinel.hypervisor,
            mock.sentinel.event_manager, self.detected_os_info,
            mock.sentinel.osmorphing_parameters)

    def test_check_os_supported(self):
        result = oracle.BaseOracleMorphingTools.check_os_supported(
            self.detected_os_info)

        self.assertTrue(result)

    def test_check_os_not_supported(self):
        self.detected_os_info['distribution_name'] = 'unsupported'

        result = oracle.BaseOracleMorphingTools.check_os_supported(
            self.detected_os_info)

        self.assertFalse(result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_find_yum_repos')
    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_yum_install')
    def test__get_oracle_repos(self, mock_yum_install, mock_find_yum_repos,
                               mock_exec_cmd_chroot):
        self.oracle_morphing_tools._version = '10'

        result = self.oracle_morphing_tools._get_oracle_repos()

        self.assertEqual(result, mock_find_yum_repos.return_value)

        mock_find_yum_repos.assert_has_calls([
            mock.call(["ol10_baseos_latest"]),
            mock.call([
                "ol10_baseos_latest",
                "ol10_appstream",
                "ol10_addons",
                "ol10_UEKR8"
            ]),
        ])
        mock_yum_install.assert_called_once_with(
            ['oraclelinux-release-el10'], mock_find_yum_repos.return_value)
        mock_exec_cmd_chroot.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_find_yum_repos')
    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_yum_install')
    @mock.patch.object(oracle.uuid, 'uuid4')
    def test__get_oracle_repos_major_version_lt_8(
            self, mock_uuid4, mock_yum_install, mock_find_yum_repos,
            mock_exec_cmd_chroot):
        result = self.oracle_morphing_tools._get_oracle_repos()

        self.assertEqual(result, mock_find_yum_repos.return_value)

        mock_find_yum_repos.assert_called_once_with([
            "ol6_software_collections",
            "ol6_addons",
            "ol6_UEKR",
            "ol6_latest"
        ])
        mock_yum_install.assert_not_called()
        mock_exec_cmd_chroot.assert_has_calls([
            mock.call(
                "curl -L http://public-yum.oracle.com/public-yum-ol6.repo "
                "-o /etc/yum.repos.d/%s.repo" % mock_uuid4.return_value),
            mock.call('sed -i "s/^enabled=1$/enabled=0/g" %s' % (
                "/etc/yum.repos.d/%s.repo" % mock_uuid4.return_value))
        ])
