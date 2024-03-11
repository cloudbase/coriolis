# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

from coriolis import exception
from coriolis.osmorphing import base
from coriolis.osmorphing import suse
from coriolis.tests import test_base


class BaseSUSEMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseSUSEMorphingTools class."""

    def setUp(self):
        super(BaseSUSEMorphingToolsTestCase, self).setUp()
        self.event_manager = mock.MagicMock()
        self.detected_os_info = {
            'os_type': 'linux',
            "distribution_name": suse.SLES_DISTRO_IDENTIFIER,
            "release_version": "12",
            'friendly_release_name': mock.sentinel.friendly_release_name,
            'suse_release_name': 'test release'
        }
        self.package_names = ['package1', 'package2']
        self.morphing_tools = suse.BaseSUSEMorphingTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.os_root_dir, mock.sentinel.hypervisor,
            self.event_manager, self.detected_os_info,
            mock.sentinel.osmorphing_parameters,
            mock.sentinel.operation_timeout)

    def test_get_required_detected_os_info_fields(self):
        result = (
            suse.BaseSUSEMorphingTools.get_required_detected_os_info_fields()
        )

        base_fields = ['os_type', 'distribution_name', 'release_version',
                       'friendly_release_name']
        expected_result = base_fields + [suse.DETECTED_SUSE_RELEASE_FIELD_NAME]

        self.assertEqual(expected_result, result)

    def test_check_os_supported(self):
        result = suse.BaseSUSEMorphingTools.check_os_supported(
            self.detected_os_info)

        self.assertTrue(result)

    def test_check_os_supported_opensuse_tumbleweed(self):
        self.detected_os_info[
            'distribution_name'] = suse.OPENSUSE_DISTRO_IDENTIFIER
        self.detected_os_info[
            'release_version'] = suse.OPENSUSE_TUMBLEWEED_VERSION_IDENTIFIER

        result = suse.BaseSUSEMorphingTools.check_os_supported(
            self.detected_os_info)

        self.assertTrue(result)

    def test_check_os_supported_opensuse_unsupported_version(self):
        self.detected_os_info[
            'distribution_name'] = suse.OPENSUSE_DISTRO_IDENTIFIER
        self.detected_os_info['release_version'] = 'unsupported'

        result = suse.BaseSUSEMorphingTools.check_os_supported(
            self.detected_os_info)

        self.assertFalse(result)

    def test_check_os_not_supported(self):
        self.detected_os_info['distribution_name'] = 'unsupported'
        result = suse.BaseSUSEMorphingTools.check_os_supported(
            self.detected_os_info)

        self.assertFalse(result)

    @mock.patch.object(
        suse.BaseSUSEMorphingTools, '_get_grub2_cfg_location'
    )
    def test_get_update_grub2_command(self, mock_get_grub2_cfg_location):
        result = self.morphing_tools.get_update_grub2_command()

        mock_get_grub2_cfg_location.assert_called_once_with()

        self.assertEqual(
            result,
            "grub2-mkconfig -o %s" % mock_get_grub2_cfg_location.return_value
        )

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__get_grub2_cfg_location_uefi(self, mock_test_path_chroot,
                                          mock_exec_cmd_chroot):
        mock_test_path_chroot.return_value = True

        result = self.morphing_tools._get_grub2_cfg_location()

        self.assertEqual(result, '/boot/efi/EFI/suse/grub.cfg')
        mock_exec_cmd_chroot.assert_has_calls([
            mock.call("mount /boot || true"),
            mock.call("mount /boot/efi || true")
        ])
        mock_test_path_chroot.assert_called_once_with(
            '/boot/efi/EFI/suse/grub.cfg')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__get_grub2_cfg_location_bios(self, mock_test_path_chroot,
                                          mock_exec_cmd_chroot):
        mock_test_path_chroot.side_effect = [False, True]

        result = self.morphing_tools._get_grub2_cfg_location()

        mock_exec_cmd_chroot.assert_has_calls([
            mock.call("mount /boot || true"),
            mock.call("mount /boot/efi || true")
        ])
        mock_test_path_chroot.assert_called_with(
            '/boot/grub2/grub.cfg')

        self.assertEqual(result, '/boot/grub2/grub.cfg')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__get_grub2_cfg_location_not_found(self, mock_test_path_chroot,
                                               mock_exec_cmd_chroot):
        mock_test_path_chroot.return_value = False

        self.assertRaisesRegex(
            Exception,
            "could not determine grub location. boot partition not mounted?",
            self.morphing_tools._get_grub2_cfg_location
        )
        mock_exec_cmd_chroot.assert_has_calls([
            mock.call("mount /boot || true"),
            mock.call("mount /boot/efi || true")
        ])
        mock_test_path_chroot.assert_has_calls([
            mock.call('/boot/efi/EFI/suse/grub.cfg'),
            mock.call('/boot/grub2/grub.cfg')
        ])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__run_dracut(self, mock_exec_cmd_chroot):
        self.morphing_tools._run_dracut()

        mock_exec_cmd_chroot.assert_called_once_with(
            "dracut --regenerate-all -f")

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__run_mkinitrd_success(self, mock_exec_cmd_chroot):
        self.morphing_tools._run_mkinitrd()

        mock_exec_cmd_chroot.assert_called_once_with(
            "mkinitrd")

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__run_mkinitrd_with_exception(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.side_effect = Exception()

        with self.assertLogs('coriolis.osmorphing.suse', level=logging.WARN):
            self.morphing_tools._run_mkinitrd()

    @mock.patch.object(suse.BaseSUSEMorphingTools, '_run_mkinitrd')
    @mock.patch.object(suse.BaseSUSEMorphingTools, '_run_dracut')
    def test__rebuild_initrds(self, mock_run_dracut, mock_run_mkinitrd):
        self.morphing_tools._detected_os_info['release_version'] = "11"

        self.morphing_tools._rebuild_initrds()

        mock_run_mkinitrd.assert_called_once()
        mock_run_dracut.assert_not_called()

    @mock.patch.object(suse.BaseSUSEMorphingTools, '_run_mkinitrd')
    @mock.patch.object(suse.BaseSUSEMorphingTools, '_run_dracut')
    def test__rebuild_initrds_old_version(self, mock_run_dracut,
                                          mock_run_mkinitrd):
        self.morphing_tools._rebuild_initrds()

        mock_run_mkinitrd.assert_not_called()
        mock_run_dracut.assert_called_once()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__has_systemd(self, mock_exec_cmd_chroot):
        result = self.morphing_tools._has_systemd()

        self.assertTrue(result)
        mock_exec_cmd_chroot.assert_called_once_with("rpm -q systemd")

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__has_systemd_with_exception(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.side_effect = Exception()

        result = self.morphing_tools._has_systemd()

        self.assertFalse(result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__enable_sles_module(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.return_value = b"module1\nmodule2\nmodule3"

        self.morphing_tools._enable_sles_module("module2")

        mock_exec_cmd_chroot.assert_has_calls([
            mock.call("SUSEConnect --list-extensions"),
            mock.call("cp /etc/zypp/zypp.conf /etc/zypp/zypp.conf.tmp"),
            mock.call(
                "sed -i -e 's/^gpgcheck.*//g' -e '$ a\\gpgcheck = off' "
                "/etc/zypp/zypp.conf"
            ),
            mock.call(
                'SUSEConnect -p %s' % 'module2'
            ),
            mock.call('mv -f /etc/zypp/zypp.conf.tmp /etc/zypp/zypp.conf'),
            mock.call('zypper --non-interactive --no-gpg-checks refresh')
        ])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__enable_sles_module_with_exception(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.side_effect = [
            b"module output", None, None, Exception()]

        self.assertRaises(exception.CoriolisException,
                          self.morphing_tools._enable_sles_module,
                          mock.sentinel.module)

    @mock.patch.object(suse.BaseSUSEMorphingTools, '_add_repo')
    def test_add_cloud_tools_repo(self, mock_add_repo):
        self.morphing_tools._add_cloud_tools_repo()

        expected_repo = suse.CLOUD_TOOLS_REPO_URI_FORMAT % (
            'test_release', '_12')
        mock_add_repo.assert_called_once_with(expected_repo, 'Cloud-Tools')

    @mock.patch.object(suse.BaseSUSEMorphingTools, '_add_repo')
    def test_add_cloud_tools_repo_with_tumbleweed_version(self, mock_add_repo):
        self.morphing_tools._version = (
            suse.OPENSUSE_TUMBLEWEED_VERSION_IDENTIFIER)

        self.morphing_tools._add_cloud_tools_repo()

        expected_repo = suse.CLOUD_TOOLS_REPO_URI_FORMAT % ('test_release', '')
        mock_add_repo.assert_called_once_with(expected_repo, 'Cloud-Tools')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__get_repos(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.return_value = (
            b"repo1 http://repo1.com\nrepo2 http://repo2.com")

        result = self.morphing_tools._get_repos()

        mock_exec_cmd_chroot.assert_called_once_with(
            "zypper repos -u | awk -F '|' '/^\\s[0-9]+/ {print $2 $7}'")

        expected_result = {
            'repo1': 'http://repo1.com', 'repo2': 'http://repo2.com'}
        self.assertEqual(result, expected_result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(suse.BaseSUSEMorphingTools, '_get_repos')
    def test__add_repo_existing_same_uri(self, mock_get_repos,
                                         mock_exec_cmd_chroot):
        mock_get_repos.return_value = {'alias': 'http://repo.com'}

        with self.assertLogs('coriolis.osmorphing.suse', level=logging.DEBUG):
            self.morphing_tools._add_repo('http://repo.com', 'alias')

        mock_get_repos.assert_called_once()
        mock_exec_cmd_chroot.assert_has_calls([
            mock.call("zypper --non-interactive modifyrepo -e alias"),
            mock.call("zypper --non-interactive --no-gpg-checks refresh")
        ])

    @mock.patch.object(suse.uuid, 'uuid4')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(suse.BaseSUSEMorphingTools, '_get_repos')
    def test__add_repo_new(self, mock_get_repos, mock_exec_cmd_chroot,
                           mock_uuid4):
        mock_get_repos.return_value = {'alias': 'http://oldrepo.com'}

        self.morphing_tools._add_repo('http://newrepo.com', 'alias')

        mock_get_repos.assert_called_once()
        mock_exec_cmd_chroot.assert_has_calls([
            mock.call(
                "zypper --non-interactive addrepo -f http://newrepo.com alias"
                "%s" % mock_uuid4.return_value),
            mock.call("zypper --non-interactive --no-gpg-checks refresh")
        ])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(suse.BaseSUSEMorphingTools, '_get_repos')
    def test__add_repo_with_exception(self, mock_get_repos,
                                      mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.side_effect = Exception()

        self.assertRaises(exception.CoriolisException,
                          self.morphing_tools._add_repo,
                          'http://repo.com', 'alias')

        mock_get_repos.assert_called_once()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_install_packages(self, mock_exec_cmd_chroot):
        self.morphing_tools.install_packages(self.package_names)

        mock_exec_cmd_chroot.assert_called_once_with(
            'zypper --non-interactive install package1 package2')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_install_packages_with_exception(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.side_effect = exception.CoriolisException()

        self.assertRaises(exception.FailedPackageInstallationException,
                          self.morphing_tools.install_packages,
                          self.package_names)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_uninstall_packages(self, mock_exec_cmd_chroot):
        self.morphing_tools.uninstall_packages(self.package_names)

        mock_exec_cmd_chroot.assert_called_once_with(
            'zypper --non-interactive remove package1 package2')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_uninstall_packages_with_exception(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.side_effect = exception.CoriolisException()

        with self.assertLogs('coriolis.osmorphing.suse', level=logging.WARN):
            self.morphing_tools.uninstall_packages(self.package_names)
