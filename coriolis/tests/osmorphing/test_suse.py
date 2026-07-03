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

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_get_installed_packages(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.return_value = "package1\npackage2"

        self.morphing_tools.get_installed_packages()

        self.assertEqual(
            self.morphing_tools.installed_packages,
            ['package1', 'package2']
        )
        mock_exec_cmd_chroot.assert_called_once_with(
            'rpm -qa --qf "%{NAME}\\n"')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_get_installed_packages_none(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.side_effect = exception.CoriolisException()

        with self.assertLogs(
            'coriolis.osmorphing.suse', level=logging.DEBUG):
            self.morphing_tools.get_installed_packages()

        self.assertEqual(
            self.morphing_tools.installed_packages,
            []
        )
        mock_exec_cmd_chroot.assert_called_once_with(
            'rpm -qa --qf "%{NAME}\\n"')

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

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__get_grub2_cfg_location_uefi(self, mock_test_path_chroot):
        mock_test_path_chroot.return_value = True

        result = self.morphing_tools._get_grub2_cfg_location()

        self.assertEqual(result, '/boot/efi/EFI/suse/grub.cfg')
        mock_test_path_chroot.assert_called_once_with(
            '/boot/efi/EFI/suse/grub.cfg')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__get_grub2_cfg_location_bios(self, mock_test_path_chroot):
        mock_test_path_chroot.side_effect = [False, True]

        result = self.morphing_tools._get_grub2_cfg_location()

        mock_test_path_chroot.assert_called_with('/boot/grub2/grub.cfg')
        self.assertEqual(result, '/boot/grub2/grub.cfg')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__get_grub2_cfg_location_not_found(self, mock_test_path_chroot):
        mock_test_path_chroot.return_value = False

        self.assertRaisesRegex(
            Exception,
            "could not determine grub location. boot partition not mounted?",
            self.morphing_tools._get_grub2_cfg_location
        )
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

    @mock.patch.object(suse.BaseSUSEMorphingTools, '_configure_cloud_init')
    @mock.patch.object(suse.BaseSUSEMorphingTools, '_run_dracut')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'post_packages_install')
    def test_post_packages_install(
            self, mock_post_packages_install, mock__run_dracut,
            mock__configure_cloud_init):

        self.morphing_tools.post_packages_install(self.package_names)

        mock__configure_cloud_init.assert_called_once()
        mock__run_dracut.assert_called_once()
        mock_post_packages_install.assert_called_once_with(self.package_names)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__enable_sles_module(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.return_value = "module1\nmodule2\nmodule3"

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
            "module output", None, None, Exception()]

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

    @mock.patch.object(suse.BaseSUSEMorphingTools, '_add_repo')
    def test_add_cloud_tools_repo_version_16(self, mock_add_repo):
        self.morphing_tools._version = "16"

        self.morphing_tools._add_cloud_tools_repo()

        expected_repo = suse.CLOUD_TOOLS_REPO_URI_VERSION_ONLY_FORMAT % "16"
        mock_add_repo.assert_called_once_with(expected_repo, 'Cloud-Tools')

    @mock.patch.object(suse.BaseSUSEMorphingTools, '_add_repo')
    def test_add_cloud_tools_repo_add_repo_failure(self, mock_add_repo):
        mock_add_repo.side_effect = Exception("connection error")

        with self.assertLogs(
                'coriolis.osmorphing.suse', level=logging.WARNING):
            self.morphing_tools._add_cloud_tools_repo()

        expected_repo = suse.CLOUD_TOOLS_REPO_URI_FORMAT % (
            'test_release', '_12')
        mock_add_repo.assert_called_once_with(expected_repo, 'Cloud-Tools')
        self.event_manager.progress_update.assert_called_once()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__get_repos(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.return_value = (
            "repo1 http://repo1.com\nrepo2 http://repo2.com")

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

    def test__get_sle_modules_default(self):
        result = self.morphing_tools._get_sle_modules()

        self.assertEqual(result, ["sle-module-public-cloud"])

    @mock.patch.object(suse.BaseSUSEMorphingTools, '_enable_sles_module')
    @mock.patch.object(suse.BaseSUSEMorphingTools, '_get_sle_modules')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'pre_packages_install')
    def test_pre_packages_install_sles_old_version(
            self, mock_super_pre, mock_get_sle_modules,
            mock_enable_sles_module):
        mock_get_sle_modules.return_value = ["mod1", "mod2"]

        self.morphing_tools.pre_packages_install(self.package_names)

        mock_super_pre.assert_called_once_with(self.package_names)
        mock_enable_sles_module.assert_has_calls([
            mock.call("mod1"),
            mock.call("mod2"),
        ])

    @mock.patch.object(suse.BaseSUSEMorphingTools, '_enable_sles_module')
    @mock.patch.object(suse.BaseSUSEMorphingTools, '_get_sle_modules')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'pre_packages_install')
    def test_pre_packages_install_sles_16(
            self, mock_super_pre, mock_get_sle_modules,
            mock_enable_sles_module):
        self.morphing_tools._version = "16"

        self.morphing_tools.pre_packages_install(self.package_names)

        mock_super_pre.assert_called_once_with(self.package_names)
        mock_get_sle_modules.assert_not_called()
        mock_enable_sles_module.assert_not_called()

    @mock.patch.object(suse.BaseSUSEMorphingTools, '_add_cloud_tools_repo')
    @mock.patch.object(suse.BaseSUSEMorphingTools, '_enable_sles_module')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'pre_packages_install')
    def test_pre_packages_install_opensuse(
            self, mock_super_pre, mock_enable_sles_module,
            mock_add_cloud_tools_repo):
        self.morphing_tools._distro = suse.OPENSUSE_DISTRO_IDENTIFIER

        self.morphing_tools.pre_packages_install(self.package_names)

        mock_super_pre.assert_called_once_with(self.package_names)
        mock_add_cloud_tools_repo.assert_called_once()
        mock_enable_sles_module.assert_not_called()

    @mock.patch.object(suse.BaseSUSEMorphingTools, '_add_cloud_tools_repo')
    @mock.patch.object(suse.BaseSUSEMorphingTools, '_enable_sles_module')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'pre_packages_install')
    def test_pre_packages_install_no_packages(
            self, mock_super_pre, mock_enable_sles_module,
            mock_add_cloud_tools_repo):
        self.morphing_tools.pre_packages_install([])

        mock_super_pre.assert_called_once_with([])
        mock_enable_sles_module.assert_not_called()
        mock_add_cloud_tools_repo.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_get_keyfiles_by_type')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test__get_existing_ethernet_nmconnection_files(
            self, mock_test_path, mock_get_keyfiles_by_type):
        mock_test_path.return_value = True
        mock_get_keyfiles_by_type.return_value = [
            ('etc/NetworkManager/system-connections/eth0.nmconnection', {}),
            ('etc/NetworkManager/system-connections/eth1.nmconnection', {})]

        result = (
            self.morphing_tools._get_existing_ethernet_nmconnection_files())

        self.assertEqual(result, [
            'etc/NetworkManager/system-connections/eth0.nmconnection',
            'etc/NetworkManager/system-connections/eth1.nmconnection'])
        mock_get_keyfiles_by_type.assert_called_once_with(
            "ethernet", "etc/NetworkManager/system-connections")

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_get_keyfiles_by_type')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test__get_existing_ethernet_nmconnection_files_no_path(
            self, mock_test_path, mock_get_keyfiles_by_type):
        mock_test_path.return_value = False

        result = (
            self.morphing_tools._get_existing_ethernet_nmconnection_files())

        self.assertEqual(result, [])
        mock_get_keyfiles_by_type.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file_sudo')
    @mock.patch.object(suse.BaseSUSEMorphingTools, '_schedule_grub2_update')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test_disable_predictable_nic_names(
            self, mock_test_path, mock_read_file_sudo,
            mock_schedule_grub2_update, mock_write_file_sudo):
        mock_test_path.return_value = True
        mock_read_file_sudo.return_value = (
            'GRUB_CMDLINE_LINUX_DEFAULT=""\nGRUB_CMDLINE_LINUX=""\n')

        self.morphing_tools.disable_predictable_nic_names()

        mock_read_file_sudo.assert_called_once_with("etc/default/grub")
        mock_write_file_sudo.assert_called_once()
        written_path, written_contents = mock_write_file_sudo.call_args[0]
        self.assertEqual("etc/default/grub", written_path)
        self.assertIn("net.ifnames=0", written_contents)
        self.assertIn("biosdevname=0", written_contents)
        # The (slow) grub regeneration must be deferred, not run eagerly.
        mock_schedule_grub2_update.assert_called_once_with()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test_disable_predictable_nic_names_no_grub_cfg(self, mock_test_path):
        mock_test_path.return_value = False

        with self.assertLogs(
                'coriolis.osmorphing.suse', level=logging.WARNING):
            self.morphing_tools.disable_predictable_nic_names()

    def test__ifcfg_class_attributes(self):
        self.assertEqual(
            "etc/sysconfig/network",
            self.morphing_tools._NETWORK_SCRIPTS_PATH)
        self.assertEqual(
            suse.SUSE_IFCFG_TEMPLATE, self.morphing_tools._IFCFG_TEMPLATE)
        self.assertNotIn("NM_CONTROLLED", suse.SUSE_IFCFG_TEMPLATE)
        self.assertIn("BOOTPROTO='dhcp'", suse.SUSE_IFCFG_TEMPLATE)
        self.assertIn("STARTMODE='auto'", suse.SUSE_IFCFG_TEMPLATE)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file_sudo')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_backup_ethernet_ifcfg_configs')
    def test__write_nic_configs(
            self, mock_backup_ethernet_ifcfg_configs, mock_write_file_sudo):
        nics_info = [{'name': 'eth0'}, {'name': 'eth1'}]

        self.morphing_tools._write_nic_configs(nics_info)

        mock_backup_ethernet_ifcfg_configs.assert_called_once_with()
        mock_write_file_sudo.assert_has_calls([
            mock.call(
                "etc/sysconfig/network/ifcfg-eth0", suse.SUSE_IFCFG_TEMPLATE),
            mock.call(
                "etc/sysconfig/network/ifcfg-eth1", suse.SUSE_IFCFG_TEMPLATE),
        ])

    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_backup_ethernet_ifcfg_configs')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_backup_nmconnection_files')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__write_nmconnection_configs(
            self, mock_exec_cmd_chroot, mock_write_file_sudo,
            mock_backup_nmconnection_files,
            mock_backup_ethernet_ifcfg_configs):
        nics_info = [{'name': 'eth0'}]
        nmconnection_files = [
            'etc/NetworkManager/system-connections/eth0.nmconnection']

        self.morphing_tools._write_nmconnection_configs(
            nics_info, nmconnection_files)

        mock_backup_nmconnection_files.assert_called_once_with(
            nmconnection_files)
        mock_backup_ethernet_ifcfg_configs.assert_called_once_with()
        mock_write_file_sudo.assert_called_once()
        args, _ = mock_write_file_sudo.call_args
        self.assertEqual(
            args[0],
            "etc/NetworkManager/system-connections/eth0.nmconnection")
        self.assertIn("[connection]", args[1])
        self.assertIn("interface-name=eth0", args[1])
        self.assertIn("method=auto", args[1])
        self.assertIn("may-fail=false", args[1])
        mock_exec_cmd_chroot.assert_called_once_with(
            "chmod 600 /etc/NetworkManager/system-connections/"
            "eth0.nmconnection")

    @mock.patch.object(
        suse.BaseSUSEMorphingTools, 'disable_predictable_nic_names')
    @mock.patch.object(suse.BaseSUSEMorphingTools, '_write_nic_configs')
    @mock.patch.object(
        suse.BaseSUSEMorphingTools, '_write_nmconnection_configs')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools,
        '_get_existing_ethernet_nmconnection_files')
    def test_set_net_config_dhcp(
            self, mock_get_existing_ethernet_nmconnection_files,
            mock_write_nmconnection_configs,
            mock_write_nic_configs,
            mock_disable_predictable_nic_names):
        mock_get_existing_ethernet_nmconnection_files.return_value = []
        nics_info = [{'name': 'eth0'}]

        self.morphing_tools.set_net_config(nics_info, dhcp=True)

        mock_get_existing_ethernet_nmconnection_files.assert_called_once_with()
        mock_write_nmconnection_configs.assert_not_called()
        mock_disable_predictable_nic_names.assert_called_once()
        mock_write_nic_configs.assert_called_once_with(nics_info)

    @mock.patch.object(
        suse.BaseSUSEMorphingTools, 'disable_predictable_nic_names')
    @mock.patch.object(suse.BaseSUSEMorphingTools, '_write_nic_configs')
    @mock.patch.object(
        suse.BaseSUSEMorphingTools, '_write_nmconnection_configs')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools,
        '_get_existing_ethernet_nmconnection_files')
    def test_set_net_config_dhcp_nmconnection(
            self, mock_get_existing_ethernet_nmconnection_files,
            mock_write_nmconnection_configs,
            mock_write_nic_configs,
            mock_disable_predictable_nic_names):
        nm_files = [
            'etc/NetworkManager/system-connections/eth0.nmconnection']
        mock_get_existing_ethernet_nmconnection_files.return_value = nm_files
        nics_info = [{'name': 'eth0'}]

        self.morphing_tools.set_net_config(nics_info, dhcp=True)

        mock_disable_predictable_nic_names.assert_called_once()
        mock_write_nmconnection_configs.assert_called_once_with(
            nics_info, nm_files)
        mock_write_nic_configs.assert_not_called()

    @mock.patch.object(
        suse.BaseSUSEMorphingTools, 'disable_predictable_nic_names')
    @mock.patch.object(suse.BaseSUSEMorphingTools, '_write_nic_configs')
    @mock.patch.object(
        suse.BaseSUSEMorphingTools, '_write_nmconnection_configs')
    @mock.patch.object(
        base.BaseLinuxOSMorphingTools,
        '_get_existing_ethernet_nmconnection_files')
    def test_set_net_config_dhcp_no_nics(
            self, mock_get_existing_ethernet_nmconnection_files,
            mock_write_nmconnection_configs,
            mock_write_nic_configs,
            mock_disable_predictable_nic_names):
        self.morphing_tools.set_net_config(None, dhcp=True)

        mock_get_existing_ethernet_nmconnection_files.assert_not_called()
        mock_disable_predictable_nic_names.assert_not_called()
        mock_write_nmconnection_configs.assert_not_called()
        mock_write_nic_configs.assert_not_called()

    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_setup_network_preservation')
    def test_set_net_config_static(self, mock_setup_network_preservation):
        nics_info = [{'name': 'eth0'}]

        self.morphing_tools.set_net_config(nics_info, dhcp=False)

        mock_setup_network_preservation.assert_called_once_with(nics_info)
