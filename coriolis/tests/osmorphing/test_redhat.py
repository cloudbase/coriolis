# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

import ddt

from coriolis import exception
from coriolis.osmorphing import base
from coriolis.osmorphing import redhat
from coriolis.tests import test_base


@ddt.ddt
class BaseRedHatMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseRedHatMorphingTools class."""

    def setUp(self):
        super(BaseRedHatMorphingToolsTestCase, self).setUp()
        self.detected_os_info = {
            'os_type': 'linux',
            'distribution_name': redhat.RED_HAT_DISTRO_IDENTIFIER,
            'release_version': '6',
            'friendly_release_name': mock.sentinel.friendly_release_name,
        }
        self.package_names = ['package1', 'package2']
        self.enable_repos = ['repo1', 'repo2']
        self.event_manager = mock.MagicMock()
        self.morphing_tools = redhat.BaseRedHatMorphingTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.os_root_dir, mock.sentinel.hypervisor,
            self.event_manager, self.detected_os_info,
            mock.sentinel.osmorphing_parameters,
            mock.sentinel.operation_timeout)
        self.morphing_tools._os_root_dir = '/root'

    def test_check_os_supported(self):
        result = redhat.BaseRedHatMorphingTools.check_os_supported(
            self.detected_os_info)

        self.assertTrue(result)

    def test_check_os_not_supported(self):
        self.detected_os_info['distribution_name'] = 'unsupported'

        result = redhat.BaseRedHatMorphingTools.check_os_supported(
            self.detected_os_info)

        self.assertFalse(result)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_disable_predictable_nic_names(self, mock_exec_cmd_chroot):
        self.morphing_tools.disable_predictable_nic_names()
        mock_exec_cmd_chroot.assert_called_once_with(
            'grubby --update-kernel=ALL --args="net.ifnames=0 biosdevname=0"')

    @mock.patch.object(
        redhat.BaseRedHatMorphingTools, '_get_grub2_cfg_location'
    )
    def test_get_update_grub2_command(self, mock_get_grub2_cfg_location):
        result = self.morphing_tools.get_update_grub2_command()

        mock_get_grub2_cfg_location.assert_called_once()

        self.assertEqual(
            result,
            'grub2-mkconfig -o %s' % mock_get_grub2_cfg_location.return_value
        )

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__get_grub2_cfg_location_uefi(self, mock_test_path_chroot,
                                          mock_exec_cmd_chroot):
        mock_test_path_chroot.return_value = True

        result = self.morphing_tools._get_grub2_cfg_location()

        self.assertEqual(result, '/boot/efi/EFI/redhat/grub.cfg')
        mock_exec_cmd_chroot.assert_has_calls([
            mock.call("mount /boot || true"),
            mock.call("mount /boot/efi || true")
        ])
        mock_test_path_chroot.assert_called_once_with(
            '/boot/efi/EFI/redhat/grub.cfg')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__get_grub2_cfg_location_bios(self, mock_test_path_chroot,
                                          mock_exec_cmd_chroot):
        mock_test_path_chroot.side_effect = [False, True]

        result = self.morphing_tools._get_grub2_cfg_location()

        mock_test_path_chroot.assert_called_with('/boot/grub2/grub.cfg')
        mock_exec_cmd_chroot.assert_has_calls([
            mock.call("mount /boot || true"),
            mock.call("mount /boot/efi || true")
        ])

        self.assertEqual(result, '/boot/grub2/grub.cfg')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path_chroot')
    def test__get_grub2_cfg_location_unknown(self, mock_test_path_chroot,
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
            mock.call('/boot/efi/EFI/redhat/grub.cfg'),
            mock.call('/boot/grub2/grub.cfg')
        ])

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

    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_write_config_file')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_config_file')
    def test__set_dhcp_net_config(self, mock_read_config_file,
                                  mock_write_config_file):
        ifcfgs_ethernet = [
            (mock.sentinel.ifcfg_file,
             {
                 "BOOTPROTO": "none",
                 "IPADDR": mock.sentinel.ip_address,
                 "GATEWAY": mock.sentinel.gateway,
                 "NETMASK": mock.sentinel.netmask,
                 "NETWORK": mock.sentinel.network,
             }),
        ]
        mock_read_config_file.return_value = {
            "GATEWAY": mock.sentinel.gateway,
        }

        self.morphing_tools._set_dhcp_net_config(ifcfgs_ethernet)

        mock_read_config_file.assert_called_once_with(
            "etc/sysconfig/network", check_exists=True)
        mock_write_config_file.assert_has_calls([
            mock.call(mock.sentinel.ifcfg_file,
                      {"BOOTPROTO": "dhcp", "UUID": mock.ANY}),
            mock.call("etc/sysconfig/network",
                      mock_read_config_file.return_value)
        ])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file_sudo')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test_write_nic_configs(
            self, mock_test_path, mock_exec_cmd_chroot, mock_write_file_sudo):
        nics_info = [{'name': 'eth0'}, {'name': 'eth1'}]
        mock_test_path.return_value = True

        self.morphing_tools._write_nic_configs(nics_info)

        mock_write_file_sudo.assert_has_calls([
            mock.call(
                "etc/sysconfig/network-scripts/ifcfg-eth0",
                redhat.IFCFG_TEMPLATE % {"device_name": "eth0"},
            ),
            mock.call(
                "etc/sysconfig/network-scripts/ifcfg-eth1",
                redhat.IFCFG_TEMPLATE % {"device_name": "eth1"},
            )
        ])
        mock_exec_cmd_chroot.assert_has_calls([
            mock.call("cp etc/sysconfig/network-scripts/ifcfg-eth0 "
                      "etc/sysconfig/network-scripts/ifcfg-eth0.bak"),
            mock.call("cp etc/sysconfig/network-scripts/ifcfg-eth1 "
                      "etc/sysconfig/network-scripts/ifcfg-eth1.bak")
        ])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd')
    @mock.patch.object(redhat.utils, 'list_ssh_dir')
    def test__comment_keys_from_ifcfg_files(
            self, mock_list_ssh_dir, mock_exec_cmd):
        keys = ['KEY1', 'KEY2']
        interfaces = ['eth0', 'eth1']
        mock_list_ssh_dir.return_value = [
            'ifcfg-eth0', 'ifcfg-eth1', 'unknown-file', 'ifcfg-eth2']

        self.morphing_tools._comment_keys_from_ifcfg_files(
            keys, interfaces=interfaces)

        mock_list_ssh_dir.assert_called_once_with(
            mock.sentinel.conn, '/root/etc/sysconfig/network-scripts')
        mock_exec_cmd.assert_has_calls([
            mock.call(
                "sudo sed -i.bak -E -e 's/^(KEY1=.*)$/# \\1/g' "
                "/root/etc/sysconfig/network-scripts/ifcfg-eth0"
            ),
            mock.call(
                "sudo sed -i.bak -E -e 's/^(KEY2=.*)$/# \\1/g' "
                "/root/etc/sysconfig/network-scripts/ifcfg-eth0"
            ),
            mock.call(
                "sudo sed -i.bak -E -e 's/^(KEY1=.*)$/# \\1/g' "
                "/root/etc/sysconfig/network-scripts/ifcfg-eth1"
            ),
            mock.call(
                "sudo sed -i.bak -E -e 's/^(KEY2=.*)$/# \\1/g' "
                "/root/etc/sysconfig/network-scripts/ifcfg-eth1"
            ),
        ])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd')
    @mock.patch.object(redhat.utils, 'list_ssh_dir')
    def test__comment_keys_from_ifcfg_files_no_interfaces(
            self, mock_list_ssh_dir, mock_exec_cmd):
        keys = ['KEY1', 'KEY2']
        mock_list_ssh_dir.return_value = ['ifcfg-eth0', 'ifcfg-eth1']

        self.morphing_tools._comment_keys_from_ifcfg_files(keys)

        mock_list_ssh_dir.assert_called_once_with(
            mock.sentinel.conn, '/root/etc/sysconfig/network-scripts')

        mock_exec_cmd.assert_has_calls([
            mock.call(
                "sudo sed -i.bak -E -e 's/^(KEY1=.*)$/# \\1/g' "
                "/root/etc/sysconfig/network-scripts/ifcfg-eth0"
            ),
            mock.call(
                "sudo sed -i.bak -E -e 's/^(KEY2=.*)$/# \\1/g' "
                "/root/etc/sysconfig/network-scripts/ifcfg-eth0"
            ),
            mock.call(
                "sudo sed -i.bak -E -e 's/^(KEY1=.*)$/# \\1/g' "
                "/root/etc/sysconfig/network-scripts/ifcfg-eth1"
            ),
            mock.call(
                "sudo sed -i.bak -E -e 's/^(KEY2=.*)$/# \\1/g' "
                "/root/etc/sysconfig/network-scripts/ifcfg-eth1"
            ),
        ])

    @mock.patch.object(
        redhat.BaseRedHatMorphingTools, 'disable_predictable_nic_names'
    )
    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_write_nic_configs')
    def test_set_net_config_dhcp(self, mock_write_nic_configs,
                                 mock_disable_predictable_nic_names):
        nics_info = [{
            'mac_address': mock.sentinel.mac_address,
        }]
        dhcp = True

        self.morphing_tools.set_net_config(nics_info, dhcp)

        mock_disable_predictable_nic_names.assert_called_once()
        mock_write_nic_configs.assert_called_once_with(nics_info)

    @mock.patch.object(
        redhat.BaseRedHatMorphingTools, 'disable_predictable_nic_names'
    )
    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_write_nic_configs')
    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_add_net_udev_rules')
    @mock.patch.object(base.BaseLinuxOSMorphingTools,
                       '_setup_network_preservation')
    def test_set_net_config_no_dhcp(
            self, mock_setup_network_preservation, mock_add_net_udev_rules,
            mock_write_nic_configs,
            mock_disable_predictable_nic_names):
        nics_info = [{
            'mac_address': mock.sentinel.mac_address,
        }]
        dhcp = False

        self.morphing_tools.set_net_config(nics_info, dhcp)

        mock_disable_predictable_nic_names.assert_not_called()
        mock_write_nic_configs.assert_not_called()
        mock_setup_network_preservation.assert_called_once()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_get_installed_packages(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.return_value = \
            "package1\npackage2".encode('utf-8')

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
            'coriolis.osmorphing.redhat', level=logging.DEBUG):
            self.morphing_tools.get_installed_packages()

        self.assertEqual(
            self.morphing_tools.installed_packages,
            []
        )
        mock_exec_cmd_chroot.assert_called_once_with(
            'rpm -qa --qf "%{NAME}\\n"')

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__yum_install(self, mock_exec_cmd_chroot):
        self.morphing_tools._yum_install(self.package_names, self.enable_repos)

        mock_exec_cmd_chroot.assert_called_once_with(
            "yum install package1 package2 -y "
            "--enablerepo=repo1 --enablerepo=repo2"
        )

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__yum_install_with_exception(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.side_effect = exception.CoriolisException()

        self.assertRaises(
            exception.FailedPackageInstallationException,
            self.morphing_tools._yum_install,
            self.package_names, self.enable_repos
        )

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__yum_uninstall(self, mock_exec_cmd_chroot):
        self.morphing_tools._yum_uninstall(self.package_names)

        mock_exec_cmd_chroot.assert_has_calls([
            mock.call("yum remove package1 -y"),
            mock.call("yum remove package2 -y")
        ])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__yum_uninstall_with_exception(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.side_effect = exception.CoriolisException()

        self.assertRaises(
            exception.FailedPackageUninstallationException,
            self.morphing_tools._yum_uninstall, self.package_names
        )

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test__yum_clean_all(self, mock_test_path, mock_exec_cmd_chroot):
        mock_test_path.return_value = True

        self.morphing_tools._yum_clean_all()

        mock_test_path.assert_called_once_with("var/cache/yum")
        mock_exec_cmd_chroot.assert_has_calls([
            mock.call("yum clean all"),
            mock.call("rm -rf /var/cache/yum")
        ])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_test_path')
    def test__yum_clean_all_path_not_exists(self, mock_test_path,
                                            mock_exec_cmd_chroot):
        mock_test_path.return_value = False

        self.morphing_tools._yum_clean_all()

        mock_exec_cmd_chroot.assert_called_once_with("yum clean all")

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_list_dir')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file_sudo')
    def test__find_yum_repos_found(self, mock_read_file_sudo, mock_list_dir):
        mock_list_dir.return_value = ['file1.repo', 'file2.repo']
        mock_read_file_sudo.return_value = b'[repo1]\n[repo2]'
        repos_to_enable = ['repo1']

        result = self.morphing_tools._find_yum_repos(repos_to_enable)

        mock_read_file_sudo.assert_has_calls([
            mock.call('etc/yum.repos.d/file1.repo'),
            mock.call('etc/yum.repos.d/file2.repo')
        ])

        self.assertEqual(result, ['repo1'])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_list_dir')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_file_sudo')
    def test__find_yum_repos_not_found(self, mock_read_file_sudo,
                                       mock_list_dir):
        mock_list_dir.return_value = ['file1.repo', 'file2.repo']
        mock_read_file_sudo.return_value = b'[repo1]\n[repo2]'
        repos_to_enable = ['repo3']

        with self.assertLogs('coriolis.osmorphing.redhat', level=logging.WARN):
            self.morphing_tools._find_yum_repos(repos_to_enable)

    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_yum_install')
    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_yum_clean_all')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'pre_packages_install')
    def test_pre_packages_install(self, mock_pre_packages_install,
                                  mock_yum_clean_all, mock_yum_install):
        self.morphing_tools.installed_packages = []

        self.morphing_tools.pre_packages_install(self.package_names)

        mock_pre_packages_install.assert_called_once_with(self.package_names)
        mock_yum_clean_all.assert_called_once()
        mock_yum_install.assert_called_once_with(['grubby'])

    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_yum_install')
    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_yum_clean_all')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'pre_packages_install')
    def test_pre_packages_install_has_grubby(
        self, mock_pre_packages_install, mock_yum_clean_all, mock_yum_install):
        self.morphing_tools.installed_packages = ['grubby']

        self.morphing_tools.pre_packages_install(self.package_names)

        mock_pre_packages_install.assert_called_once_with(self.package_names)
        mock_yum_clean_all.assert_called_once()
        mock_yum_install.assert_not_called()

    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_yum_install')
    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_get_repos_to_enable')
    def test_install_packages(self, mock_get_repos_to_enable,
                              mock_yum_install):
        self.morphing_tools.install_packages(self.package_names)

        mock_get_repos_to_enable.assert_called_once()
        mock_yum_install.assert_called_once_with(
            self.package_names, mock_get_repos_to_enable.return_value)

    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_yum_uninstall')
    def test_uninstall_packages(self, mock_yum_uninstall):
        self.morphing_tools.uninstall_packages(self.package_names)

        mock_yum_uninstall.assert_called_once_with(self.package_names)

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test__run_dracut(self, mock_exec_cmd_chroot):
        self.morphing_tools._run_dracut()

        mock_exec_cmd_chroot.assert_called_once_with(
            "dracut --regenerate-all -f")

    @mock.patch.object(redhat.BaseRedHatMorphingTools, '_write_config_file')
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_read_config_file')
    def test__set_network_nozeroconf_config(
            self, mock_read_config_file, mock_write_config_file):
        self.morphing_tools._set_network_nozeroconf_config()

        mock_read_config_file.assert_called_once_with(
            "etc/sysconfig/network", check_exists=True)
        mock_write_config_file.assert_called_once_with(
            "etc/sysconfig/network", mock_read_config_file.return_value)

    @mock.patch.object(
        redhat.BaseRedHatMorphingTools, '_get_config_file_content'
    )
    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_write_file_sudo')
    def test__write_config_file(self, mock_write_file_sudo,
                                mock_get_config_file_content):
        self.morphing_tools._write_config_file(
            mock.sentinel.chroot_path, mock.sentinel.config_data)

        mock_get_config_file_content.assert_called_once_with(
            mock.sentinel.config_data)
        mock_write_file_sudo.assert_called_once_with(
            mock.sentinel.chroot_path,
            mock_get_config_file_content.return_value
        )

    def test__get_config_file_content(self):
        config = {
            'key1': 'value1',
            'key2': 'value2'
        }
        result = self.morphing_tools._get_config_file_content(config)

        self.assertEqual(result, 'key1="value1"\nkey2="value2"\n')
