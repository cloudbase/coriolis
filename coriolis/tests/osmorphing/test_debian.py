# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.
from unittest import mock

from coriolis import exception
from coriolis.osmorphing import base
from coriolis.osmorphing import debian
from coriolis.tests import test_base


class BaseDebianMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseDebianMorphingTools class."""

    def setUp(self):
        super(BaseDebianMorphingToolsTestCase, self).setUp()
        self.detected_os_info = {
            'os_type': 'linux',
            'distribution_name': debian.DEBIAN_DISTRO_IDENTIFIER,
            'release_version': '8',
            'friendly_release_name': mock.sentinel.friendly_release_name,
        }
        self.os_root_dir = '/root'
        self.nics_info = [{'name': 'eth0'}, {'name': 'eth1'}]
        self.package_names = ['package1', 'package2']
        self._event_manager = mock.MagicMock()

        self.morpher = debian.BaseDebianMorphingTools(
            mock.sentinel.conn, self.os_root_dir, mock.sentinel.os_root_dev,
            mock.sentinel.hypervisor, self._event_manager,
            self.detected_os_info, mock.sentinel.osmorphing_parameters,
            mock.sentinel.osmorphing_config)

    def test_check_os_supported(self):
        result = debian.BaseDebianMorphingTools.check_os_supported(
            self.detected_os_info)

        self.assertTrue(result)

    def test_check_os_not_supported(self):
        self.detected_os_info['distribution_name'] = 'unsupported'

        result = debian.BaseDebianMorphingTools.check_os_supported(
            self.detected_os_info)

        self.assertFalse(result)

    @mock.patch('coriolis.utils.Grub2ConfigEditor')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_test_path_chroot')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_write_file_sudo')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_read_file_sudo')
    def test_disable_predictable_nic_names(
            self, mock_read_file_sudo, mock_write_file_sudo,
            mock_exec_cmd_chroot, mock_test_path_chroot,
            mock_grub2_cfg_editor):
        mock_test_path_chroot.return_value = True

        self.morpher.disable_predictable_nic_names()

        mock_test_path_chroot.assert_called_once_with('etc/default/grub')
        mock_grub2_cfg_editor.assert_called_once_with(
            mock_read_file_sudo.return_value.decode.return_value)
        mock_grub2_cfg_editor.return_value.append_to_option.assert_has_calls(
            [
                mock.call(
                    "GRUB_CMDLINE_LINUX_DEFAULT",
                    {"opt_type": "key_val", "opt_key": "net.ifnames",
                     "opt_val": 0}),
                mock.call(
                    "GRUB_CMDLINE_LINUX_DEFAULT",
                    {"opt_type": "key_val", "opt_key": "biosdevname",
                     "opt_val": 0}),
                mock.call(
                    "GRUB_CMDLINE_LINUX",
                    {"opt_type": "key_val", "opt_key": "net.ifnames",
                     "opt_val": 0}),
                mock.call(
                    "GRUB_CMDLINE_LINUX",
                    {"opt_type": "key_val", "opt_key": "biosdevname",
                     "opt_val": 0})
            ]
        )
        mock_read_file_sudo.assert_called_once_with('etc/default/grub')
        mock_write_file_sudo.assert_called_once_with(
            "etc/default/grub", mock_grub2_cfg_editor.return_value.dump())
        mock_exec_cmd_chroot.assert_called_once_with("/usr/sbin/update-grub")

    @mock.patch('coriolis.utils.Grub2ConfigEditor')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_write_file_sudo')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_read_file_sudo')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_test_path_chroot')
    def test_disable_predictable_nic_names_no_test_path_chroot(
            self, mock_test_path_chroot, mock_read_file_sudo,
            mock_write_file_sudo, mock_exec_cmd_chroot, mock_grub2_cfg_editor):

        mock_test_path_chroot.return_value = False

        self.morpher.disable_predictable_nic_names()

        mock_read_file_sudo.assert_not_called()
        mock_write_file_sudo.assert_not_called()
        mock_exec_cmd_chroot.assert_not_called()
        mock_grub2_cfg_editor.assert_not_called()

    def test_get_update_grub2_command(self):
        result = self.morpher.get_update_grub2_command()
        self.assertEqual(result, "update-grub")

    def test__compose_interfaces_config(self):
        result = self.morpher._compose_interfaces_config(self.nics_info)

        expected_result = (
            "\n"
            "auto lo\n"
            "iface lo inet loopback\n"
            "\n\n\n"
            "auto eth0\n"
            "iface eth0 inet dhcp\n"
            "\n\n\n"
            "auto eth1\n"
            "iface eth1 inet dhcp\n"
            "\n\n"
        )

        self.assertEqual(result, expected_result)

    def test__compose_netplan_cfg(self):
        expected_cfg = {
            "network": {
                "version": 2,
                "ethernets": {
                    "lo": {
                        "match": {
                            "name": "lo"
                        },
                        "addresses": ["127.0.0.1/8"]
                    },
                    "eth0": {
                        "dhcp4": True,
                        "dhcp6": True,
                    },
                    "eth1": {
                        "dhcp4": True,
                        "dhcp6": True,
                    }
                }
            }
        }

        result = self.morpher._compose_netplan_cfg(self.nics_info)

        expected_result = debian.yaml.dump(
            expected_cfg, default_flow_style=False)

        self.assertEqual(result, expected_result)

    @mock.patch.object(debian.BaseDebianMorphingTools, '_write_file_sudo')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_test_path')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_list_dir')
    @mock.patch.object(
        debian.BaseDebianMorphingTools, 'disable_predictable_nic_names'
    )
    def test_set_net_config(
            self, mock_disable_predictable_nic_names, mock_list_dir,
            mock_test_path, mock_exec_cmd_chroot, mock_write_file_sudo):
        dhcp = True
        mock_test_path.return_value = True
        mock_list_dir.return_value = ['file1.yaml', 'file2.yml']

        self.morpher.set_net_config(self.nics_info, dhcp)

        netplan_cfg = self.morpher._compose_netplan_cfg(self.nics_info)
        interfaces_config = self.morpher._compose_interfaces_config(
            self.nics_info)

        mock_disable_predictable_nic_names.assert_called_once()
        mock_test_path.assert_has_calls([
            mock.call('etc/network'),
            mock.call('etc/network/interfaces'),
            mock.call('etc/netplan')])
        mock_list_dir.assert_called_once_with('etc/netplan')
        mock_write_file_sudo.assert_has_calls(
            [mock.call('etc/network/interfaces', interfaces_config),
             mock.call('etc/netplan/coriolis_netplan.yaml', netplan_cfg)])
        mock_exec_cmd_chroot.assert_has_calls(
            [mock.call('cp etc/network/interfaces etc/network/interfaces.bak'),
             mock.call('mv etc/netplan/file1.yaml etc/netplan/file1.yaml.bak'),
             mock.call('mv etc/netplan/file2.yml etc/netplan/file2.yml.bak')])

    @mock.patch.object(debian.BaseDebianMorphingTools, '_write_file_sudo')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_exec_cmd_chroot')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_test_path')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_list_dir')
    @mock.patch.object(
        debian.BaseDebianMorphingTools, 'disable_predictable_nic_names'
    )
    def test_set_net_config_no_dhcp(
            self, mock_disable_predictable_nic_names, mock_list_dir,
            mock_test_path, mock_exec_cmd_chroot, mock_write_file_sudo):
        dhcp = False
        mock_test_path.return_value = True

        self.morpher.set_net_config(self.nics_info, dhcp)

        mock_disable_predictable_nic_names.assert_not_called()
        mock_write_file_sudo.assert_not_called()

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_get_installed_packages(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.return_value = \
            "package1\npackage2".encode('utf-8')

        self.morpher.get_installed_packages()

        self.assertEqual(
            self.morpher.installed_packages,
            ['package1', 'package2']
        )
        mock_exec_cmd_chroot.assert_called_once_with(
            "dpkg-query -f '${binary:Package}\\n' -W")

    @mock.patch.object(base.BaseLinuxOSMorphingTools, '_exec_cmd_chroot')
    def test_get_installed_packages_none(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.side_effect = exception.CoriolisException()

        self.morpher.get_installed_packages()

        self.assertEqual(
            self.morpher.installed_packages,
            []
        )
        mock_exec_cmd_chroot.assert_called_once_with(
            "dpkg-query -f '${binary:Package}\\n' -W")

    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'pre_packages_install')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_exec_cmd_chroot')
    def test_pre_packages_install(self, mock_exec_cmd_chroot,
                                  mock_pre_packages_install):

        self.morpher.pre_packages_install(self.package_names)

        mock_pre_packages_install.assert_called_once_with(self.package_names)
        mock_exec_cmd_chroot.assert_has_calls([
            mock.call('apt-get clean'),
            mock.call('apt-get update -y')
        ])

    @mock.patch.object(base.BaseLinuxOSMorphingTools, 'pre_packages_install')
    @mock.patch.object(debian.BaseDebianMorphingTools, '_exec_cmd_chroot')
    def test_pre_packages_install_with_exception(self, mock_exec_cmd_chroot,
                                                 mock_pre_packages_install):
        mock_exec_cmd_chroot.side_effect = Exception()

        self.assertRaises(exception.PackageManagerOperationException,
                          self.morpher.pre_packages_install,
                          self.package_names)

        mock_pre_packages_install.assert_called_once_with(self.package_names)

    @mock.patch.object(debian.BaseDebianMorphingTools, '_exec_cmd_chroot')
    def test_install_packages(self, mock_exec_cmd_chroot):
        self.morpher.install_packages(self.package_names)

        apt_get_cmd = (
            '/bin/bash -c "DEBIAN_FRONTEND=noninteractive '
            'apt-get install %s -y '
            '-o Dpkg::Options::=\'--force-confdef\'"' % (
                " ".join(self.package_names)))
        deb_reconfigure_cmd = "dpkg --configure --force-confold -a"

        mock_exec_cmd_chroot.assert_has_calls([
            mock.call(deb_reconfigure_cmd),
            mock.call(apt_get_cmd)
        ])

    @mock.patch.object(debian.BaseDebianMorphingTools, '_exec_cmd_chroot')
    def test_install_packages_with_exception(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.side_effect = Exception()

        self.assertRaises(exception.FailedPackageInstallationException,
                          self.morpher.install_packages, self.package_names)

    @mock.patch.object(debian.BaseDebianMorphingTools, '_exec_cmd_chroot')
    def test_uninstall_packages(self, mock_exec_cmd_chroot):
        self.morpher.uninstall_packages(self.package_names)

        mock_exec_cmd_chroot.assert_has_calls([
            mock.call('apt-get remove %s -y || true' % self.package_names[0]),
            mock.call('apt-get remove %s -y || true' % self.package_names[1])
        ])

    @mock.patch.object(debian.BaseDebianMorphingTools, '_exec_cmd_chroot')
    def test_uninstall_packages_with_exception(self, mock_exec_cmd_chroot):
        mock_exec_cmd_chroot.side_effect = exception.CoriolisException()

        self.assertRaises(exception.FailedPackageUninstallationException,
                          self.morpher.uninstall_packages, self.package_names)
