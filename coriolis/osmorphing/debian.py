# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import os

from coriolis import constants
from coriolis.osmorphing import base


class DebianMorphingTools(base.BaseLinuxOSMorphingTools):
    _packages = {
        (constants.HYPERVISOR_VMWARE, None): [("open-vm-tools", True)],
        # TODO: add cloud-initramfs-growroot
        (None, constants.PLATFORM_OPENSTACK): [("cloud-init", True)],
    }

    def _check_os(self):
        lsb_release_path = "etc/lsb-release"
        debian_version_path = "etc/debian_version"
        if self._test_path(lsb_release_path):
            config = self._read_config_file("etc/lsb-release")
            dist_id = config.get('DISTRIB_ID')
            if dist_id == 'Debian':
                release = config.get('DISTRIB_RELEASE')
                return (dist_id, release)
        elif self._test_path(debian_version_path):
            release = self._read_file(
                debian_version_path).decode().split('\n')[0]
            return ('Debian', release)

    def set_net_config(self, nics_info, dhcp):
        if dhcp:
            # NOTE: doesn't work with chroot
            interfaces_path = os.path.join(
                self._os_root_dir, "etc/network/interfaces")
            self._exec_cmd('sudo sed -i.bak "s/static/dhcp/g" %s' %
                           interfaces_path)

    def pre_packages_install(self, package_names):
        super(DebianMorphingTools, self).pre_packages_install(package_names)

        if package_names:
            self._event_manager.progress_update("Updating packages list")
            self._exec_cmd_chroot('apt-get clean')
            self._exec_cmd_chroot('apt-get update -y')

    def install_packages(self, package_names):
        apt_get_cmd = 'apt-get install %s -y' % " ".join(package_names)
        self._exec_cmd_chroot(apt_get_cmd)

    def uninstall_packages(self, package_names):
        for package_name in package_names:
            apt_get_cmd = 'apt-get remove %s -y || true' % package_name
            self._exec_cmd_chroot(apt_get_cmd)
