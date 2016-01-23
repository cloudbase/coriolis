import itertools
import os
import re

from coriolis import constants
from coriolis import utils


class UbuntuOSMorphingTools(object):
    _packages = {
        (constants.HYPERVISOR_VMWARE, None): [("open-vm-tools", True)],
        # TODO: sudo agt-get install linux-tool-<kernel release>
        # linux-cloud-tools-<kernel release> -y
        (constants.HYPERVISOR_HYPERV, None): [("hv-kvp-daemon-init", True)],
        (None, constants.PLATFORM_OPENSTACK): [("cloud-init", True)],
    }

    def __init__(self, os_root_dir):
        self._os_root_dir = os_root_dir

    @staticmethod
    def check_os(ssh, os_root_dir):
        lsb_release_path = os.path.join(os_root_dir, "etc/lsb-release")
        out = utils.exec_ssh_cmd(
            ssh, "cat %s || true" % lsb_release_path).decode()

        dist_id = re.findall('^DISTRIB_ID=(.*)$', out, re.MULTILINE)
        release = re.findall('^DISTRIB_RELEASE=(.*)$', out, re.MULTILINE)
        # TODO: validate release as well
        if 'Ubuntu' in dist_id:
            return True

    def set_dhcp(self, ssh):
        interfaces_path = os.path.join(
            self._os_root_dir, "etc/network/interfaces")
        utils.exec_ssh_cmd(
            ssh, 'sudo sed -i.bak "s/static/dhcp/g" %s' % interfaces_path)

    def get_packages(self, hypervisor, platform):
        k_add = [(h, p) for (h, p) in self._packages.keys() if
                 (h is None or h == hypervisor) and
                 (p is None or p == platform)]

        add = [p[0] for p in itertools.chain.from_iterable(
               [l for k, l in self._packages.items() if k in k_add])]

        k_remove = set(self._packages.keys()) - set(k_add)
        remove = [p[0] for p in itertools.chain.from_iterable(
                  [l for k, l in self._packages.items() if k in k_remove])
                  if p[1]]

        return add, remove

    def update_packages_list(self, ssh):
        apt_get_cmd = 'sudo apt-get update -y'
        utils.exec_ssh_cmd_chroot(ssh, self._os_root_dir, apt_get_cmd)

    def install_packages(self, ssh, package_names):
        apt_get_cmd = 'sudo apt-get install %s -y' % " ".join(package_names)
        utils.exec_ssh_cmd_chroot(ssh, self._os_root_dir, apt_get_cmd)

    def uninstall_packages(self, ssh, package_names):
        for package_name in package_names:
            apt_get_cmd = 'sudo apt-get remove %s -y || true' % package_name
            utils.exec_ssh_cmd_chroot(ssh, self._os_root_dir, apt_get_cmd)
