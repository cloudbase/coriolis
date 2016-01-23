import os
import re

from coriolis import constants
from coriolis.osmorphing import debian
from coriolis import utils


class UbuntuOSMorphingTools(debian.DebianOSMorphingTools):
    _packages = {
        (constants.HYPERVISOR_VMWARE, None): [("open-vm-tools", True)],
        # TODO: sudo agt-get install linux-tool-<kernel release>
        # linux-cloud-tools-<kernel release> -y
        (constants.HYPERVISOR_HYPERV, None): [("hv-kvp-daemon-init", True)],
        # TODO: add cloud-initramfs-growroot
        (None, constants.PLATFORM_OPENSTACK): [("cloud-init", True)],
    }

    @staticmethod
    def check_os(ssh, os_root_dir):
        lsb_release_path = os.path.join(os_root_dir, "etc/lsb-release")
        if utils.test_ssh_path(ssh, lsb_release_path):
            out = utils.exec_ssh_cmd(
                ssh, "cat %s" % lsb_release_path).decode()

            dist_id = re.findall('^DISTRIB_ID=(.*)$', out, re.MULTILINE)
            release = re.findall('^DISTRIB_RELEASE=(.*)$', out, re.MULTILINE)
            if 'Ubuntu' in dist_id:
                return (dist_id, release)
