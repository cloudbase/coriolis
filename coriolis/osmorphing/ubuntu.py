import re

from coriolis import constants
from coriolis.osmorphing import debian


class UbuntuMorphingTools(debian.DebianMorphingTools):
    _packages = {
        (constants.HYPERVISOR_VMWARE, None): [("open-vm-tools", True)],
        # TODO: sudo agt-get install linux-tool-<kernel release>
        # linux-cloud-tools-<kernel release> -y
        (constants.HYPERVISOR_HYPERV, None): [("hv-kvp-daemon-init", True)],
        # TODO: add cloud-initramfs-growroot
        (None, constants.PLATFORM_OPENSTACK): [("cloud-init", True)],
    }

    def check_os(self):
        lsb_release_path = "etc/lsb-release"
        if self._test_path(lsb_release_path):
            out = self._read_file(lsb_release_path).decode()

            dist_id = re.findall('^DISTRIB_ID=(.*)$', out, re.MULTILINE)
            release = re.findall('^DISTRIB_RELEASE=(.*)$', out, re.MULTILINE)
            if 'Ubuntu' in dist_id:
                return (dist_id, release)
