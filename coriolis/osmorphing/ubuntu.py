# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

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

    def _check_os(self):
        config = self._read_config_file("etc/lsb-release", check_exists=True)
        dist_id = config.get('DISTRIB_ID')
        if dist_id == 'Ubuntu':
            release = config.get('DISTRIB_RELEASE')
            return (dist_id, release)
