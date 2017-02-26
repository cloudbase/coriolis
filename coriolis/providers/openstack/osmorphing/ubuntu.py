# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis.osmorphing import ubuntu as base_ubuntu


class UbuntuMorphingTools(base_ubuntu.BaseUbuntuMorphingTools):
    _packages = {
        constants.HYPERVISOR_VMWARE: [("open-vm-tools", True)],
        # TODO: sudo agt-get install linux-tool-<kernel release>
        # linux-cloud-tools-<kernel release> -y
        constants.HYPERVISOR_HYPERV: [("hv-kvp-daemon-init", True)],
        # TODO: add cloud-initramfs-growroot
        None: [("cloud-init", True)],
    }
