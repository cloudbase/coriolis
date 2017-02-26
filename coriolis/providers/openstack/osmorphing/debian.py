# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis.osmorphing import debian as base_debian


class DebianMorphingTools(base_debian.BaseDebianMorphingTools):
    _packages = {
        constants.HYPERVISOR_VMWARE: [("open-vm-tools", True)],
        # TODO: add cloud-initramfs-growroot
        None: [("cloud-init", True)],
    }
