# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Rocky Linux OS Morphing tools.
"""

from coriolis.osmorphing import rocky


class TestRockyLinuxOSMorphingTools(rocky.BaseRockyLinuxMorphingTools):
    """Rocky Linux OSMorphing tools for integration tests."""

    # Package meant to be installed during OS morphing.
    # jq is a small package not present in the base container image.
    _packages = {
        None: [("jq", True)],
    }


class LUKSTestRockyLinuxOSMorphingTools(TestRockyLinuxOSMorphingTools):
    """Rocky Linux morphing tools for LUKS integration tests.

    Extends the base test tools with dracut, cryptsetup, and kernel-core.
    The base Rocky Linux Docker image omits them; kernel-core provides the
    lib/modules/<kver>/ tree that dracut needs to rebuild the initramfs for
    LUKS auto-unlock.

    linux-firmware is a large (~150 MB) weak dependency of kernel-core that
    is not needed for initramfs rebuilds and would overflow the test device
    during download. install_weak_deps is therefore disabled in the chroot's
    dnf.conf before any packages are installed.
    """

    _packages = {
        None: [
            ("jq", True),
            ("dracut", False),
            ("cryptsetup", False),
            ("device-mapper", False),
            ("kernel-core", False),
        ],
    }

    def pre_packages_install(self, package_names):
        # Disable weak deps so kernel-core does not pull in linux-firmware.
        self._exec_cmd_chroot(
            "bash -c 'mkdir -p /etc/dnf && "
            "echo install_weak_deps=False >> /etc/dnf/dnf.conf'"
        )
        super().pre_packages_install(package_names)
