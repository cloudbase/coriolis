# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Ubuntu OS Morphing tools.
"""

from coriolis.osmorphing import ubuntu


class TestUbuntuOSMorphingTools(ubuntu.BaseUbuntuMorphingTools):
    """Ubuntu OSMorphing tools for integration tests."""

    # Package meant to be installed during OS morphing.
    # jq is a very small package which is not available by default.
    _packages = {
        None: [("jq", True)],
    }


class LUKSTestUbuntuOSMorphingTools(TestUbuntuOSMorphingTools):
    """Ubuntu morphing tools for LUKS integration tests.

    Extends the base test tools with initramfs-tools and cryptsetup-initramfs,
    which provide update-initramfs and the LUKS hook. The base Ubuntu Docker
    image omits them; they must be installed so initramfs can be rebuilt.
    """

    _packages = {
        None: [
            ("jq", True),
            ("initramfs-tools", False),
            ("cryptsetup-initramfs", False),
        ],
    }
