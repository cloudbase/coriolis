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

    Extends the base test tools with dracut and cryptsetup, which provide
    the initramfs rebuild tool and LUKS support. The base Rocky Linux Docker
    image omits them; they must be installed so initramfs can be rebuilt.
    """

    _packages = {
        None: [
            ("jq", True),
            ("dracut", False),
            ("cryptsetup", False),
        ],
    }
