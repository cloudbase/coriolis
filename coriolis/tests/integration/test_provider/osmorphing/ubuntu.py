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
