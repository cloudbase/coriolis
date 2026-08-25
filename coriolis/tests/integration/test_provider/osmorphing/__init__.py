# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing import base
from coriolis.tests.integration.test_provider.osmorphing import rocky, ubuntu

OS_MORPHERS: list[base.BaseLinuxOSMorphingTools] = [
    rocky.TestRockyLinuxOSMorphingTools,
    ubuntu.TestUbuntuOSMorphingTools,
]

LUKS_OS_MORPHERS: list[base.BaseLinuxOSMorphingTools] = [
    rocky.LUKSTestRockyLinuxOSMorphingTools,
    ubuntu.LUKSTestUbuntuOSMorphingTools,
]
