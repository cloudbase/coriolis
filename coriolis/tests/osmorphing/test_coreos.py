# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing import coreos
from coriolis.osmorphing.osdetect import coreos as coreos_detect
from coriolis.tests import test_base


class BaseCoreOSMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseCoreOSMorphingTools class."""

    def test_check_os_supported(self):
        detected_os_info = {
            "distribution_name": coreos_detect.COREOS_DISTRO_IDENTIFIER
        }
        result = coreos.BaseCoreOSMorphingTools.check_os_supported(
            detected_os_info)
        self.assertTrue(result)

    def test_check_os_not_supported(self):
        detected_os_info = {
            "distribution_name": "unsupported"
        }
        result = coreos.BaseCoreOSMorphingTools.check_os_supported(
            detected_os_info)
        self.assertFalse(result)
