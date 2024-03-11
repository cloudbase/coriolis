# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.


from coriolis.osmorphing import rocky
from coriolis.tests import test_base


class BaseRockyLinuxMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseRockyLinuxMorphingTools class."""

    def test_check_os_supported(self):
        detected_os_info = {
            "distribution_name": rocky.ROCKY_LINUX_DISTRO_IDENTIFIER,
            "release_version": "8"
        }
        result = rocky.BaseRockyLinuxMorphingTools.check_os_supported(
            detected_os_info
        )

        self.assertTrue(result)

    def test_check_os_not_supported(self):
        detected_os_info = {
            "distribution_name": "unsupported",
        }
        result = rocky.BaseRockyLinuxMorphingTools.check_os_supported(
            detected_os_info
        )

        self.assertFalse(result)
