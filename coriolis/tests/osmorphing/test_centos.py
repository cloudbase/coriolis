# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.


from coriolis.osmorphing import centos
from coriolis.tests import test_base


class BaseCentOSMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseCentOSMorphingTools class."""

    def test_check_os_supported(self):
        detected_os_info = {
            "distribution_name": centos.CENTOS_DISTRO_IDENTIFIER,
            "release_version": "6"
        }

        result = centos.BaseCentOSMorphingTools.check_os_supported(
            detected_os_info)

        self.assertTrue(result)

    def test_check_os_not_supported(self):
        detected_os_info = {
            "distribution_name": 'unsupported',
        }
        result = centos.BaseCentOSMorphingTools.check_os_supported(
            detected_os_info)

        self.assertFalse(result)
