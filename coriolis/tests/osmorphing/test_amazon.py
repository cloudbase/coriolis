# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.


from coriolis.osmorphing import amazon
from coriolis.tests import test_base


class BaseAmazonLinuxOSMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseAmazonLinuxOSMorphingTools class."""

    def test_check_os_supported(self):
        detected_os_info = {
            "distribution_name": amazon.AMAZON_DISTRO_NAME_IDENTIFIER,
            "release_version": "2"
        }

        result = amazon.BaseAmazonLinuxOSMorphingTools.check_os_supported(
            detected_os_info)

        self.assertTrue(result)

    def test_check_os_not_supported(self):
        detected_os_info = {
            "distribution_name": 'unsupported',
        }
        result = amazon.BaseAmazonLinuxOSMorphingTools.check_os_supported(
            detected_os_info)

        self.assertFalse(result)
