# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.


from unittest import mock

from coriolis.osmorphing import amazon
from coriolis.osmorphing import base
from coriolis.tests import test_base


class BaseAmazonLinuxOSMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseAmazonLinuxOSMorphingTools class."""

    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_version_supported_util'
    )
    def test_check_os_supported(self, mock_version_supported_util):
        detected_os_info = {
            "distribution_name": amazon.AMAZON_DISTRO_NAME_IDENTIFIER,
            "release_version": mock.sentinel.release_version
        }

        result = amazon.BaseAmazonLinuxOSMorphingTools.check_os_supported(
            detected_os_info)

        mock_version_supported_util.assert_called_once_with(
            detected_os_info['release_version'], minimum=2)

        self.assertEqual(result, mock_version_supported_util.return_value)

    @mock.patch.object(
        base.BaseLinuxOSMorphingTools, '_version_supported_util'
    )
    def test_check_os_not_supported(self, mock_version_supported_util):
        detected_os_info = {
            "distribution_name": 'unsupported',
            "release_version": mock.sentinel.release_version
        }
        result = amazon.BaseAmazonLinuxOSMorphingTools.check_os_supported(
            detected_os_info)

        mock_version_supported_util.assert_not_called()

        self.assertFalse(result)
