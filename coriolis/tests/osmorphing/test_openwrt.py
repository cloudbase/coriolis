# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing import openwrt
from coriolis.tests import test_base


class MockOpenWRTMorphingTools(openwrt.BaseOpenWRTMorphingTools):
    def __init__(self):
        pass

    def get_installed_packages(self):
        pass

    def install_packages(self, packages):
        pass

    def uninstall_packages(self, packages):
        pass


class BaseOpenWRTMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test case for the BaseOpenWRTMorphingTools class."""

    def test_check_os_supported(self):
        detected_os_info = {
            'distribution_name': openwrt.OPENWRT_DISTRO_IDENTIFIER
        }
        result = openwrt.BaseOpenWRTMorphingTools.check_os_supported(
            detected_os_info)

        self.assertTrue(result)

    def test_check_os_not_supported(self):
        detected_os_info = {
            'distribution_name': 'unsupported'
        }
        result = openwrt.BaseOpenWRTMorphingTools.check_os_supported(
            detected_os_info)

        self.assertFalse(result)

    def test_get_update_grub2_command(self):
        morphing_tools = MockOpenWRTMorphingTools()
        self.assertRaises(NotImplementedError,
                          morphing_tools.get_update_grub2_command)
