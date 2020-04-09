# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing import base
from coriolis.osmorphing.osdetect import openwrt as openwrt_detect


OPENWRT_DISTRO_IDENTIFIER = openwrt_detect.OPENWRT_DISTRO_IDENTIFIER


class BaseOpenWRTMorphingTools(base.BaseLinuxOSMorphingTools):

    @classmethod
    def check_os_supported(cls, detected_os_info):
        if detected_os_info['distribution_name'] == (
                OPENWRT_DISTRO_IDENTIFIER):
            return True
        return False

    def disable_predictable_nic_names(self):
        pass

    def pre_packages_install(self, package_names):
        pass

    def post_packages_install(self, package_names):
        pass

    def pre_packages_uninstall(self, package_names):
        pass

    def post_packages_uninstall(self, package_names):
        pass

    def set_net_config(self, nics_info, dhcp):
        pass
