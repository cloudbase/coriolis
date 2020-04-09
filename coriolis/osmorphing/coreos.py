# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing import base
from coriolis.osmorphing.osdetect import coreos as coreos_detect


class BaseCoreOSMorphingTools(base.BaseLinuxOSMorphingTools):

    @classmethod
    def check_os_supported(cls, detected_os_info):
        if detected_os_info['distribution_name'] == (
                coreos_detect.COREOS_DISTRO_IDENTIFIER):
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
