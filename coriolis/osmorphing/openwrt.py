# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing import base


class BaseOpenWRTMorphingTools(base.BaseLinuxOSMorphingTools):
    def _check_os(self):
        openwrt_release = self._read_config_file(
            "etc/openwrt_release", check_exists=True)
        distrib_id = openwrt_release.get("DISTRIB_ID")
        if distrib_id == "OpenWrt":
            name = openwrt_release.get("DISTRIB_DESCRIPTION", distrib_id)
            version = openwrt_release.get("DISTRIB_RELEASE")
            return (name, version)

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
