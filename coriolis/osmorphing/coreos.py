# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing import base


class BaseCoreOSMorphingTools(base.BaseLinuxOSMorphingTools):
    def _check_os(self):
        os_release = self._get_os_release()
        id = os_release.get("ID")
        if id == "coreos":
            name = os_release.get("NAME")
            version = os_release.get("VERSION_ID")
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
