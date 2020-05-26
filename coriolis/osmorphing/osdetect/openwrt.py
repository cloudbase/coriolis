# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis.osmorphing.osdetect import base


OPENWRT_DISTRO_IDENTIFIER = "OpenWRT"


class OpenWRTOSDetectTools(base.BaseLinuxOSDetectTools):

    def detect_os(self):
        info = {}
        openwrt_release = self._read_config_file(
            "etc/openwrt_release", check_exists=True)
        distrib_id = openwrt_release.get("DISTRIB_ID")
        if distrib_id == "OpenWrt":
            name = openwrt_release.get("DISTRIB_DESCRIPTION", distrib_id)
            version = openwrt_release.get("DISTRIB_RELEASE")
            info = {
                "os_type": constants.OS_TYPE_LINUX,
                "distribution_name": OPENWRT_DISTRO_IDENTIFIER,
                "release_version": version,
                "friendly_release_name": "%s Version %s" % (
                    name, version)}
        return info
