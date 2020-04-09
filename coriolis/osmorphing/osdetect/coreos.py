# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis.osmorphing.osdetect import base


COREOS_DISTRO_IDENTIFIER = "CoreOS"


class CoreOSOSDetectTools(base.BaseLinuxOSDetectTools):

    def detect_os(self):
        info = {}
        os_release = self._get_os_release()
        osid = os_release.get("ID")
        if osid == "coreos":
            name = os_release.get("NAME")
            version = os_release.get("VERSION_ID")
            info = {
                "os_type": constants.OS_TYPE_LINUX,
                "distribution_name": COREOS_DISTRO_IDENTIFIER,
                "release_version": version,
                "friendly_release_name": "CoreOS Linux %s" % version}
        return info
