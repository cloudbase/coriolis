# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis.osmorphing.osdetect import base


AMAZON_DISTRO_IDENTIFIER = "amzn"
AMAZON_DISTRO_NAME = "Amazon Linux"


class AmazonLinuxOSDetectTools(base.BaseLinuxOSDetectTools):

    def detect_os(self):
        info = {}
        os_release = self._get_os_release()
        osid = os_release.get("ID")
        osname = os_release.get("NAME")
        if osid == AMAZON_DISTRO_IDENTIFIER or osname == AMAZON_DISTRO_NAME:
            version = os_release.get("VERSION")
            friendly_name = "%s %s" % (AMAZON_DISTRO_NAME, version)
            info = {
                "os_type": constants.OS_TYPE_LINUX,
                "distribution_name": AMAZON_DISTRO_NAME,
                "release_version": version,
                "friendly_release_name": friendly_name}
        return info
