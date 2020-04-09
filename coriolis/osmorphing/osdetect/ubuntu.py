# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis.osmorphing.osdetect import base


UBUNTU_DISTRO_IDENTIFIER = "Ubuntu"


class UbuntuOSDetectTools(base.BaseLinuxOSDetectTools):

    def detect_os(self):
        info = {}
        config = self._read_config_file("etc/lsb-release", check_exists=True)
        dist_id = config.get('DISTRIB_ID')
        if dist_id == 'Ubuntu':
            release = config.get('DISTRIB_RELEASE')
            info = {
                "os_type": constants.OS_TYPE_LINUX,
                "distribution_name": UBUNTU_DISTRO_IDENTIFIER,
                "release_version": release,
                "friendly_release_name": "Ubuntu %s" % release}
        return info
