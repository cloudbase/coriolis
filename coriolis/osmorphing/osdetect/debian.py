# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis.osmorphing.osdetect import base


DEBIAN_DISTRO_IDENTIFIER = "Debian"


class DebianOSDetectTools(base.BaseLinuxOSDetectTools):

    def detect_os(self):
        release = None
        base_info = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": DEBIAN_DISTRO_IDENTIFIER}
        lsb_release_path = "etc/lsb-release"
        debian_version_path = "etc/debian_version"
        if self._test_path(lsb_release_path):
            config = self._read_config_file("etc/lsb-release")
            dist_id = config.get('DISTRIB_ID')
            if dist_id == 'Debian':
                release = config.get('DISTRIB_RELEASE')
        elif self._test_path(debian_version_path):
            deb_release_info = self._read_file(
                debian_version_path).decode().splitlines()
            if deb_release_info:
                release = deb_release_info[0]

        if not release:
            return {}

        base_info['release_version'] = release
        base_info['friendly_release_name'] = (
            "Debian Linux %s" % release)

        return base_info
