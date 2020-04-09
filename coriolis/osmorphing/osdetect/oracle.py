# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import re

from coriolis import constants
from coriolis.osmorphing.osdetect import base


ORACLE_DISTRO_IDENTIFIER = "Oracle Linux"


class OracleOSDetectTools(base.BaseLinuxOSDetectTools):

    def detect_os(self):
        info = {}
        oracle_release_path = "etc/oracle-release"
        if self._test_path(oracle_release_path):
            release_info = self._read_file(
                oracle_release_path).decode().splitlines()
            if release_info:
                m = re.match(r"^(.*) release ([0-9].*)$",
                             release_info[0].strip())
                if m:
                    distro, version = m.groups()
                    info = {
                        "os_type": constants.OS_TYPE_LINUX,
                        "distribution_name": ORACLE_DISTRO_IDENTIFIER,
                        "release_version": version,
                        "friendly_release_name": "%s Version %s" % (
                            distro, version)}
        return info
