# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

import re

from oslo_log import log as logging
from coriolis import constants
from coriolis.osmorphing.osdetect import base


LOG = logging.getLogger(__name__)
ROCKY_LINUX_DISTRO_IDENTIFIER = "Rocky Linux"


class RockyLinuxOSDetectTools(base.BaseLinuxOSDetectTools):

    def detect_os(self):
        info = {}
        redhat_release_path = "etc/redhat-release"
        if self._test_path(redhat_release_path):
            release_info = self._read_file(
                redhat_release_path).decode().splitlines()
            if release_info:
                m = re.match(r"^(.*) release ([0-9](\.[0-9])*)( \(.*\))?.*$",
                             release_info[0].strip())
                if m:
                    distro, version, _, _ = m.groups()
                    if ROCKY_LINUX_DISTRO_IDENTIFIER not in distro:
                        LOG.debug(
                            "Distro does not appear to be a Rocky Linux: %s",
                            distro)
                        return {}

                    info = {
                        "os_type": constants.OS_TYPE_LINUX,
                        "distribution_name": ROCKY_LINUX_DISTRO_IDENTIFIER,
                        "release_version": version,
                        "friendly_release_name": "%s Version %s" % (
                            ROCKY_LINUX_DISTRO_IDENTIFIER, version)}
        return info
