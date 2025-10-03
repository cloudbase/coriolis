# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import re

from coriolis import constants
from coriolis.osmorphing.osdetect import base
from oslo_log import log as logging


LOG = logging.getLogger(__name__)
CENTOS_DISTRO_IDENTIFIER = "CentOS"
CENTOS_STREAM_DISTRO_IDENTIFIER = "CentOS Stream"


class CentOSOSDetectTools(base.BaseLinuxOSDetectTools):

    def detect_os(self):
        info = {}
        redhat_release_path = "etc/redhat-release"
        if self._test_path(redhat_release_path):
            release_info = self._read_file(
                redhat_release_path).decode().splitlines()
            if release_info:
                m = re.match(
                    r"^(.*) release ([0-9]+(?:\.[0-9]+)*)( \(.*\))?.*$",
                    release_info[0].strip())
                if m:
                    distro, version, _ = m.groups()
                    if CENTOS_DISTRO_IDENTIFIER not in distro:
                        LOG.debug(
                            "Distro does not appear to be a CentOS: %s",
                            distro)
                        return {}

                    distribution_name = CENTOS_DISTRO_IDENTIFIER
                    if CENTOS_STREAM_DISTRO_IDENTIFIER in distro:
                        distribution_name = CENTOS_STREAM_DISTRO_IDENTIFIER
                    info = {
                        "os_type": constants.OS_TYPE_LINUX,
                        "distribution_name": distribution_name,
                        "release_version": version,
                        "friendly_release_name": "%s Version %s" % (
                            distribution_name, version)}
        return info
