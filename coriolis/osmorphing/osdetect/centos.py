# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import common


CENTOS_DISTRO_IDENTIFIER = common.CENTOS_DISTRO_IDENTIFIER
CENTOS_STREAM_DISTRO_IDENTIFIER = common.CENTOS_STREAM_DISTRO_IDENTIFIER


class CentOSOSDetectTools(base.BaseLinuxOSDetectTools):

    def detect_os(self):
        return common.detect_os_from_os_release(
            self._get_os_release(),
            match_ids={"centos", "almalinux"})
