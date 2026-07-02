# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import common


ROCKY_LINUX_DISTRO_IDENTIFIER = common.ROCKY_LINUX_DISTRO_IDENTIFIER


class RockyLinuxOSDetectTools(base.BaseLinuxOSDetectTools):

    def detect_os(self):
        return common.detect_os_from_os_release(
            self._get_os_release(),
            match_ids={"rocky"})
