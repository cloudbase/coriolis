# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import common


RED_HAT_DISTRO_IDENTIFIER = common.RED_HAT_DISTRO_IDENTIFIER


class RedHatOSDetectTools(base.BaseLinuxOSDetectTools):

    def detect_os(self):
        return common.detect_os_from_os_release(
            self._get_os_release(),
            match_ids={"rhel"})
