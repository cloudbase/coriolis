# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing.osdetect import common


ORACLE_DISTRO_IDENTIFIER = common.ORACLE_DISTRO_IDENTIFIER


class OracleOSDetectTools(base.BaseLinuxOSDetectTools):

    def detect_os(self):
        return common.detect_os_from_os_release(
            self._get_os_release(),
            match_ids={"ol"})
