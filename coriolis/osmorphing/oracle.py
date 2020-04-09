# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import re

from coriolis.osmorphing import redhat
from coriolis.osmorphing.osdetect import oracle as oracle_detect


ORACLE_DISTRO_IDENTIFIER = oracle_detect.ORACLE_DISTRO_IDENTIFIER


class BaseOracleMorphingTools(redhat.BaseRedHatMorphingTools):

    @classmethod
    def check_os_supported(cls, detected_os_info):
        if detected_os_info['distribution_name'] != (
                ORACLE_DISTRO_IDENTIFIER):
            return False
        return cls._version_supported_util(
            detected_os_info['release_version'], minimum=7)

    def _run_dracut(self):
        self._run_dracut_base('kernel')
        self._run_dracut_base('kernel-uek')
