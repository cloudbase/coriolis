# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing import centos
from coriolis.osmorphing.osdetect import rocky as rocky_osdetect


ROCKY_LINUX_DISTRO_IDENTIFIER = rocky_osdetect.ROCKY_LINUX_DISTRO_IDENTIFIER


class BaseRockyLinuxMorphingTools(centos.BaseCentOSMorphingTools):

    @classmethod
    def check_os_supported(cls, detected_os_info):
        if detected_os_info['distribution_name'] != (
                ROCKY_LINUX_DISTRO_IDENTIFIER):
            return False
        return cls._version_supported_util(
            detected_os_info['release_version'], minimum=8)
