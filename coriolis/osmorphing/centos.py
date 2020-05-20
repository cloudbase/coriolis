# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.


from coriolis.osmorphing import redhat
from coriolis.osmorphing.osdetect import centos as centos_detect


CENTOS_DISTRO_IDENTIFIER = centos_detect.CENTOS_DISTRO_IDENTIFIER


class BaseCentOSMorphingTools(redhat.BaseRedHatMorphingTools):

    @classmethod
    def check_os_supported(cls, detected_os_info):
        if detected_os_info['distribution_name'] != (
                CENTOS_DISTRO_IDENTIFIER):
            return False
        return cls._version_supported_util(
            detected_os_info['release_version'], minimum=6)
