# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.


from coriolis.osmorphing.osdetect import centos as centos_detect
from coriolis.osmorphing import redhat


CENTOS_DISTRO_IDENTIFIER = centos_detect.CENTOS_DISTRO_IDENTIFIER
CENTOS_STREAM_DISTRO_IDENTIFIER = centos_detect.CENTOS_STREAM_DISTRO_IDENTIFIER


class BaseCentOSMorphingTools(redhat.BaseRedHatMorphingTools):

    UEFI_GRUB_LOCATION = "/boot/efi/EFI/centos"

    @classmethod
    def check_os_supported(cls, detected_os_info):
        supported_oses = [
            CENTOS_STREAM_DISTRO_IDENTIFIER, CENTOS_DISTRO_IDENTIFIER]
        if detected_os_info['distribution_name'] not in supported_oses:
            return False
        return cls._version_supported_util(
            detected_os_info['release_version'], minimum=6)
