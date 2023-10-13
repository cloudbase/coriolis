# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing.osdetect import amazon as amazon_detect
from coriolis.osmorphing import redhat


AMAZON_DISTRO_NAME_IDENTIFIER = amazon_detect.AMAZON_DISTRO_NAME


class BaseAmazonLinuxOSMorphingTools(redhat.BaseRedHatMorphingTools):

    UEFI_GRUB_LOCATION = "/boot/efi/EFI/amzn"

    @classmethod
    def check_os_supported(cls, detected_os_info):
        if detected_os_info['distribution_name'] != (
                AMAZON_DISTRO_NAME_IDENTIFIER):
            return False
        return cls._version_supported_util(
            detected_os_info['release_version'], minimum=2)
