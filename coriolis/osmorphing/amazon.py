# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis import exception
from coriolis.osmorphing.osdetect import amazon as amazon_detect
from coriolis.osmorphing import redhat
from coriolis import utils


AMAZON_DISTRO_NAME_IDENTIFIER = amazon_detect.AMAZON_DISTRO_NAME

LOG = logging.getLogger(__name__)


class BaseAmazonLinuxOSMorphingTools(redhat.BaseRedHatMorphingTools):

    UEFI_GRUB_LOCATION = "/boot/efi/EFI/amzn"

    @classmethod
    def check_os_supported(cls, detected_os_info):
        if detected_os_info['distribution_name'] != (
                AMAZON_DISTRO_NAME_IDENTIFIER):
            return False
        return cls._version_supported_util(
            detected_os_info['release_version'], minimum=2)

    def enable_repos(self, repo_names):
        """Enable repositories for Amazon Linux.

        Uses yum-config-manager for Amazon Linux 2,
        dnf config-manager for Amazon Linux 2023 and later.
        """
        if not repo_names:
            return

        # Determine package manager based on version
        # Amazon Linux 2 has version "2", AL2023 has version "2023"
        try:
            major_version = int(str(self._version).split('.')[0])
        except (ValueError, AttributeError):
            # Fallback to yum if version parsing fails
            major_version = 2

        if major_version >= 2023:
            # Amazon Linux 2023+ uses dnf
            config_manager = 'dnf config-manager'
            enable_flag = '--set-enabled %s'
        else:
            # Amazon Linux 2 and earlier use yum
            config_manager = 'yum-config-manager'
            enable_flag = '--enable %s'

        for repo in repo_names:
            cmd = '%s %s' % (config_manager, enable_flag % repo)
            try:
                self._exec_cmd_chroot(cmd)
                LOG.info("Enabled repository '%s' using %s",
                         repo, config_manager)
            except exception.CoriolisException:
                LOG.warning(f"Failed to enable repository {repo}. "
                            f"Error was: {utils.get_exception_details()}")
