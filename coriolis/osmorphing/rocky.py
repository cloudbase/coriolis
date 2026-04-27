# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

import logging

from coriolis import exception
from coriolis.osmorphing import centos
from coriolis.osmorphing.osdetect import rocky as rocky_osdetect
from coriolis.taskflow import utils


ROCKY_LINUX_DISTRO_IDENTIFIER = rocky_osdetect.ROCKY_LINUX_DISTRO_IDENTIFIER

LOG = logging.getLogger(__name__)


class BaseRockyLinuxMorphingTools(centos.BaseCentOSMorphingTools):

    UEFI_GRUB_LOCATION = "/boot/efi/EFI/rocky"

    @classmethod
    def check_os_supported(cls, detected_os_info):
        if detected_os_info['distribution_name'] != (
                ROCKY_LINUX_DISTRO_IDENTIFIER):
            return False
        return cls._version_supported_util(
            detected_os_info['release_version'], minimum=8)

    def enable_repos(self, repo_names):
        """Enable repositories for Rocky Linux.

        Uses dnf config-manager for all Rocky Linux versions (8+).
        """
        if not repo_names:
            return

        # Rocky Linux only exists as version 8+, always uses dnf
        config_manager = 'dnf config-manager'
        for repo in repo_names:
            cmd = '%s --set-enabled %s' % (config_manager, repo)
            try:
                self._exec_cmd_chroot(cmd)
                LOG.info("Enabled repository '%s' using %s",
                         repo, config_manager)
            except exception.CoriolisException:
                LOG.warning(f"Failed to enable repository {repo}. "
                            f"Error was: {utils.get_exception_details()}")
