# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis import exception
from coriolis.osmorphing.osdetect import centos as centos_detect
from coriolis.osmorphing import redhat
from coriolis import utils


CENTOS_DISTRO_IDENTIFIER = centos_detect.CENTOS_DISTRO_IDENTIFIER
CENTOS_STREAM_DISTRO_IDENTIFIER = centos_detect.CENTOS_STREAM_DISTRO_IDENTIFIER

LOG = logging.getLogger(__name__)


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

    def enable_repos(self, repo_names):
        """Enable repositories for CentOS.

        Uses yum-config-manager for CentOS 7 and earlier,
        dnf config-manager for CentOS 8 and later.
        """
        if not repo_names:
            return

        # Determine package manager based on version
        major_version = int(str(self._version).split('.')[0])
        if major_version >= 8:
            # CentOS 8+ uses dnf
            config_manager = 'dnf config-manager'
            enable_flag = '--set-enabled %s'
        else:
            # CentOS 7 and earlier use yum
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
