# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis import exception
from coriolis.osmorphing.osmount import base
from coriolis import utils

LOG = logging.getLogger(__name__)

SUSE_DISTRO_IDENTIFIERS = [
    'sles', 'opensuse-leap', 'opensuse-tumbleweed', 'opensuse']

SSHD_CONFIG_PATH = "/etc/ssh/sshd_config"
USR_SSHD_CONFIG_PATH = "/usr/etc/ssh/sshd_config"


class SUSEOSMountTools(base.BaseLinuxOSMountTools):
    def check_os(self):
        os_info = utils.get_linux_os_info(self._ssh)
        if os_info and os_info[0] in SUSE_DISTRO_IDENTIFIERS:
            return True

    def _allow_ssh_env_vars(self):
        if not utils.test_ssh_path(self._ssh, SSHD_CONFIG_PATH):
            self._exec_cmd(
                "sudo cp %s %s" % (USR_SSHD_CONFIG_PATH, SSHD_CONFIG_PATH))
        self._exec_cmd(
            'sudo sed -i -e "\\$aAcceptEnv *" %s' % SSHD_CONFIG_PATH)
        try:
            utils.restart_service(self._ssh, "sshd")
        except exception.CoriolisException:
            LOG.warning(
                "Could not restart sshd service. The SSH connection "
                "may have been reset during the restart.")
        return True

    def setup(self):
        super(SUSEOSMountTools, self).setup()
        retry_ssh_cmd = utils.retry_on_error(
            max_attempts=10, sleep_seconds=30)(self._exec_cmd)
        retry_ssh_cmd("sudo -E zypper --non-interactive install lvm2 psmisc")
        self._exec_cmd("sudo modprobe dm-mod")
        self._exec_cmd("sudo rm -f /etc/lvm/devices/system.devices")
