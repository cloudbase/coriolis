# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis.osmorphing.osmount import base
from coriolis import utils

LOG = logging.getLogger(__name__)


class UbuntuOSMountTools(base.BaseLinuxOSMountTools):
    def check_os(self):
        os_info = utils.get_linux_os_info(self._ssh)
        if os_info and os_info[0] in ('Ubuntu', 'ubuntu'):
            return True

    def setup(self):
        super(UbuntuOSMountTools, self).setup()

        # NOTE(aznashwan): it's possible that if the Ubuntu minion has
        # unattended automatic upgrades enabled, the package list
        # locks (/var/lib/apt/lists/lock*) may be held because
        # another package list refresh is happening.
        # Apart from relying on possibly not-yet-installed tools like `fuser`,
        # or checking every /proc/*/fd ourselves, we simply retry it:
        retry_ssh_cmd = utils.retry_on_error(
            max_attempts=10, sleep_seconds=30)(self._exec_cmd)
        retry_ssh_cmd("sudo -E apt-get update -y")

        # NOTE(aznashwan): in case an unattended upgrade is already happening
        # and is at the package installation stage (in which case the
        # /var/lib/dpkg/* locks will be held), we pass a 10-minute timeout:
        self._exec_cmd(
            "sudo -E apt-get -o DPkg::Lock::Timeout=600 "
            "install lvm2 psmisc -y")

        self._exec_cmd("sudo modprobe dm-mod")

    def _allow_ssh_env_vars(self):
        self._exec_cmd('sudo sed -i -e "\$aAcceptEnv *" /etc/ssh/sshd_config')
        utils.restart_service(self._ssh, "sshd")
        return True
