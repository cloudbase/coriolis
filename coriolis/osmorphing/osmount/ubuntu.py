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
        self._exec_cmd("sudo -E apt-get update -y")
        self._exec_cmd("sudo -E apt-get install lvm2 psmisc -y")
        self._exec_cmd("sudo modprobe dm-mod")

    def _allow_ssh_env_vars(self):
        self._exec_cmd('sudo sed -i -e "\$aAcceptEnv *" /etc/ssh/sshd_config')
        utils.restart_service(self._ssh, "sshd")
        return True
