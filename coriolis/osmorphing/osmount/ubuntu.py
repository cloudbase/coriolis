# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis.osmorphing.osmount import base
from coriolis import utils

LOG = logging.getLogger(__name__)


class UbuntuOSMountTools(base.BaseLinuxOSMountTools):
    def check_os(self):
        os_info = utils.get_linux_os_info(self._ssh)
        if os_info and os_info[0] == 'Ubuntu':
            return True

    def _pre_mount_os(self):
        self._exec_cmd("sudo apt-get update -y")
        self._exec_cmd("sudo apt-get install lvm2 -y")
        self._exec_cmd("sudo modprobe dm-mod")
