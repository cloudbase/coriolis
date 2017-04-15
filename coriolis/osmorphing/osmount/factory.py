# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools

from oslo_log import log as logging

from coriolis import constants
from coriolis import exception
from coriolis.osmorphing.osmount import redhat
from coriolis.osmorphing.osmount import ubuntu
from coriolis.osmorphing.osmount import windows

LOG = logging.getLogger(__name__)


def get_os_mount_tools(os_type, connection_info, event_manager,
                       ignore_devices):
    os_mount_tools = {constants.OS_TYPE_LINUX: [ubuntu.UbuntuOSMountTools,
                                                redhat.RedHatOSMountTools],
                      constants.OS_TYPE_WINDOWS: [windows.WindowsMountTools]}

    if os_type and os_type not in os_mount_tools:
        raise exception.CoriolisException("Unsupported OS type: %s" % os_type)

    for cls in os_mount_tools.get(os_type,
                                  itertools.chain(*os_mount_tools.values())):
        tools = cls(connection_info, event_manager, ignore_devices)
        LOG.debug("Testing OS mount tools: %s", cls.__name__)
        if tools.check_os():
            return tools
    raise exception.CoriolisException("OS mount tools not found")
