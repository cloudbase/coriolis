from coriolis import exception
from coriolis.osmorphing.osmount import ubuntu


def get_os_mount_tools(ssh):
    os_mount_tools = [ubuntu.UbuntuOSMountTools]

    for cls in os_mount_tools:
        if cls.check_os(ssh):
            return cls()
    raise exception.CoriolisException("OS mount tools not found")