from coriolis import exception
from coriolis.osmorphing.osmount import ubuntu


def get_os_mount_tools(ssh, event_manager):
    os_mount_tools = [ubuntu.UbuntuOSMountTools]

    for cls in os_mount_tools:
        tools = cls(ssh, event_manager)
        if tools.check_os():
            return tools
    raise exception.CoriolisException("OS mount tools not found")
