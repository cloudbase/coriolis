from coriolis.osmorphing import debian
from coriolis.osmorphing import redhat
from coriolis.osmorphing import ubuntu


def get_os_morphing_tools(ssh, os_root_dir, target_hypervisor,
                          target_platform):
    os_morphing_tools_clss = [debian.DebianMorphingTools,
                              ubuntu.UbuntuMorphingTools,
                              redhat.RedHatMorphingTools]

    for cls in os_morphing_tools_clss:
        tools = cls(ssh, os_root_dir, target_hypervisor, target_platform)
        os_info = tools.check_os()
        if os_info:
            return (tools, os_info)
    raise Exception("Cannot find the morphing tools for this OS image")
