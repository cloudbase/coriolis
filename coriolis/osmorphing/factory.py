from coriolis.osmorphing import debian
from coriolis.osmorphing import ubuntu


def get_os_morphing_tools(ssh, os_root_dir):
    os_morphing_tools_clss = [debian.DebianOSMorphingTools,
                              ubuntu.UbuntuOSMorphingTools]

    for cls in os_morphing_tools_clss:
        os_info = cls.check_os(ssh, os_root_dir)
        if os_info:
            return cls(os_root_dir), os_info
    raise Exception("Cannot find the morphing tools for this OS image")
