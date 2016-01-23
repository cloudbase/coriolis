from coriolis.osmorphing import ubuntu


def get_os_morphing_tools(ssh, os_root_dir):
    os_morphing_tools_clss = [ubuntu.UbuntuOSMorphingTools]

    for cls in os_morphing_tools_clss:
        if cls.check_os(ssh, os_root_dir):
            return cls(os_root_dir)
    raise Exception("Cannot find the morphing tools for this OS image")
