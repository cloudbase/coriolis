# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing.netpreserver import ifcfg
from coriolis.osmorphing.netpreserver import interfaces
from coriolis.osmorphing.netpreserver import netplan
from coriolis.osmorphing.netpreserver import nmconnection

# This should contain the list of netpreserver classes
NET_PRESERVERS = [netplan.NetplanNetPreserver,
                  nmconnection.NmconnectionNetPreserver,
                  ifcfg.IfcfgNetPreserver,
                  interfaces.InterfacesNetPreserver]


def get_net_preserver(osmorphing_tool):
    # The network configuration class identification goes here.
    # Basically just go through the NET_PRESERVER list of classes, stop
    # when cls.check_net_preserver returns True, return cls
    for net_preserver_class in NET_PRESERVERS:
        net_preserver = net_preserver_class(osmorphing_tool)
        if net_preserver.check_net_preserver():
            return net_preserver_class
    return None
