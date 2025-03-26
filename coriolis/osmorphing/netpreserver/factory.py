# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing.netpreserver import ifcfg
from coriolis.osmorphing.netpreserver import interfaces
from coriolis.osmorphing.netpreserver import netplan
from coriolis.osmorphing.netpreserver import nmconnection

NET_PRESERVERS = [netplan.NetplanNetPreserver,
                  nmconnection.NmconnectionNetPreserver,
                  ifcfg.IfcfgNetPreserver,
                  interfaces.InterfacesNetPreserver]


def get_net_preserver(osmorphing_tool):
    for net_preserver_class in NET_PRESERVERS:
        net_preserver = net_preserver_class(osmorphing_tool)
        if net_preserver.check_net_preserver():
            return net_preserver_class
    return None
