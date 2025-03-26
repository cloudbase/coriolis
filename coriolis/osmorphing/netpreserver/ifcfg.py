# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

import os
import re

from oslo_log import log as logging

from coriolis.osmorphing.netpreserver import base

LOG = logging.getLogger(__name__)


class IfcfgNetPreserver(base.BaseNetPreserver):

    def __init__(self, osmorphing_tool):
        super(IfcfgNetPreserver, self).__init__(osmorphing_tool)
        self.network_scripts_path = "etc/sysconfig/network-scripts"

    def check_net_preserver(self):
        if self.osmorphing_tool._test_path(self.network_scripts_path):
            ifcfg_files = self._get_net_config_files(self.network_scripts_path)
            if ifcfg_files:
                ifcfgs_ethernet = self._get_ifcfgs_by_type(
                    "Ethernet", self.network_scripts_path)
                if ifcfgs_ethernet:
                    return True
        return False

    def parse_network(self):
        ifcfgs_ethernet = self._get_ifcfgs_by_type(
            "Ethernet", self.network_scripts_path)
        if ifcfgs_ethernet:
            for ifcfg_file, ifcfg in ifcfgs_ethernet:
                name = ifcfg.get("DEVICE")
                if not name:
                    # Get the name from the config file
                    name = re.match("^.*/ifcfg-(.*)", ifcfg_file).groups()[0]
                mac_address = ifcfg.get("HWADDR")
                ip_address = ifcfg.get("IPADDR")
                self.interface_info[name] = {
                    "mac_address": mac_address,
                    "ip_addresses": [ip_address] if ip_address else []
                }

    def _get_net_config_files(self, network_scripts_path):
        dir_content = self.osmorphing_tool._list_dir(network_scripts_path)
        return [os.path.join(network_scripts_path, f) for f in
                dir_content if re.match("^ifcfg-(.*)", f)]

    def _get_ifcfgs_by_type(self, ifcfg_type, network_scripts_path):
        ifcfgs = []
        for ifcfg_file in self._get_net_config_files(network_scripts_path):
            ifcfg = self.osmorphing_tool._read_config_file(ifcfg_file)
            if ifcfg.get("TYPE") == ifcfg_type:
                ifcfgs.append((ifcfg_file, ifcfg))
        return ifcfgs
