# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

import re

from oslo_log import log as logging

from coriolis.osmorphing.netpreserver import base

LOG = logging.getLogger(__name__)


class IfcfgNetPreserver(base.BaseNetPreserver):
    def check_net_preserver(self):
        network_scripts_path = self.osmorphing_tool._NETWORK_SCRIPTS_PATH
        if self.osmorphing_tool._test_path(network_scripts_path):
            ifcfg_files = self.osmorphing_tool._get_net_config_files(
                network_scripts_path
            )
            if ifcfg_files:
                ifcfgs_ethernet = self.osmorphing_tool._get_ifcfgs_by_type(
                    "Ethernet", network_scripts_path
                )
                if ifcfgs_ethernet:
                    return True
        return False

    def parse_network(self):
        network_scripts_path = self.osmorphing_tool._NETWORK_SCRIPTS_PATH
        ifcfgs_ethernet = self.osmorphing_tool._get_ifcfgs_by_type(
            "Ethernet", network_scripts_path
        )
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
                    "ip_addresses": [ip_address] if ip_address else [],
                }
