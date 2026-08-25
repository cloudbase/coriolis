# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

import re

from oslo_log import log as logging

from coriolis.osmorphing.netpreserver import base

LOG = logging.getLogger(__name__)


class NmconnectionNetPreserver(base.BaseNetPreserver):
    def check_net_preserver(self):
        nmconnection_path = self.osmorphing_tool._NM_CONNECTIONS_PATH
        if self.osmorphing_tool._test_path(nmconnection_path):
            nmconnection_files = self.osmorphing_tool._get_nmconnection_files(
                nmconnection_path
            )
            if nmconnection_files:
                nmconnection_ethernet = self.osmorphing_tool._get_keyfiles_by_type(
                    "ethernet", nmconnection_path
                )
                if nmconnection_ethernet:
                    return True
        return False

    def parse_network(self):
        nmconnection_path = self.osmorphing_tool._NM_CONNECTIONS_PATH
        nmconnection_ethernet = self.osmorphing_tool._get_keyfiles_by_type(
            "ethernet", nmconnection_path
        )
        if nmconnection_ethernet:
            for nmconn_file, nmconn in nmconnection_ethernet:
                name = nmconn.get("interface-name", nmconn.get("id"))
                if not name:
                    name = re.match(r"^.*/(.*)\.nmconnection$", nmconn_file).groups()[0]
                name = name.replace(" ", "_")
                mac_address = nmconn.get("mac-address")
                self.interface_info[name] = {
                    "mac_address": mac_address,
                    "ip_addresses": [],
                }
                for key, value in nmconn.items():
                    if key.lower().startswith("address") and value:
                        for addr in value.split(','):
                            addr = addr.strip()
                            ip = addr.strip().split('/')[0]
                            self.interface_info[name]["ip_addresses"].append(ip)
                if not mac_address and not self.interface_info[name]["ip_addresses"]:
                    LOG.warning(
                        "Could not find MAC address or IP addresses for "
                        "interface '%s' in nmconnection configuration "
                        "'%s'",
                        name,
                        nmconn_file,
                    )
