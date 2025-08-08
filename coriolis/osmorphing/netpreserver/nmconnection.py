# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

import os
import re

from oslo_log import log as logging

from coriolis.osmorphing.netpreserver import base

LOG = logging.getLogger(__name__)


class NmconnectionNetPreserver(base.BaseNetPreserver):

    def __init__(self, osmorphing_tool):
        super(NmconnectionNetPreserver, self).__init__(osmorphing_tool)
        self.nmconnection_file = "etc/NetworkManager/system-connections"

    def check_net_preserver(self):
        if self.osmorphing_tool._test_path(self.nmconnection_file):
            nmconnection_files = self._get_nmconnection_files(
                self.nmconnection_file)
            if nmconnection_files:
                nmconnection_ethernet = self._get_keyfiles_by_type(
                    "ethernet", self.nmconnection_file)
                if nmconnection_ethernet:
                    return True
        return False

    def parse_network(self):
        nmconnection_ethernet = self._get_keyfiles_by_type(
            "ethernet", self.nmconnection_file)
        if nmconnection_ethernet:
            for nmconn_file, nmconn in nmconnection_ethernet:
                name = nmconn.get("connection", {}).get("id")
                if not name:
                    name = re.match(r"^.*/(.*)\.nmconnection$",
                                    nmconn_file).groups()[0]
                mac_address = nmconn.get("mac-address")
                self.interface_info[name] = {
                    "mac_address": mac_address,
                    "ip_addresses": []
                }
                for key, value in nmconn.items():
                    if key.lower().startswith("address") and value:
                        for addr in value.split(','):
                            addr = addr.strip()
                            ip = addr.strip().split('/')[0]
                            self.interface_info[name][
                                "ip_addresses"].append(ip)
                if not mac_address and not self.interface_info[
                    name]["ip_addresses"]:
                    LOG.warning(
                        "Could not find MAC address or IP addresses for "
                        "interface '%s' in nmconnection configuration "
                        "'%s'", name, nmconn_file)

    def _get_nmconnection_files(self, network_scripts_path):
        dir_content = self.osmorphing_tool._list_dir(network_scripts_path)
        return [os.path.join(network_scripts_path, f)
                for f in dir_content if re.match(
                    r"^(.*\.nmconnection)$", f)]

    def _get_keyfiles_by_type(self, nmconnection_type, network_scripts_path):
        keyfiles = []
        for file in self._get_nmconnection_files(network_scripts_path):
            keyfile = self.osmorphing_tool._read_config_file_sudo(file)
            if keyfile.get("type") == nmconnection_type:
                keyfiles.append((file, keyfile))
        return keyfiles
