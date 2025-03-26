# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

import yaml

from oslo_log import log as logging

from coriolis.osmorphing.netpreserver import base
from coriolis import utils

LOG = logging.getLogger(__name__)


class NetplanNetPreserver(base.BaseNetPreserver):

    def __init__(self, osmorphing_tool):
        super(NetplanNetPreserver, self).__init__(osmorphing_tool)
        self.netplan_base = "etc/netplan"
        self.netplan_files = []

    def check_net_preserver(self):
        if self.osmorphing_tool._test_path(self.netplan_base):
            return any(f.endswith((".yaml", ".yml"))
                       for f in self.osmorphing_tool._list_dir(
                           self.netplan_base))
        return False

    def parse_network(self):
        self.netplan_files = [f for f in self.osmorphing_tool._list_dir(
                              self.netplan_base)
                              if f.endswith(".yaml") or f.endswith(".yml")]
        for cfg in self.netplan_files:
            cfg_path = "%s/%s" % (self.netplan_base, cfg)
            try:
                contents = yaml.safe_load(self.osmorphing_tool._read_file_sudo(
                                          cfg_path).decode())
                LOG.info("Parsing netplan configuration '%s'", cfg_path)
                ifaces = contents.get('network', {}).get('ethernets', {})
                for iface, net_cfg in ifaces.items():
                    self.interface_info[iface] = {"mac_address": "",
                                                  "ip_addresses": []}
                    mac_address = net_cfg.get('match', {}).get('macaddress')
                    if mac_address:
                        self.interface_info[iface]["mac_address"] = mac_address
                    else:
                        LOG.warn(
                            "Could not find MAC address or IP addresses for "
                            "interface '%s' in netplan configuration '%s'",
                            iface, cfg_path)

                    for ip in net_cfg.get('addresses', []):
                        if isinstance(ip, dict):
                            ip_keys = list(ip.keys())
                            if not ip_keys:
                                LOG.warning(
                                    "Found empty IP address object entry. "
                                    "Skipping")
                                continue
                            ip_obj_addrs = [
                                addr.split('/')[0] for addr in ip_keys]
                            self.interface_info[iface]["ip_addresses"].extend(
                                ip_obj_addrs)
                        else:
                            self.interface_info[iface]["ip_addresses"].append(
                                ip.split('/')[0])
                if not self.interface_info[iface]["ip_addresses"]:
                    LOG.warning(
                        "No IP addresses found for interface '%s' in netplan"
                        "config '%s'", iface, cfg_path)
            except yaml.YAMLError:
                LOG.warn(
                    "Could not parse netplan configuration '%s'. Invalid YAML "
                    "file: %s", cfg_path, utils.get_exception_details())
