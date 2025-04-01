# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.

import os

from oslo_log import log as logging

from coriolis.osmorphing.netpreserver import base

LOG = logging.getLogger(__name__)


class InterfacesNetPreserver(base.BaseNetPreserver):

    def __init__(self, osmorphing_tool):
            super(InterfacesNetPreserver, self).__init__(osmorphing_tool)
            self.ifaces_file = "etc/network/interfaces"

    def check_net_preserver(self):
        if self.osmorphing_tool._test_path(self.ifaces_file):
            if self.osmorphing_tool._list_dir(
                    os.path.dirname(self.ifaces_file)):
                return True
        return False

    def parse_network(self):
        paths = [self.ifaces_file]

        def _parse_iface_file(interface_file):
            nonlocal paths
            curr_ip_address = None
            curr_iface = None
            interfaces_contents = self.osmorphing_tool._read_file_sudo(
                interface_file).decode()
            LOG.debug(
                "Fetched %s contents: %s", interface_file, interfaces_contents)
            for line in interfaces_contents.splitlines():
                if line.strip().startswith("iface"):
                    words = line.split()
                    if len(words) > 1:
                        curr_iface = words[1]
                        self.interface_info[curr_iface] = {
                            "mac_address": "",
                            "ip_addresses": []
                        }
                elif line.strip().startswith("hwaddress ether"):
                    words = line.split()
                    if len(words) > 2:
                        if not curr_iface:
                            LOG.warn("Found MAC address %s does not belong to "
                                     "any interface stanza. Skipping.",
                                     words[2])
                            continue

                        self.interface_info[curr_iface][
                            "mac_address"] = words[2]
                elif line.strip().startswith("address"):
                    words = line.split()
                    if len(words) > 1:
                        curr_ip_address = words[1].split('/')[0]
                        if curr_iface:
                            self.interface_info[curr_iface][
                                "ip_addresses"].append(curr_ip_address)

                elif line.strip().startswith("source"):
                    words = line.split()
                    if len(words) > 1:
                        source_path = words[1]
                        if self.osmorphing_tool._test_path(source_path):
                            paths += self.osmorphing_tool._exec_cmd_chroot(
                                'ls -1 %s' % source_path).splitlines()

        while paths:
            _parse_iface_file(paths[0])
            paths.pop(0)
