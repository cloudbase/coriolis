# Copyright 2025 Cloudbase Solutions Srl
# All Rights Reserved.


class BaseNetPreserver(object):

    def __init__(self, osmorphing_tool):
        # This is included so you can more easily update parsed configuration
        # As discussed, it should be in this format:
        # {"iname": {"mac_address": "", "ip_addresses": []}}
        self.osmorphing_tool = osmorphing_tool
        self.interface_info = dict()

    def check_net_preserver(self):
        # This will check its preserver dir and files, ideally return boolean
        pass

    def parse_network(self, nics_info):
        # This will go through the network config files and
        # populate `interface_info`
        pass
