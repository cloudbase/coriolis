# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.minion_manager.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.MinionManagerClient()

    def get_endpoint_destination_options(
            self, ctxt, endpoint_id, env=None, option_names=None):
        return self._rpc_client.get_endpoint_destination_options(
            ctxt, endpoint_id, env, option_names)

    def get_endpoint_source_minion_pool_options(
            self, ctxt, endpoint_id, env=None, option_names=None):
        return self._rpc_client.get_endpoint_source_minion_pool_options(
            ctxt, endpoint_id, env, option_names)

    def get_endpoint_destination_minion_pool_options(
            self, ctxt, endpoint_id, env=None, option_names=None):
        return self._rpc_client.get_endpoint_destination_minion_pool_options(
            ctxt, endpoint_id, env, option_names)

    def get_endpoint_source_options(
            self, ctxt, endpoint_id, env=None, option_names=None):
        return self._rpc_client.get_endpoint_source_options(
            ctxt, endpoint_id, env, option_names)
