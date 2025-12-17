# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def get_endpoint_instances(self, ctxt, endpoint_id, source_environment,
                               marker=None, limit=None,
                               instance_name_pattern=None, refresh=False):
        return self._rpc_client.get_endpoint_instances(
            ctxt, endpoint_id, source_environment, marker,
            limit, instance_name_pattern, refresh=refresh)

    def get_endpoint_instance(
            self, ctxt, endpoint_id, source_environment, instance_name):
        return self._rpc_client.get_endpoint_instance(
            ctxt, endpoint_id, source_environment, instance_name)

    def get_endpoint_networks(self, ctxt, endpoint_id, env):
        return self._rpc_client.get_endpoint_networks(
            ctxt, endpoint_id, env)

    def get_endpoint_storage(self, ctxt, endpoint_id, env):
        return self._rpc_client.get_endpoint_storage(
            ctxt, endpoint_id, env)
