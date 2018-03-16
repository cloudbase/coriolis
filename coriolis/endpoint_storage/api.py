# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def get_endpoint_storage(self, ctxt, endpoint_id, env):
        return self._rpc_client.get_endpoint_storage(
            ctxt, endpoint_id, env)
