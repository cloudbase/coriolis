# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def get_available_providers(self, ctxt):
        return self._rpc_client.get_available_providers(ctxt)

    def get_provider_schemas(self, ctxt, platform_name, provider_type):
        return self._rpc_client.get_provider_schemas(
            ctxt, platform_name, int(provider_type))
