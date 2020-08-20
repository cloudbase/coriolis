# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import utils
from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def create(
            self, ctxt, host, binary, topic, mapped_regions,
            enabled):
        return self._rpc_client.register_service(
            ctxt, host, binary, topic, enabled, mapped_regions)

    def update(self, ctxt, service_id, updated_values):
        return self._rpc_client.update_service(
            ctxt, service_id, updated_values)

    def delete(self, ctxt, region_id):
        self._rpc_client.delete_service(ctxt, region_id)

    def get_services(self, ctxt):
        return self._rpc_client.get_services(ctxt)

    def get_service(self, ctxt, service_id):
        return self._rpc_client.get_service(ctxt, service_id)
