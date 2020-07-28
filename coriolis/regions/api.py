# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import utils
from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def create(self, ctxt, region_name, description, enabled=True):
        return self._rpc_client.create_region(
            ctxt, region_name, description=description, enabled=enabled)

    def update(self, ctxt, region_id, updated_values):
        return self._rpc_client.update_region(
            ctxt, region_id, updated_values=updated_values)

    def delete(self, ctxt, region_id):
        self._rpc_client.delete_region(ctxt, region_id)

    def get_regions(self, ctxt):
        return self._rpc_client.get_regions(ctxt)

    def get_region(self, ctxt, region_id):
        return self._rpc_client.get_region(ctxt, region_id)
