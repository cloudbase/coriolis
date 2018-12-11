# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import utils
from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def create(self, ctxt, name, endpoint_type, description,
               connection_info):
        return self._rpc_client.create_endpoint(
            ctxt, name, endpoint_type, description, connection_info)

    def update(self, ctxt, endpoint_id, properties):
        return self._rpc_client.update_endpoint(
            ctxt, endpoint_id, properties)

    def delete(self, ctxt, endpoint_id):
        self._rpc_client.delete_endpoint(ctxt, endpoint_id)

    def get_endpoints(self, ctxt):
        return self._rpc_client.get_endpoints(ctxt)

    def get_endpoint(self, ctxt, endpoint_id):
        return self._rpc_client.get_endpoint(ctxt, endpoint_id)

    def validate_connection(self, ctxt, endpoint_id):
        return self._rpc_client.validate_endpoint_connection(
            ctxt, endpoint_id)

    @utils.bad_request_on_error("Invalid destination environment: %s")
    def validate_target_environment(self, ctxt, endpoint_id, target_env):
        return self._rpc_client.validate_endpoint_target_environment(
            ctxt, endpoint_id, target_env)

    @utils.bad_request_on_error("Invalid source environment: %s")
    def validate_source_environment(self, ctxt, endpoint_id, source_env):
        return self._rpc_client.validate_endpoint_source_environment(
            ctxt, endpoint_id, source_env)
