# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from typing import Any

from coriolis import utils
from coriolis.conductor.rpc import client as rpc_conductor_client
from coriolis.minion_manager.rpc import client as rpc_minion_manager_client


class API(object):
    def __init__(self):
        self._rpc_conductor_client = rpc_conductor_client.ConductorClient()
        self._rpc_minion_manager_client = (
            rpc_minion_manager_client.MinionManagerClient()
        )

    def create(
        self,
        ctxt,
        name: str,
        endpoint_type: str,
        description: str,
        connection_info: dict[str, str],
        mapped_regions: list[str],
    ):
        return self._rpc_conductor_client.create_endpoint(
            ctxt, name, endpoint_type, description, connection_info, mapped_regions
        )

    def update(
        self,
        ctxt,
        endpoint_id: str,
        properties: dict,
    ):
        return self._rpc_conductor_client.update_endpoint(ctxt, endpoint_id, properties)

    def delete(self, ctxt, endpoint_id: str) -> None:
        self._rpc_conductor_client.delete_endpoint(ctxt, endpoint_id)

    def get_endpoints(self, ctxt) -> list[Any]:
        return self._rpc_conductor_client.get_endpoints(ctxt)

    def get_endpoint(self, ctxt, endpoint_id: str):
        return self._rpc_conductor_client.get_endpoint(ctxt, endpoint_id)

    def validate_connection(self, ctxt, endpoint_id: str) -> tuple:
        return self._rpc_conductor_client.validate_endpoint_connection(
            ctxt, endpoint_id
        )

    @utils.bad_request_on_error("Invalid destination environment: %s")
    def validate_target_environment(
        self,
        ctxt,
        endpoint_id: str,
        target_env: dict,
    ) -> tuple[bool, None]:
        return self._rpc_conductor_client.validate_endpoint_target_environment(
            ctxt, endpoint_id, target_env
        )

    @utils.bad_request_on_error("Invalid source environment: %s")
    def validate_source_environment(
        self,
        ctxt,
        endpoint_id: str,
        source_env: dict,
    ) -> tuple[bool, None]:
        return self._rpc_conductor_client.validate_endpoint_source_environment(
            ctxt, endpoint_id, source_env
        )

    @utils.bad_request_on_error("Invalid source minion pool environment: %s")
    def validate_endpoint_source_minion_pool_options(
        self,
        ctxt,
        endpoint_id: str,
        pool_environment: dict[Any, Any],
    ) -> tuple[bool, None]:
        return (
            self._rpc_minion_manager_client.validate_endpoint_source_minion_pool_options
        )(ctxt, endpoint_id, pool_environment)

    @utils.bad_request_on_error("Invalid destination minion pool environment: %s")
    def validate_endpoint_destination_minion_pool_options(
        self,
        ctxt,
        endpoint_id: str,
        pool_environment: dict[Any, Any],
    ) -> tuple[bool, None]:
        return (
            self._rpc_minion_manager_client.validate_endpoint_destination_minion_pool_options
        )(ctxt, endpoint_id, pool_environment)
