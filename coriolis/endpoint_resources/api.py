# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from typing import Any

from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def get_endpoint_instances(
        self,
        ctxt,
        endpoint_id: str,
        source_environment: dict[Any, Any],
        marker=None,
        limit=None,
        instance_name_pattern: str | None = None,
        refresh: bool = False,
    ) -> list[dict]:
        return self._rpc_client.get_endpoint_instances(
            ctxt,
            endpoint_id,
            source_environment,
            marker,
            limit,
            instance_name_pattern,
            refresh=refresh,
        )

    def get_endpoint_instance(
        self,
        ctxt,
        endpoint_id: str,
        source_environment: dict[Any, Any],
        instance_name: str,
    ) -> dict:
        return self._rpc_client.get_endpoint_instance(
            ctxt, endpoint_id, source_environment, instance_name
        )

    def get_endpoint_networks(
        self,
        ctxt,
        endpoint_id: str,
        env: dict[Any, Any],
    ) -> list[dict]:
        return self._rpc_client.get_endpoint_networks(ctxt, endpoint_id, env)

    def get_endpoint_storage(
        self,
        ctxt,
        endpoint_id: str,
        env: dict[Any, Any],
    ) -> dict[str, list]:
        return self._rpc_client.get_endpoint_storage(ctxt, endpoint_id, env)

    def get_endpoint_inventory_csv(
        self,
        ctxt,
        endpoint_id: str,
        source_environment: dict[Any, Any],
    ) -> str:
        return self._rpc_client.get_endpoint_inventory_csv(
            ctxt, endpoint_id, source_environment
        )
