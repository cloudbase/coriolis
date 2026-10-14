# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from typing import Any

from coriolis.minion_manager.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.MinionManagerClient()

    def create(
        self,
        ctxt,
        name: str,
        endpoint_id: str,
        pool_platform: str,
        pool_os_type: str,
        environment_options: dict[Any, Any],
        minimum_minions: int,
        maximum_minions: int,
        minion_max_idle_time: int,
        minion_retention_strategy: str,
        notes=None,
        skip_allocation: bool = False,
    ):
        return self._rpc_client.create_minion_pool(
            ctxt,
            name,
            endpoint_id,
            pool_platform,
            pool_os_type,
            environment_options,
            minimum_minions,
            maximum_minions,
            minion_max_idle_time,
            minion_retention_strategy,
            notes=notes,
            skip_allocation=skip_allocation,
        )

    def update(
        self,
        ctxt,
        minion_pool_id: str,
        updated_values: dict,
    ):
        return self._rpc_client.update_minion_pool(
            ctxt, minion_pool_id, updated_values=updated_values
        )

    def delete(self, ctxt, minion_pool_id: str) -> None:
        self._rpc_client.delete_minion_pool(ctxt, minion_pool_id)

    def get_minion_pools(self, ctxt) -> list:
        return self._rpc_client.get_minion_pools(ctxt)

    def get_minion_pool(self, ctxt, minion_pool_id: str):
        return self._rpc_client.get_minion_pool(ctxt, minion_pool_id)

    def allocate_minion_pool(self, ctxt, minion_pool_id: str):
        return self._rpc_client.allocate_minion_pool(ctxt, minion_pool_id)

    def refresh_minion_pool(self, ctxt, minion_pool_id: str):
        return self._rpc_client.refresh_minion_pool(ctxt, minion_pool_id)

    def deallocate_minion_pool(
        self,
        ctxt,
        minion_pool_id: str,
        force: bool = False,
    ):
        return self._rpc_client.deallocate_minion_pool(
            ctxt, minion_pool_id, force=force
        )
