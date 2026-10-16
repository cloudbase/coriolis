# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from typing import Any

from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def create(
        self,
        ctxt,
        transfer_scenario: str,
        origin_endpoint_id: str,
        destination_endpoint_id: str,
        origin_minion_pool_id: str | None,
        destination_minion_pool_id: str | None,
        instance_osmorphing_minion_pool_mappings: dict[Any, Any],
        source_environment: dict,
        destination_environment: dict[str, dict],
        instances: list[str],
        network_map: dict[Any, Any],
        storage_mappings: dict[Any, Any],
        notes: str | None = None,
        user_scripts=None,
        clone_disks: bool = True,
        skip_os_morphing: bool = False,
    ) -> dict:
        return self._rpc_client.create_instances_transfer(
            ctxt,
            transfer_scenario,
            origin_endpoint_id,
            destination_endpoint_id,
            origin_minion_pool_id,
            destination_minion_pool_id,
            instance_osmorphing_minion_pool_mappings,
            source_environment,
            destination_environment,
            instances,
            network_map,
            storage_mappings,
            notes,
            user_scripts,
            clone_disks,
            skip_os_morphing,
        )

    def update(
        self,
        ctxt,
        transfer_id: str,
        updated_properties: dict,
    ) -> dict:
        return self._rpc_client.update_transfer(ctxt, transfer_id, updated_properties)

    def delete(self, ctxt, transfer_id: str) -> None:
        self._rpc_client.delete_transfer(ctxt, transfer_id)

    def get_transfers(
        self,
        ctxt,
        include_tasks_executions: bool = False,
        include_task_info: bool = False,
        marker: str | None = None,
        limit: int | None = None,
        sort_keys: list | None = None,
        sort_dirs: list | None = None,
        filters: dict | None = None,
    ) -> list[dict]:
        return self._rpc_client.get_transfers(
            ctxt,
            include_tasks_executions,
            include_task_info=include_task_info,
            marker=marker,
            limit=limit,
            sort_keys=sort_keys,
            sort_dirs=sort_dirs,
            filters=filters,
        )

    def get_transfer(
        self,
        ctxt,
        transfer_id: str,
        include_task_info: bool = False,
    ) -> dict:
        return self._rpc_client.get_transfer(
            ctxt, transfer_id, include_task_info=include_task_info
        )

    def delete_disks(self, ctxt, transfer_id: str) -> dict:
        return self._rpc_client.delete_transfer_disks(ctxt, transfer_id)
