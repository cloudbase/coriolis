# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def create(
        self,
        ctxt,
        transfer_id: str,
        shutdown_instances: bool,
        auto_deploy: bool,
    ) -> dict:
        return self._rpc_client.execute_transfer_tasks(
            ctxt, transfer_id, shutdown_instances, auto_deploy
        )

    def delete(
        self,
        ctxt,
        transfer_id: str,
        execution_id: str,
    ) -> None:
        self._rpc_client.delete_transfer_tasks_execution(
            ctxt, transfer_id, execution_id
        )

    def cancel(
        self,
        ctxt,
        transfer_id: str,
        execution_id: str,
        force: bool,
    ) -> None:
        self._rpc_client.cancel_transfer_tasks_execution(
            ctxt, transfer_id, execution_id, force
        )

    def get_executions(
        self,
        ctxt,
        transfer_id: str,
        include_tasks: bool = False,
        marker: str | None = None,
        limit: int | None = None,
        sort_keys: list | None = None,
        sort_dirs: list | None = None,
        filters: dict | None = None,
    ) -> list:
        return self._rpc_client.get_transfer_tasks_executions(
            ctxt,
            transfer_id,
            include_tasks,
            marker,
            limit,
            sort_keys,
            sort_dirs,
            filters,
        )

    def get_execution(
        self,
        ctxt,
        transfer_id: str,
        execution_id: str,
    ) -> dict:
        return self._rpc_client.get_transfer_tasks_execution(
            ctxt, transfer_id, execution_id
        )
