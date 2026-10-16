# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def deploy_transfer_instances(
        self,
        ctxt,
        transfer_id: str,
        instance_osmorphing_minion_pool_mappings: dict[str, str],
        clone_disks: bool = False,
        force: bool = False,
        skip_os_morphing: bool = False,
        user_scripts: dict | None = None,
    ) -> dict:
        return self._rpc_client.deploy_transfer_instances(
            ctxt,
            transfer_id,
            instance_osmorphing_minion_pool_mappings=(
                instance_osmorphing_minion_pool_mappings
            ),
            clone_disks=clone_disks,
            force=force,
            skip_os_morphing=skip_os_morphing,
            user_scripts=user_scripts,
        )

    def delete(self, ctxt, deployment_id: str) -> None:
        self._rpc_client.delete_deployment(ctxt, deployment_id)

    def cancel(self, ctxt, deployment_id: str, force: bool) -> None:
        self._rpc_client.cancel_deployment(ctxt, deployment_id, force)

    def get_deployments(
        self,
        ctxt,
        include_tasks: bool = False,
        include_task_info: bool = False,
        marker=None,
        limit=None,
        sort_keys: list | None = None,
        sort_dirs: list | None = None,
        filters: dict | None = None,
    ) -> list:
        return self._rpc_client.get_deployments(
            ctxt,
            include_tasks,
            include_task_info=include_task_info,
            marker=marker,
            limit=limit,
            sort_keys=sort_keys,
            sort_dirs=sort_dirs,
            filters=filters,
        )

    def get_deployment(
        self,
        ctxt,
        deployment_id: str,
        include_task_info: bool = False,
    ) -> dict:
        return self._rpc_client.get_deployment(
            ctxt, deployment_id, include_task_info=include_task_info
        )
