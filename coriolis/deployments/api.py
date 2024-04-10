# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def deploy_replica_instances(self, ctxt, replica_id,
                                 instance_osmorphing_minion_pool_mappings,
                                 clone_disks=False, force=False,
                                 skip_os_morphing=False, user_scripts=None):
        return self._rpc_client.deploy_replica_instances(
            ctxt, replica_id, instance_osmorphing_minion_pool_mappings=(
                instance_osmorphing_minion_pool_mappings),
            clone_disks=clone_disks, force=force,
            skip_os_morphing=skip_os_morphing,
            user_scripts=user_scripts)

    def delete(self, ctxt, deployment_id):
        self._rpc_client.delete_deployment(ctxt, deployment_id)

    def cancel(self, ctxt, deployment_id, force):
        self._rpc_client.cancel_deployment(ctxt, deployment_id, force)

    def get_deployments(self, ctxt, include_tasks=False,
                        include_task_info=False):
        return self._rpc_client.get_deployments(
            ctxt, include_tasks, include_task_info=include_task_info)

    def get_deployment(self, ctxt, deployment_id, include_task_info=False):
        return self._rpc_client.get_deployment(
            ctxt, deployment_id, include_task_info=include_task_info)
