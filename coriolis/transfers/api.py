# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def create(self, ctxt, transfer_scenario,
               origin_endpoint_id, destination_endpoint_id,
               origin_minion_pool_id, destination_minion_pool_id,
               instance_osmorphing_minion_pool_mappings,
               source_environment, destination_environment, instances,
               network_map, storage_mappings, notes=None, user_scripts=None,
               clone_disks=True, skip_os_morphing=False):
        return self._rpc_client.create_instances_transfer(
            ctxt, transfer_scenario,
            origin_endpoint_id, destination_endpoint_id,
            origin_minion_pool_id, destination_minion_pool_id,
            instance_osmorphing_minion_pool_mappings,
            source_environment, destination_environment, instances,
            network_map, storage_mappings, notes, user_scripts, clone_disks,
            skip_os_morphing)

    def update(self, ctxt, transfer_id, updated_properties):
        return self._rpc_client.update_transfer(
            ctxt, transfer_id, updated_properties)

    def delete(self, ctxt, transfer_id):
        self._rpc_client.delete_transfer(ctxt, transfer_id)

    def get_transfers(self, ctxt, include_tasks_executions=False,
                      include_task_info=False):
        return self._rpc_client.get_transfers(
            ctxt, include_tasks_executions,
            include_task_info=include_task_info)

    def get_transfer(self, ctxt, transfer_id, include_task_info=False):
        return self._rpc_client.get_transfer(
            ctxt, transfer_id, include_task_info=include_task_info)

    def delete_disks(self, ctxt, transfer_id):
        return self._rpc_client.delete_transfer_disks(ctxt, transfer_id)
