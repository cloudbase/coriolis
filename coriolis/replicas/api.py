# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def create(self, ctxt, origin_endpoint_id, destination_endpoint_id,
               destination_environment, instances, network_map, notes=None):
        return self._rpc_client.create_instances_replica(
            ctxt, origin_endpoint_id, destination_endpoint_id,
            destination_environment, instances, network_map, notes)

    def delete(self, ctxt, replica_id):
        self._rpc_client.delete_replica(ctxt, replica_id)

    def get_replicas(self, ctxt, include_tasks_executions=False):
        return self._rpc_client.get_replicas(ctxt, include_tasks_executions)

    def get_replica(self, ctxt, replica_id):
        return self._rpc_client.get_replica(ctxt, replica_id)

    def delete_disks(self, ctxt, replica_id):
        return self._rpc_client.delete_replica_disks(ctxt, replica_id)
