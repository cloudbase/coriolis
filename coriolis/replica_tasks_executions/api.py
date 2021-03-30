# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient(
            reset_transport_on_call=False)

    def create(self, ctxt, replica_id, shutdown_instances):
        return self._rpc_client.execute_replica_tasks(
            ctxt, replica_id, shutdown_instances)

    def delete(self, ctxt, replica_id, execution_id):
        self._rpc_client.delete_replica_tasks_execution(
            ctxt, replica_id, execution_id)

    def cancel(self, ctxt, replica_id, execution_id, force):
        self._rpc_client.cancel_replica_tasks_execution(
            ctxt, replica_id, execution_id, force)

    def get_executions(self, ctxt, replica_id, include_tasks=False):
        return self._rpc_client.get_replica_tasks_executions(
            ctxt, replica_id, include_tasks)

    def get_execution(self, ctxt, replica_id, execution_id):
        return self._rpc_client.get_replica_tasks_execution(
            ctxt, replica_id, execution_id)
