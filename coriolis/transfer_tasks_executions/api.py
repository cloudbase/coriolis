# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def create(self, ctxt, transfer_id, shutdown_instances, auto_deploy):
        return self._rpc_client.execute_transfer_tasks(
            ctxt, transfer_id, shutdown_instances, auto_deploy)

    def delete(self, ctxt, transfer_id, execution_id):
        self._rpc_client.delete_transfer_tasks_execution(
            ctxt, transfer_id, execution_id)

    def cancel(self, ctxt, transfer_id, execution_id, force):
        self._rpc_client.cancel_transfer_tasks_execution(
            ctxt, transfer_id, execution_id, force)

    def get_executions(self, ctxt, transfer_id, include_tasks=False):
        return self._rpc_client.get_transfer_tasks_executions(
            ctxt, transfer_id, include_tasks)

    def get_execution(self, ctxt, transfer_id, execution_id):
        return self._rpc_client.get_transfer_tasks_execution(
            ctxt, transfer_id, execution_id)
