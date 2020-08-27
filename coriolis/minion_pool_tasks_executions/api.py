# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import utils
from coriolis.conductor.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()

    def list(self, ctxt, minion_pool_id, include_tasks=False):
        return self._rpc_client.get_minion_pool_lifecycle_executions(
            ctxt, minion_pool_id, include_tasks=include_tasks)

    def get(self, ctxt, minion_pool_id, execution_id):
        return self._rpc_client.get_minion_pool_lifecycle_execution(
            ctxt, minion_pool_id, execution_id)

    def cancel(self, ctxt, minion_pool_id, execution_id, force):
        return self._rpc_client.cancel_minion_pool_lifecycle_execution(
            ctxt, minion_pool_id, execution_id, force)

    def delete(self, ctxt, minion_pool_id, execution_id):
        return self._rpc_client.delete_minion_pool_lifecycle_execution(
            ctxt, minion_pool_id, execution_id)
