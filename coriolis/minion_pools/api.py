# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import utils
from coriolis.minion_manager.rpc import client as rpc_client


class API(object):
    def __init__(self):
        self._rpc_client = rpc_client.MinionManagerClient()

    def create(
            self, ctxt, name, endpoint_id, pool_platform, pool_os_type,
            environment_options, minimum_minions, maximum_minions,
            minion_max_idle_time, minion_retention_strategy, notes=None,
            skip_allocation=False):
        return self._rpc_client.create_minion_pool(
            ctxt, name, endpoint_id, pool_platform, pool_os_type,
            environment_options, minimum_minions, maximum_minions,
            minion_max_idle_time, minion_retention_strategy, notes=notes,
            skip_allocation=skip_allocation)

    def update(self, ctxt, minion_pool_id, updated_values):
        return self._rpc_client.update_minion_pool(
            ctxt, minion_pool_id, updated_values=updated_values)

    def delete(self, ctxt, minion_pool_id):
        self._rpc_client.delete_minion_pool(ctxt, minion_pool_id)

    def get_minion_pools(self, ctxt):
        return self._rpc_client.get_minion_pools(ctxt)

    def get_minion_pool(self, ctxt, minion_pool_id):
        return self._rpc_client.get_minion_pool(ctxt, minion_pool_id)

    def allocate_minion_pool(self, ctxt, minion_pool_id):
        return self._rpc_client.allocate_minion_pool(
            ctxt, minion_pool_id)

    def healthcheck_minion_pool(self, ctxt, minion_pool_id):
        return self._rpc_client.healthcheck_minion_pool(
            ctxt, minion_pool_id)

    def deallocate_minion_pool(self, ctxt, minion_pool_id, force=False):
        return self._rpc_client.deallocate_minion_pool(
            ctxt, minion_pool_id, force=force)
