# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_config import cfg
import oslo_messaging as messaging

from coriolis import rpc

VERSION = "1.0"

scheduler_opts = [
    cfg.IntOpt("minion_mananger_rpc_timeout",
               help="Number of seconds until RPC calls to the "
                    "minion manager timeout.")
]

CONF = cfg.CONF
CONF.register_opts(scheduler_opts, 'minion_manager')


class MinionManagerClient(object):
    def __init__(self, timeout=None):
        target = messaging.Target(topic='coriolis_minion_manager', version=VERSION)
        if timeout is None:
            timeout = CONF.minion_manager.minion_mananger_rpc_timeout
        self._client = rpc.get_client(target, timeout=timeout)

    def add_minion_pool_progress_update(
            self, ctxt, minion_pool_id, total_steps, message):
        return self._client.call(
            ctxt, 'add_minion_pool_progress_update',
            minion_pool_id=minion_pool_id,
            total_steps=total_steps, message=message)

    def update_minion_pool_progress_update(
            self, ctxt, minion_pool_id, step, total_steps, message):
        return self._client.call(
            ctxt, 'update_minion_pool_progress_update',
            minion_pool_id=minion_pool_id,
            step=step, total_steps=total_steps, message=message)

    def get_minion_pool_progress_step(self, ctxt, minion_pool_id):
        return self._client.call(
            ctxt, 'get_minion_pool_progress_step',
            minion_pool_id=minion_pool_id)

    def add_minion_pool_event(self, ctxt, minion_pool_id, level, message):
        return self._client.call(
            ctxt, 'add_minion_pool_event', minion_pool_id=minion_pool_id,
            level=level, message=message)

    def get_diagnostics(self, ctxt):
        return self._client.call(ctxt, 'get_diagnostics')

    def validate_minion_pool_selections_for_action(self, ctxt, action):
        return self._client.call(
            ctxt, 'validate_minion_pool_selections_for_action',
            action=action)

    def allocate_minion_machines_for_action(
            self, ctxt, action, include_transfer_minions=True,
            include_osmorphing_minions=True):
        return self._client.call(
            ctxt, 'allocate_minion_machines_for_action', action=action,
            include_transfer_minions=include_transfer_minions,
            include_osmorphing_minions=include_osmorphing_minions)

    def deallocate_minion_machines_for_action(self, ctxt, action):
        return self._client.call(
            ctxt, 'deallocate_minion_machines_for_action', action=action)

    def create_minion_pool(
            self, ctxt, name, endpoint_id, pool_platform, pool_os_type,
            environment_options, minimum_minions, maximum_minions,
            minion_max_idle_time, minion_retention_strategy, notes=None,
            skip_allocation=False):
        return self._client.call(
            ctxt, 'create_minion_pool', name=name, endpoint_id=endpoint_id,
            pool_platform=pool_platform, pool_os_type=pool_os_type,
            environment_options=environment_options,
            minimum_minions=minimum_minions,
            maximum_minions=maximum_minions,
            minion_max_idle_time=minion_max_idle_time,
            minion_retention_strategy=minion_retention_strategy,
            notes=notes, skip_allocation=skip_allocation)

    def set_up_shared_minion_pool_resources(self, ctxt, minion_pool_id):
        return self._client.call(
            ctxt, "set_up_shared_minion_pool_resources",
            minion_pool_id=minion_pool_id)

    def tear_down_shared_minion_pool_resources(
            self, ctxt, minion_pool_id, force=False):
        return self._client.call(
            ctxt, "tear_down_shared_minion_pool_resources",
            minion_pool_id=minion_pool_id, force=force)

    def allocate_minion_pool(self, ctxt, minion_pool_id):
        return self._client.call(
            ctxt, "allocate_minion_pool",
            minion_pool_id=minion_pool_id)

    def healthcheck_minion_pool(self, ctxt, minion_pool_id):
        return self._client.call(
            ctxt, "healthcheck_minion_pool",
            minion_pool_id=minion_pool_id)

    def deallocate_minion_pool(
            self, ctxt, minion_pool_id, force=False):
        return self._client.call(
            ctxt, "deallocate_minion_pool",
            minion_pool_id=minion_pool_id,
            force=force)

    def get_minion_pools(self, ctxt):
        return self._client.call(ctxt, 'get_minion_pools')

    def get_minion_pool(self, ctxt, minion_pool_id):
        return self._client.call(
            ctxt, 'get_minion_pool', minion_pool_id=minion_pool_id)

    def update_minion_pool(self, ctxt, minion_pool_id, updated_values):
        return self._client.call(
            ctxt, 'update_minion_pool',
            minion_pool_id=minion_pool_id, updated_values=updated_values)

    def delete_minion_pool(self, ctxt, minion_pool_id):
        return self._client.call(
            ctxt, 'delete_minion_pool', minion_pool_id=minion_pool_id)

    def get_minion_pool_lifecycle_executions(
            self, ctxt, minion_pool_id, include_tasks=False):
        return self._client.call(
            ctxt, 'get_minion_pool_lifecycle_executions',
            minion_pool_id=minion_pool_id, include_tasks=include_tasks)

    def get_minion_pool_lifecycle_execution(
            self, ctxt, minion_pool_id, execution_id):
        return self._client.call(
            ctxt, 'get_minion_pool_lifecycle_execution',
            minion_pool_id=minion_pool_id, execution_id=execution_id)

    def delete_minion_pool_lifecycle_execution(
            self, ctxt, minion_pool_id, execution_id):
        return self._client.call(
            ctxt, 'delete_minion_pool_lifecycle_execution',
            minion_pool_id=minion_pool_id, execution_id=execution_id)

    def cancel_minion_pool_lifecycle_execution(
            self, ctxt, minion_pool_id, execution_id, force):
        return self._client.call(
            ctxt, 'cancel_minion_pool_lifecycle_execution',
            minion_pool_id=minion_pool_id, execution_id=execution_id,
            force=force)

    def get_endpoint_source_minion_pool_options(
            self, ctxt, endpoint_id, env, option_names):
        return self._client.call(
            ctxt, 'get_endpoint_source_minion_pool_options',
            endpoint_id=endpoint_id, env=env, option_names=option_names)

    def get_endpoint_destination_minion_pool_options(
            self, ctxt, endpoint_id, env, option_names):
        return self._client.call(
            ctxt, 'get_endpoint_destination_minion_pool_options',
            endpoint_id=endpoint_id, env=env, option_names=option_names)

    def validate_endpoint_source_minion_pool_options(
            self, ctxt, endpoint_id, pool_environment):
        return self._client.call(
            ctxt, 'validate_endpoint_source_minion_pool_options',
            endpoint_id=endpoint_id, pool_environment=pool_environment)

    def validate_endpoint_destination_minion_pool_options(
            self, ctxt, endpoint_id, pool_environment):
        return self._client.call(
            ctxt, 'validate_endpoint_destination_minion_pool_options',
            endpoint_id=endpoint_id, pool_environment=pool_environment)
