# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging

from coriolis import constants
from coriolis import events
from coriolis import rpc


VERSION = "1.0"
LOG = logging.getLogger(__name__)

MINION_MANAGER_OPTS = [
    cfg.IntOpt("minion_mananger_rpc_timeout",
               help="Number of seconds until RPC calls to the "
                    "minion manager timeout.")
]

CONF = cfg.CONF
CONF.register_opts(MINION_MANAGER_OPTS, 'minion_manager')


class MinionManagerClient(rpc.BaseRPCClient):

    def __init__(self, timeout=None):
        target = messaging.Target(
            topic='coriolis_minion_manager', version=VERSION)
        if timeout is None:
            timeout = CONF.minion_manager.minion_mananger_rpc_timeout
        super(MinionManagerClient, self).__init__(
            target, timeout=timeout)

    def add_minion_pool_progress_update(
            self, ctxt, minion_pool_id, message, initial_step=0,
            total_steps=0, return_event=False):
        operation = self._cast
        if return_event:
            operation = self._call
        return operation(
            ctxt, 'add_minion_pool_progress_update',
            minion_pool_id=minion_pool_id, message=message,
            initial_step=initial_step, total_steps=total_steps)

    def update_minion_pool_progress_update(
            self, ctxt, minion_pool_id, progress_update_index,
            new_current_step, new_total_steps=None, new_message=None):
        self._cast(
            ctxt, 'update_minion_pool_progress_update',
            minion_pool_id=minion_pool_id,
            progress_update_index=progress_update_index,
            new_current_step=new_current_step,
            new_total_steps=new_total_steps, new_message=new_message)

    def add_minion_pool_event(self, ctxt, minion_pool_id, level, message):
        return self._cast(
            ctxt, 'add_minion_pool_event', minion_pool_id=minion_pool_id,
            level=level, message=message)

    def get_diagnostics(self, ctxt):
        return self._call(ctxt, 'get_diagnostics')

    def validate_minion_pool_selections_for_action(self, ctxt, action):
        return self._call(
            ctxt, 'validate_minion_pool_selections_for_action',
            action=action)

    def allocate_minion_machines_for_replica(
            self, ctxt, replica):
        return self._cast(
            ctxt, 'allocate_minion_machines_for_replica', replica=replica)

    def allocate_minion_machines_for_migration(
            self, ctxt, migration, include_transfer_minions=True,
            include_osmorphing_minions=True):
        return self._cast(
            ctxt, 'allocate_minion_machines_for_migration',
            migration=migration,
            include_transfer_minions=include_transfer_minions,
            include_osmorphing_minions=include_osmorphing_minions)

    def deallocate_minion_machine(self, ctxt, minion_machine_id):
        return self._cast(
            ctxt, 'deallocate_minion_machine',
            minion_machine_id=minion_machine_id)

    def deallocate_minion_machines_for_action(self, ctxt, action_id):
        return self._cast(
            ctxt, 'deallocate_minion_machines_for_action',
            action_id=action_id)

    def create_minion_pool(
            self, ctxt, name, endpoint_id, pool_platform, pool_os_type,
            environment_options, minimum_minions, maximum_minions,
            minion_max_idle_time, minion_retention_strategy, notes=None,
            skip_allocation=False):
        return self._call(
            ctxt, 'create_minion_pool', name=name, endpoint_id=endpoint_id,
            pool_platform=pool_platform, pool_os_type=pool_os_type,
            environment_options=environment_options,
            minimum_minions=minimum_minions,
            maximum_minions=maximum_minions,
            minion_max_idle_time=minion_max_idle_time,
            minion_retention_strategy=minion_retention_strategy,
            notes=notes, skip_allocation=skip_allocation)

    def set_up_shared_minion_pool_resources(self, ctxt, minion_pool_id):
        return self._call(
            ctxt, "set_up_shared_minion_pool_resources",
            minion_pool_id=minion_pool_id)

    def tear_down_shared_minion_pool_resources(
            self, ctxt, minion_pool_id, force=False):
        return self._call(
            ctxt, "tear_down_shared_minion_pool_resources",
            minion_pool_id=minion_pool_id, force=force)

    def allocate_minion_pool(self, ctxt, minion_pool_id):
        return self._call(
            ctxt, "allocate_minion_pool",
            minion_pool_id=minion_pool_id)

    def refresh_minion_pool(self, ctxt, minion_pool_id):
        return self._call(
            ctxt, "refresh_minion_pool",
            minion_pool_id=minion_pool_id)

    def deallocate_minion_pool(
            self, ctxt, minion_pool_id, force=False):
        return self._call(
            ctxt, "deallocate_minion_pool",
            minion_pool_id=minion_pool_id,
            force=force)

    def get_minion_pools(self, ctxt):
        return self._call(ctxt, 'get_minion_pools')

    def get_minion_pool(self, ctxt, minion_pool_id):
        return self._call(
            ctxt, 'get_minion_pool', minion_pool_id=minion_pool_id)

    def update_minion_pool(self, ctxt, minion_pool_id, updated_values):
        return self._call(
            ctxt, 'update_minion_pool',
            minion_pool_id=minion_pool_id, updated_values=updated_values)

    def delete_minion_pool(self, ctxt, minion_pool_id):
        return self._call(
            ctxt, 'delete_minion_pool', minion_pool_id=minion_pool_id)

    def get_endpoint_source_minion_pool_options(
            self, ctxt, endpoint_id, env, option_names):
        return self._call(
            ctxt, 'get_endpoint_source_minion_pool_options',
            endpoint_id=endpoint_id, env=env, option_names=option_names)

    def get_endpoint_destination_minion_pool_options(
            self, ctxt, endpoint_id, env, option_names):
        return self._call(
            ctxt, 'get_endpoint_destination_minion_pool_options',
            endpoint_id=endpoint_id, env=env, option_names=option_names)

    def validate_endpoint_source_minion_pool_options(
            self, ctxt, endpoint_id, pool_environment):
        return self._call(
            ctxt, 'validate_endpoint_source_minion_pool_options',
            endpoint_id=endpoint_id, pool_environment=pool_environment)

    def validate_endpoint_destination_minion_pool_options(
            self, ctxt, endpoint_id, pool_environment):
        return self._call(
            ctxt, 'validate_endpoint_destination_minion_pool_options',
            endpoint_id=endpoint_id, pool_environment=pool_environment)


class MinionManagerPoolRpcEventHandler(events.BaseEventHandler):
    def __init__(self, ctxt, pool_id):
        self._ctxt = ctxt
        self._pool_id = pool_id
        self._rpc_minion_manager_client_instance = None

    @property
    def _rpc_minion_manager_client(self):
        # NOTE(aznashwan): it is unsafe to fork processes with pre-instantiated
        # oslo_messaging clients as the underlying eventlet thread queues will
        # be invalidated.
        if self._rpc_minion_manager_client_instance is None:
            self._rpc_minion_manager_client_instance = MinionManagerClient()
        return self._rpc_minion_manager_client_instance

    @classmethod
    def get_progress_update_identifier(self, progress_update):
        return progress_update['index']

    def add_progress_update(
            self, message, initial_step=0, total_steps=0, return_event=False):
        LOG.info(
            "Sending progress update for pool '%s' to minion manager : %s",
            self._pool_id, message)
        return self._rpc_minion_manager_client.add_minion_pool_progress_update(
            self._ctxt, self._pool_id, message, initial_step=initial_step,
            total_steps=total_steps, return_event=return_event)

    def update_progress_update(
            self, update_identifier, new_current_step,
            new_total_steps=None, new_message=None):
        LOG.info(
            "Updating progress update '%s' for pool '%s' with new step %s",
            update_identifier, self._pool_id, new_current_step)
        self._rpc_minion_manager_client.update_minion_pool_progress_update(
            self._ctxt, self._pool_id, update_identifier, new_current_step,
            new_total_steps=new_total_steps, new_message=new_message)

    def add_event(self, message, level=constants.TASK_EVENT_INFO):
        self._rpc_minion_manager_client.add_minion_pool_event(
            self._ctxt, self._pool_id, level, message)
