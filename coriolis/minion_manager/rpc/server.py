# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import contextlib
import functools
import itertools
import uuid

from oslo_concurrency import lockutils
from oslo_config import cfg
from oslo_log import log as logging

from coriolis import constants
from coriolis import exception
from coriolis import utils
from coriolis.conductor.rpc import client as rpc_conductor_client
from coriolis.db import api as db_api
from coriolis.db.sqlalchemy import models


VERSION = "1.0"

LOG = logging.getLogger(__name__)


MINION_MANAGER_OPTS = []

CONF = cfg.CONF
CONF.register_opts(MINION_MANAGER_OPTS, 'minion_manager')


def minion_pool_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, minion_pool_id, *args, **kwargs):
        @lockutils.synchronized(
            constants.MINION_POOL_LOCK_NAME_FORMAT % minion_pool_id,
            external=True)
        def inner():
            return func(self, ctxt, minion_pool_id, *args, **kwargs)
        return inner()
    return wrapper


class MinionManagerServerEndpoint(object):
    def __init__(self):
        self._rpc_conductor_client = rpc_conductor_client.ConductorClient()

    def get_diagnostics(self, ctxt):
        return utils.get_diagnostics_info()

    @staticmethod
    def _update_minion_pool_status_for_finished_execution(
            ctxt, execution, new_execution_status):
        # status map if execution is active:
        stat_map = {
            constants.EXECUTION_TYPE_MINION_POOL_ALLOCATE_MINIONS:
                constants.MINION_POOL_STATUS_ALLOCATING,
            constants.EXECUTION_TYPE_MINION_POOL_DEALLOCATE_MINIONS:
                constants.MINION_POOL_STATUS_DEALLOCATING,
            constants.EXECUTION_TYPE_MINION_POOL_SET_UP_SHARED_RESOURCES:
                constants.MINION_POOL_STATUS_INITIALIZING,
            constants.EXECUTION_TYPE_MINION_POOL_TEAR_DOWN_SHARED_RESOURCES:
                constants.MINION_POOL_STATUS_UNINITIALIZING}
        if new_execution_status == constants.EXECUTION_STATUS_COMPLETED:
            stat_map = {
                constants.EXECUTION_TYPE_MINION_POOL_ALLOCATE_MINIONS:
                    constants.MINION_POOL_STATUS_ALLOCATED,
                constants.EXECUTION_TYPE_MINION_POOL_DEALLOCATE_MINIONS:
                    constants.MINION_POOL_STATUS_DEALLOCATED,
                constants.EXECUTION_TYPE_MINION_POOL_SET_UP_SHARED_RESOURCES:
                    constants.MINION_POOL_STATUS_DEALLOCATED,
                constants.EXECUTION_TYPE_MINION_POOL_TEAR_DOWN_SHARED_RESOURCES:
                    constants.MINION_POOL_STATUS_UNINITIALIZED}
        elif new_execution_status in constants.FINALIZED_TASK_STATUSES:
            stat_map = {
                constants.EXECUTION_TYPE_MINION_POOL_ALLOCATE_MINIONS:
                    constants.MINION_POOL_STATUS_DEALLOCATED,
                constants.EXECUTION_TYPE_MINION_POOL_DEALLOCATE_MINIONS:
                    constants.MINION_POOL_STATUS_ALLOCATED,
                constants.EXECUTION_TYPE_MINION_POOL_SET_UP_SHARED_RESOURCES:
                    constants.MINION_POOL_STATUS_UNINITIALIZED,
                constants.EXECUTION_TYPE_MINION_POOL_TEAR_DOWN_SHARED_RESOURCES:
                    constants.MINION_POOL_STATUS_UNINITIALIZED}
        final_pool_status = stat_map.get(execution.type)
        if not final_pool_status:
            LOG.error(
                "Could not determine pool status following transition of "
                "execution '%s' (type '%s') to status '%s'. Presuming error "
                "has occured. Marking piil as error'd.",
                execution.id, execution.type, new_execution_status)
            final_pool_status = constants.MINION_POOL_STATUS_ERROR

        LOG.info(
            "Marking minion pool '%s' status as '%s' in the DB following the "
            "transition of execution '%s' (type '%s') to status '%s'.",
            execution.action_id, final_pool_status, execution.id,
            execution.type, new_execution_status)
        db_api.set_minion_pool_status(
            ctxt, execution.action_id, final_pool_status)

    def validate_minion_pool_selections_for_action(self, ctxt, action_id):
        action = db_api.get_action(ctxt, action_id)
        minion_pools = {
            pool.id: pool
            for pool in db_api.get_minion_pools(
                ctxt, include_machines=False, to_dict=False)}
        def _get_pool(pool_id):
            pool = minion_pools.get(pool_id)
            if not pool:
                raise exception.NotFound(
                    "Could not find minion pool with ID '%s'." % pool_id)
            return pool

        if action.origin_minion_pool_id:
            origin_pool = _get_pool(action.origin_minion_pool_id)
            if origin_pool.endpoint_id != action.origin_endpoint_id:
                raise exception.InvalidMinionPoolSelection(
                    "The selected origin minion pool ('%s') belongs to a "
                    "different Coriolis endpoint ('%s') than the requested "
                    "origin endpoint ('%s')" % (
                        action.origin_minion_pool_id,
                        origin_pool.endpoint_id,
                        action.origin_endpoint_id))
            if origin_pool.pool_platform != constants.PROVIDER_PLATFORM_SOURCE:
                raise exception.InvalidMinionPoolSelection(
                    "The selected origin minion pool ('%s') is configured as a"
                    " '%s' pool. The pool must be of type %s to be used for "
                    "data exports." % (
                        action.origin_minion_pool_id,
                        origin_pool.pool_platform,
                        constants.PROVIDER_PLATFORM_SOURCE))
            if origin_pool.pool_os_type != constants.OS_TYPE_LINUX:
                raise exception.InvalidMinionPoolSelection(
                    "The selected origin minion pool ('%s') is of OS type '%s'"
                    " instead of the Linux OS type required for a source "
                    "transfer minion pool." % (
                        action.origin_minion_pool_id,
                        origin_pool.pool_os_type))
            LOG.debug(
                "Successfully validated compatibility of origin minion pool "
                "'%s' for use with action '%s'." % (
                    action.origin_minion_pool_id, action.id))

        if action.destination_minion_pool_id:
            destination_pool = _get_pool(action.destination_minion_pool_id)
            if destination_pool.endpoint_id != (
                    action.destination_endpoint_id):
                raise exception.InvalidMinionPoolSelection(
                    "The selected destination minion pool ('%s') belongs to a "
                    "different Coriolis endpoint ('%s') than the requested "
                    "destination endpoint ('%s')" % (
                        action.destination_minion_pool_id,
                        destination_pool.endpoint_id,
                        action.destination_endpoint_id))
            if destination_pool.pool_platform != (
                    constants.PROVIDER_PLATFORM_DESTINATION):
                raise exception.InvalidMinionPoolSelection(
                    "The selected destination minion pool ('%s') is configured"
                    " as a '%s'. The pool must be of type %s to be used for "
                    "data imports." % (
                        action.destination_minion_pool_id,
                        destination_pool.pool_platform,
                        constants.PROVIDER_PLATFORM_DESTINATION))
            if destination_pool.pool_os_type != constants.OS_TYPE_LINUX:
                raise exception.InvalidMinionPoolSelection(
                    "The selected destination minion pool ('%s') is of OS type"
                    " '%s' instead of the Linux OS type required for a source "
                    "transfer minion pool." % (
                        action.destination_minion_pool_id,
                        destination_pool.pool_os_type))
            LOG.debug(
                "Successfully validated compatibility of destination minion "
                "pool '%s' for use with action '%s'." % (
                    action.origin_minion_pool_id, action.id))

        if action.instance_osmorphing_minion_pool_mappings:
            osmorphing_pool_mappings = {
                instance_id: pool_id
                for (instance_id, pool_id) in (
                    action.instance_osmorphing_minion_pool_mappings.items())
                if pool_id}
            for (instance, pool_id) in osmorphing_pool_mappings.items():
                osmorphing_pool = _get_pool(pool_id)
                if osmorphing_pool.endpoint_id != (
                        action.destination_endpoint_id):
                    raise exception.InvalidMinionPoolSelection(
                        "The selected OSMorphing minion pool for instance '%s'"
                        " ('%s') belongs to a different Coriolis endpoint "
                        "('%s') than the destination endpoint ('%s')" % (
                            instance, pool_id,
                            osmorphing_pool.endpoint_id,
                            action.destination_endpoint_id))
                if osmorphing_pool.pool_platform != (
                        constants.PROVIDER_PLATFORM_DESTINATION):
                    raise exception.InvalidMinionPoolSelection(
                        "The selected OSMorphing minion pool for instance '%s'"
                        "  ('%s') is configured as a '%s' pool. The pool must "
                        "be of type %s to be used for OSMorphing." % (
                            instance, pool_id,
                            osmorphing_pool.pool_platform,
                            constants.PROVIDER_PLATFORM_DESTINATION))
                LOG.debug(
                    "Successfully validated compatibility of destination "
                    "minion pool '%s' for use as OSMorphing minion for "
                    "instance '%s' during action '%s'." % (
                        action.origin_minion_pool_id, instance, action.id))

    def allocate_minion_machines_for_action(
            self, ctxt, action_id, include_transfer_minions=True,
            include_osmorphing_minions=True):
        """ Returns a dict of the form:
        {
            "instance_id": {
                "source_minion": <source minion properties>,
                "target_minion": <target minion properties>,
                "osmorphing_minion": <osmorphing minion properties>
            }
        }
        """
        action = db_api.get_action(ctxt, action_id)
        instance_machine_allocations = {
            instance: {} for instance in action.instances}

        minion_pool_ids = set()
        if action.origin_minion_pool_id:
            minion_pool_ids.add(action.origin_minion_pool_id)
        if action.destination_minion_pool_id:
            minion_pool_ids.add(action.destination_minion_pool_id)
        if action.instance_osmorphing_minion_pool_mappings:
            minion_pool_ids = minion_pool_ids.union(set(
                action.instance_osmorphing_minion_pool_mappings.values()))
        if None in minion_pool_ids:
            minion_pool_ids.remove(None)

        if not minion_pool_ids:
            LOG.debug(
                "No minion pool settings found for action '%s'. "
                "Skipping minion machine allocations." % (
                    action.id))
            return instance_machine_allocations

        LOG.debug(
            "All minion pool selections for action '%s': %s",
            action.id, minion_pool_ids)

        def _select_machine(minion_pool, exclude=None):
            if not minion_pool.minion_machines:
                raise exception.InvalidMinionPoolSelection(
                    "Minion pool with ID '%s' has no machines defined." % (
                        minion_pool.id))
            selected_machine = None
            for machine in minion_pool.minion_machines:
                if exclude and machine.id in exclude:
                    LOG.debug(
                        "Excluding minion machine '%s' from search.",
                        machine.id)
                    continue
                if machine.status != constants.MINION_MACHINE_STATUS_AVAILABLE:
                    LOG.debug(
                        "Minion machine with ID '%s' is in status '%s' "
                        "instead of '%s'. Skipping.", machine.id,
                        machine.status,
                        constants.MINION_MACHINE_STATUS_AVAILABLE)
                    continue
                selected_machine = machine
                break
            if not selected_machine:
                raise exception.InvalidMinionPoolSelection(
                    "There are no more available minion machines within minion"
                    " pool with ID '%s' (excluding the following ones already "
                    "planned for this transfer: %s). Please ensure that the "
                    "minion pool has enough minion machines allocated and "
                    "available (i.e. not being used for other operations) "
                    "to satisfy the number of VMs required by the Migration or"
                    " Replica." % (
                        minion_pool.id, exclude))
            return selected_machine

        osmorphing_pool_map = (
            action.instance_osmorphing_minion_pool_mappings)
        with contextlib.ExitStack() as stack:
            _ = [
                stack.enter_context(
                    lockutils.lock(
                        constants.MINION_POOL_LOCK_NAME_FORMAT % pool_id,
                        external=True))
                for pool_id in minion_pool_ids]

            minion_pools = db_api.get_minion_pools(
                ctxt, include_machines=True, to_dict=False)
            minion_pool_id_mappings = {
                pool.id: pool for pool in minion_pools
                if pool.id in minion_pool_ids}

            missing_pools = [
                pool_id for pool_id in minion_pool_ids
                if pool_id not in minion_pool_id_mappings]
            if missing_pools:
                raise exception.InvalidMinionPoolSelection(
                    "The following minion pools could not be found: %s" % (
                        missing_pools))

            unallocated_pools = {
                pool_id: pool.pool_status
                for (pool_id, pool) in minion_pool_id_mappings.items()
                if pool.pool_status != constants.MINION_POOL_STATUS_ALLOCATED}
            if unallocated_pools:
                raise exception.InvalidMinionPoolSelection(
                    "The following minion pools have not had their machines "
                    "allocated and thus cannot be used: %s" % (
                        unallocated_pools))

            allocated_source_machine_ids = set()
            allocated_target_machine_ids = set()
            allocated_osmorphing_machine_ids = set()
            for instance in action.instances:

                if include_transfer_minions:
                    if action.origin_minion_pool_id:
                        origin_pool = minion_pool_id_mappings[
                            action.origin_minion_pool_id]
                        machine = _select_machine(
                            origin_pool, exclude=allocated_source_machine_ids)
                        allocated_source_machine_ids.add(machine.id)
                        instance_machine_allocations[
                            instance]['source_minion'] = machine
                        LOG.debug(
                            "Selected minion machine '%s' for source-side "
                            "syncing of instance '%s' as part of transfer "
                            "action '%s'.", machine.id, instance, action.id)

                    if action.destination_minion_pool_id:
                        dest_pool = minion_pool_id_mappings[
                            action.destination_minion_pool_id]
                        machine = _select_machine(
                            dest_pool, exclude=allocated_target_machine_ids)
                        allocated_target_machine_ids.add(machine.id)
                        instance_machine_allocations[
                            instance]['target_minion'] = machine
                        LOG.debug(
                            "Selected minion machine '%s' for target-side "
                            "syncing of instance '%s' as part of transfer "
                            "action '%s'.", machine.id, instance, action.id)

                if include_osmorphing_minions:
                    if instance not in osmorphing_pool_map:
                        LOG.debug(
                            "Instance '%s' is not listed in the OSMorphing "
                            "minion pool mappings for action '%s'." % (
                                instance, action.id))
                    elif osmorphing_pool_map[instance] is None:
                        LOG.debug(
                            "OSMorphing pool ID for instance '%s' is "
                            "None in action '%s'. Ignoring." % (
                                instance, action.id))
                    else:
                        osmorphing_pool_id = osmorphing_pool_map[instance]
                        # if the selected target and OSMorphing pools
                        # are the same, reuse the same worker:
                        ima = instance_machine_allocations[instance]
                        if osmorphing_pool_id == (
                                action.destination_minion_pool_id) and (
                                    'target_minion' in ima):
                            allocated_target_machine = ima[
                                'target_minion']
                            LOG.debug(
                                "Reusing disk sync minion '%s' for the "
                                "OSMorphing of instance '%s' as port of "
                                "transfer action '%s'",
                                allocated_target_machine.id, instance,
                                action.id)
                            instance_machine_allocations[
                                instance]['osmorphing_minion'] = (
                                    allocated_target_machine)
                        # else, allocate a new minion from the selected pool:
                        else:
                            osmorphing_pool = minion_pool_id_mappings[
                                osmorphing_pool_id]
                            machine = _select_machine(
                                osmorphing_pool,
                                exclude=allocated_osmorphing_machine_ids)
                            allocated_osmorphing_machine_ids.add(machine.id)
                            instance_machine_allocations[
                                instance]['osmorphing_minion'] = machine
                            LOG.debug(
                                "Selected minion machine '%s' for OSMorphing "
                                " of instance '%s' as part of transfer "
                                "action '%s'.",
                                machine.id, instance, action.id)

            # mark the selected machines as allocated:
            all_machine_ids = set(itertools.chain(
                allocated_source_machine_ids,
                allocated_target_machine_ids,
                allocated_osmorphing_machine_ids))
            db_api.set_minion_machines_allocation_statuses(
                ctxt, all_machine_ids, action.id,
                constants.MINION_MACHINE_STATUS_ALLOCATED)

        # filter out redundancies:
        instance_machine_allocations = {
            instance: allocations
            for (instance, allocations) in instance_machine_allocations.items()
            if allocations}

        LOG.debug(
            "Allocated the following minion machines for action '%s': %s",
            action.id, {
                instance: {
                    typ: machine.id
                    for (typ, machine) in allocation.items()}
                for (instance, allocation) in instance_machine_allocations.items()})
        return instance_machine_allocations

    def deallocate_minion_machines_for_action(self, ctxt, action_id):
        action = db_api.get_action(ctxt, action_id)
        minion_pool_ids = set()
        if action.origin_minion_pool_id:
            minion_pool_ids.add(action.origin_minion_pool_id)
        if action.destination_minion_pool_id:
            minion_pool_ids.add(action.destination_minion_pool_id)
        if action.instance_osmorphing_minion_pool_mappings:
            minion_pool_ids = minion_pool_ids.union(set(
                action.instance_osmorphing_minion_pool_mappings.values()))
        if None in minion_pool_ids:
            minion_pool_ids.remove(None)

        if not minion_pool_ids:
            LOG.debug(
                "No minion pools seem to have been used for action with "
                "base_id '%s'. Skipping minion machine deallocation.",
                action.base_id)
        else:
            LOG.debug(
                "Attempting to deallocate all minion pool machine selections "
                "for action '%s'. Afferent pools are: %s",
                action.base_id, minion_pool_ids)

            with contextlib.ExitStack() as stack:
                _ = [
                    stack.enter_context(
                        lockutils.lock(
                            constants.MINION_POOL_LOCK_NAME_FORMAT % pool_id,
                            external=True))
                    for pool_id in minion_pool_ids]

                minion_machines = db_api.get_minion_machines(
                    ctxt, allocated_action_id=action.base_id)
                machine_ids = [m.id for m in minion_machines]
                if machine_ids:
                    LOG.info(
                        "Releasing the following minion machines for "
                        "action '%s': %s", action.base_id, machine_ids)
                    db_api.set_minion_machines_allocation_statuses(
                        ctxt, machine_ids, None,
                        constants.MINION_MACHINE_STATUS_AVAILABLE)
                else:
                    LOG.debug(
                        "No minion machines were found to be associated "
                        "with action with base_id '%s'.", action.base_id)

    def create_minion_pool(
            self, ctxt, name, endpoint_id, pool_platform, pool_os_type,
            environment_options, minimum_minions, maximum_minions,
            minion_max_idle_time, minion_retention_strategy, notes=None):
        endpoint = db_api.get_endpoint(ctxt, endpoint_id)

        minion_pool = models.MinionPool()
        minion_pool.id = str(uuid.uuid4())
        minion_pool.pool_name = name
        minion_pool.notes = notes
        minion_pool.pool_platform = pool_platform
        minion_pool.pool_os_type = pool_os_type
        minion_pool.endpoint_id = endpoint_id
        minion_pool.environment_options = environment_options
        minion_pool.pool_status = constants.MINION_POOL_STATUS_UNINITIALIZED
        minion_pool.minimum_minions = minimum_minions
        minion_pool.maximum_minions = maximum_minions
        minion_pool.minion_max_idle_time = minion_max_idle_time
        minion_pool.minion_retention_strategy = minion_retention_strategy

        db_api.add_minion_pool(ctxt, minion_pool)
        return self.get_minion_pool(ctxt, minion_pool.id)

    def get_minion_pools(self, ctxt, include_machines=True):
        return db_api.get_minion_pools(ctxt, include_machines=include_machines)

    def _get_minion_pool(
            self, ctxt, minion_pool_id, include_machines=True):
        minion_pool = db_api.get_minion_pool(
            ctxt, minion_pool_id, include_machines=include_machines)
        if not minion_pool:
            raise exception.NotFound(
                "Minion pool with ID '%s' not found." % minion_pool_id)
        return minion_pool

    # @minion_pool_synchronized
    # def set_up_shared_minion_pool_resources(self, ctxt, minion_pool_id):
    #     LOG.info(
    #         "Attempting to set up shared resources for Minion Pool '%s'.",
    #         minion_pool_id)
    #     minion_pool = db_api.get_minion_pool_lifecycle(
    #         ctxt, minion_pool_id, include_tasks_executions=False,
    #         include_machines=False)
    #     if minion_pool.pool_status != constants.MINION_POOL_STATUS_UNINITIALIZED:
    #         raise exception.InvalidMinionPoolState(
    #             "Minion Pool '%s' cannot have shared resources set up as it "
    #             "is in '%s' state instead of the expected %s."% (
    #                 minion_pool_id, minion_pool.pool_status,
    #                 constants.MINION_POOL_STATUS_UNINITIALIZED))

    #     execution = models.TasksExecution()
    #     execution.id = str(uuid.uuid4())
    #     execution.action = minion_pool
    #     execution.status = constants.EXECUTION_STATUS_UNEXECUTED
    #     execution.type = (
    #         constants.EXECUTION_TYPE_MINION_POOL_SET_UP_SHARED_RESOURCES)

    #     minion_pool.info[minion_pool_id] = {
    #         "pool_os_type": minion_pool.pool_os_type,
    #         "pool_identifier": minion_pool.id,
    #         # TODO(aznashwan): remove redundancy once transfer
    #         # action DB models have been overhauled:
    #         "pool_environment_options": minion_pool.source_environment}

    #     validate_task_type = (
    #         constants.TASK_TYPE_VALIDATE_DESTINATION_MINION_POOL_OPTIONS)
    #     set_up_task_type = (
    #         constants.TASK_TYPE_SET_UP_DESTINATION_POOL_SHARED_RESOURCES)
    #     if minion_pool.pool_platform == constants.PROVIDER_PLATFORM_SOURCE:
    #         validate_task_type = (
    #             constants.TASK_TYPE_VALIDATE_SOURCE_MINION_POOL_OPTIONS)
    #         set_up_task_type = (
    #             constants.TASK_TYPE_SET_UP_SOURCE_POOL_SHARED_RESOURCES)

    #     validate_pool_options_task = self._create_task(
    #         minion_pool.id, validate_task_type, execution)

    #     setup_pool_resources_task = self._create_task(
    #         minion_pool.id,
    #         set_up_task_type,
    #         execution,
    #         depends_on=[validate_pool_options_task.id])

    #     self._check_execution_tasks_sanity(execution, minion_pool.info)

    #     # update the action info for the pool's instance:
    #     db_api.update_transfer_action_info_for_instance(
    #         ctxt, minion_pool.id, minion_pool.id,
    #         minion_pool.info[minion_pool.id])

    #     # add new execution to DB:
    #     db_api.add_minion_pool_lifecycle_execution(ctxt, execution)
    #     LOG.info(
    #         "Minion pool shared resource creation execution created: %s",
    #         execution.id)

    #     self._begin_tasks(ctxt, minion_pool, execution)
    #     db_api.set_minion_pool_lifecycle_status(
    #         ctxt, minion_pool.id, constants.MINION_POOL_STATUS_INITIALIZING)

    #     return self._get_minion_pool_lifecycle_execution(
    #         ctxt, minion_pool_id, execution.id).to_dict()

    # @minion_pool_synchronized
    # def tear_down_shared_minion_pool_resources(
    #         self, ctxt, minion_pool_id, force=False):
    #     minion_pool = db_api.get_minion_pool_lifecycle(
    #         ctxt, minion_pool_id, include_tasks_executions=False,
    #         include_machines=False)
    #     if minion_pool.pool_status != (
    #             constants.MINION_POOL_STATUS_DEALLOCATED) and not force:
    #         raise exception.InvalidMinionPoolState(
    #             "Minion Pool '%s' cannot have shared resources torn down as it"
    #             " is in '%s' state instead of the expected %s. "
    #             "Please use the force flag if you are certain you want "
    #             "to tear down the shared resources for this pool." % (
    #                 minion_pool_id, minion_pool.pool_status,
    #                 constants.MINION_POOL_STATUS_DEALLOCATED))

    #     LOG.info(
    #         "Attempting to tear down shared resources for Minion Pool '%s'.",
    #         minion_pool_id)

    #     execution = models.TasksExecution()
    #     execution.id = str(uuid.uuid4())
    #     execution.action = minion_pool
    #     execution.status = constants.EXECUTION_STATUS_UNEXECUTED
    #     execution.type = (
    #         constants.EXECUTION_TYPE_MINION_POOL_TEAR_DOWN_SHARED_RESOURCES)

    #     tear_down_task_type = (
    #         constants.TASK_TYPE_TEAR_DOWN_DESTINATION_POOL_SHARED_RESOURCES)
    #     if minion_pool.pool_platform == constants.PROVIDER_PLATFORM_SOURCE:
    #         tear_down_task_type = (
    #             constants.TASK_TYPE_TEAR_DOWN_SOURCE_POOL_SHARED_RESOURCES)

    #     self._create_task(
    #         minion_pool.id, tear_down_task_type, execution)

    #     self._check_execution_tasks_sanity(execution, minion_pool.info)

    #     # update the action info for the pool's instance:
    #     db_api.update_transfer_action_info_for_instance(
    #         ctxt, minion_pool.id, minion_pool.id,
    #         minion_pool.info[minion_pool.id])

    #     # add new execution to DB:
    #     db_api.add_minion_pool_lifecycle_execution(ctxt, execution)
    #     LOG.info(
    #         "Minion pool shared resource teardown execution created: %s",
    #         execution.id)

    #     self._begin_tasks(ctxt, minion_pool, execution)
    #     db_api.set_minion_pool_lifecycle_status(
    #         ctxt, minion_pool.id, constants.MINION_POOL_STATUS_UNINITIALIZING)

    #     return self._get_minion_pool_lifecycle_execution(
    #         ctxt, minion_pool_id, execution.id).to_dict()

    # @minion_pool_synchronized
    # def allocate_minion_pool_machines(self, ctxt, minion_pool_id):
    #     LOG.info("Attempting to allocate Minion Pool '%s'.", minion_pool_id)
    #     minion_pool = self._get_minion_pool(
    #         ctxt, minion_pool_id, include_tasks_executions=False,
    #         include_machines=True)
    #     if minion_pool.pool_status != constants.MINION_POOL_STATUS_DEALLOCATED:
    #         raise exception.InvalidMinionPoolState(
    #             "Minion machines for pool '%s' cannot be allocated as the pool"
    #             " is in '%s' state instead of the expected %s."% (
    #                 minion_pool_id, minion_pool.pool_status,
    #                 constants.MINION_POOL_STATUS_DEALLOCATED))

    #     execution = models.TasksExecution()
    #     execution.id = str(uuid.uuid4())
    #     execution.action = minion_pool
    #     execution.status = constants.EXECUTION_STATUS_UNEXECUTED
    #     execution.type = constants.EXECUTION_TYPE_MINION_POOL_ALLOCATE_MINIONS

    #     new_minion_machine_ids = [
    #         str(uuid.uuid4()) for _ in range(minion_pool.minimum_minions)]

    #     create_minion_task_type = (
    #         constants.TASK_TYPE_CREATE_DESTINATION_MINION_MACHINE)
    #     delete_minion_task_type = (
    #         constants.TASK_TYPE_DELETE_DESTINATION_MINION_MACHINE)
    #     if minion_pool.pool_platform == constants.PROVIDER_PLATFORM_SOURCE:
    #         create_minion_task_type = (
    #             constants.TASK_TYPE_CREATE_SOURCE_MINION_MACHINE)
    #         delete_minion_task_type = (
    #             constants.TASK_TYPE_DELETE_DESTINATION_MINION_MACHINE)

    #     for minion_machine_id in new_minion_machine_ids:
    #         minion_pool.info[minion_machine_id] = {
    #             "pool_identifier": minion_pool_id,
    #             "pool_os_type": minion_pool.pool_os_type,
    #             "pool_shared_resources": minion_pool.pool_shared_resources,
    #             "pool_environment_options": minion_pool.source_environment,
    #             # NOTE: we default this to an empty dict here to avoid possible
    #             # task info conflicts on the cleanup task below for minions
    #             # which were slower to deploy:
    #             "minion_provider_properties": {}}

    #         create_minion_task = self._create_task(
    #             minion_machine_id, create_minion_task_type, execution)

    #         self._create_task(
    #             minion_machine_id,
    #             delete_minion_task_type,
    #             execution, on_error_only=True,
    #             depends_on=[create_minion_task.id])

    #     self._check_execution_tasks_sanity(execution, minion_pool.info)

    #     # update the action info for all of the pool's minions:
    #     for minion_machine_id in new_minion_machine_ids:
    #         db_api.update_transfer_action_info_for_instance(
    #             ctxt, minion_pool.id, minion_machine_id,
    #             minion_pool.info[minion_machine_id])

    #     # add new execution to DB:
    #     db_api.add_minion_pool_lifecycle_execution(ctxt, execution)
    #     LOG.info("Minion pool allocation execution created: %s", execution.id)

    #     self._begin_tasks(ctxt, minion_pool, execution)
    #     db_api.set_minion_pool_lifecycle_status(
    #         ctxt, minion_pool.id, constants.MINION_POOL_STATUS_ALLOCATING)

    #     return self._get_minion_pool_lifecycle_execution(
    #         ctxt, minion_pool_id, execution.id).to_dict()

    # def _check_all_pool_minion_machines_available(self, minion_pool):
    #     if not minion_pool.minion_machines:
    #         LOG.debug(
    #             "Minion pool '%s' does not have any allocated machines.",
    #             minion_pool.id)
    #         return

    #     allocated_machine_statuses = {
    #         machine.id: machine.status
    #         for machine in minion_pool.minion_machines
    #         if machine.status != constants.MINION_MACHINE_STATUS_AVAILABLE}

    #     if allocated_machine_statuses:
    #         raise exception.InvalidMinionPoolState(
    #             "Minion pool with ID '%s' has one or more machines which are "
    #             "in-use or otherwise unmodifiable: %s" % (
    #                 minion_pool.id,
    #                 allocated_machine_statuses))

    # @minion_pool_synchronized
    # def deallocate_minion_pool_machines(self, ctxt, minion_pool_id, force=False):
    #     LOG.info("Attempting to deallocate Minion Pool '%s'.", minion_pool_id)
    #     minion_pool = db_api.get_minion_pool_lifecycle(
    #         ctxt, minion_pool_id, include_tasks_executions=False,
    #         include_machines=True)
    #     if minion_pool.pool_status not in (
    #             constants.MINION_POOL_STATUS_ALLOCATED) and not force:
    #         raise exception.InvalidMinionPoolState(
    #             "Minion Pool '%s' cannot be deallocated as it is in '%s' "
    #             "state instead of the expected '%s'. Please use the "
    #             "force flag if you are certain you want to deallocate "
    #             "the minion pool's machines." % (
    #                 minion_pool_id, minion_pool.pool_status,
    #                 constants.MINION_POOL_STATUS_ALLOCATED))

    #     if not force:
    #         self._check_all_pool_minion_machines_available(minion_pool)

    #     execution = models.TasksExecution()
    #     execution.id = str(uuid.uuid4())
    #     execution.action = minion_pool
    #     execution.status = constants.EXECUTION_STATUS_UNEXECUTED
    #     execution.type = (
    #         constants.EXECUTION_TYPE_MINION_POOL_DEALLOCATE_MINIONS)

    #     delete_minion_task_type = (
    #         constants.TASK_TYPE_DELETE_DESTINATION_MINION_MACHINE)
    #     if minion_pool.pool_platform == constants.PROVIDER_PLATFORM_SOURCE:
    #         delete_minion_task_type = (
    #             constants.TASK_TYPE_DELETE_DESTINATION_MINION_MACHINE)

    #     for minion_machine in minion_pool.minion_machines:
    #         minion_machine_id = minion_machine.id
    #         minion_pool.info[minion_machine_id] = {
    #             "pool_environment_options": minion_pool.source_environment,
    #             "minion_provider_properties": (
    #                 minion_machine.provider_properties)}
    #         self._create_task(
    #             minion_machine_id, delete_minion_task_type,
    #             # NOTE: we set 'on_error=True' to allow for the completion of
    #             # already running deletion tasks to prevent partial deletes:
    #             execution, on_error=True)

    #     self._check_execution_tasks_sanity(execution, minion_pool.info)

    #     # update the action info for all of the pool's minions:
    #     for minion_machine in minion_pool.minion_machines:
    #         db_api.update_transfer_action_info_for_instance(
    #             ctxt, minion_pool.id, minion_machine.id,
    #             minion_pool.info[minion_machine.id])

    #     # add new execution to DB:
    #     db_api.add_minion_pool_lifecycle_execution(ctxt, execution)
    #     LOG.info(
    #         "Minion pool deallocation execution created: %s", execution.id)

    #     self._begin_tasks(ctxt, minion_pool, execution)
    #     db_api.set_minion_pool_lifecycle_status(
    #         ctxt, minion_pool.id, constants.MINION_POOL_STATUS_DEALLOCATING)

    #     return self._get_minion_pool_lifecycle_execution(
    #         ctxt, minion_pool_id, execution.id).to_dict()

    @minion_pool_synchronized
    def get_minion_pool(self, ctxt, minion_pool_id):
        return self._get_minion_pool(
            ctxt, minion_pool_id, include_machines=True)

    @minion_pool_synchronized
    def update_minion_pool(self, ctxt, minion_pool_id, updated_values):
        minion_pool = self._get_minion_pool(
            ctxt, minion_pool_id, include_machines=False)
        if minion_pool.pool_status != constants.MINION_POOL_STATUS_UNINITIALIZED:
            raise exception.InvalidMinionPoolState(
                "Minion Pool '%s' cannot be updated as it is in '%s' status "
                "instead of the expected '%s'. Please ensure the pool machines"
                "have been deallocated and the pool's supporting resources "
                "have been torn down before updating the pool." % (
                    minion_pool_id, minion_pool.pool_status,
                    constants.MINION_POOL_STATUS_UNINITIALIZED))
        LOG.info(
            "Attempting to update minion_pool '%s' with payload: %s",
            minion_pool_id, updated_values)
        db_api.update_minion_pool(ctxt, minion_pool_id, updated_values)
        LOG.info("Minion Pool '%s' successfully updated", minion_pool_id)
        return db_api.get_minion_pool(ctxt, minion_pool_id)

    @minion_pool_synchronized
    def delete_minion_pool(self, ctxt, minion_pool_id):
        minion_pool = self._get_minion_pool(
            ctxt, minion_pool_id, include_machines=True)
        acceptable_deletion_statuses = [
            constants.MINION_POOL_STATUS_UNINITIALIZED,
            constants.MINION_POOL_STATUS_ERROR]
        if minion_pool.pool_status not in acceptable_deletion_statuses:
            raise exception.InvalidMinionPoolState(
                "Minion Pool '%s' cannot be deleted as it is in '%s' status "
                "instead of one of the expected '%s'. Please ensure the pool "
                "machines have been deallocated and the pool's supporting "
                "resources have been torn down before deleting the pool." % (
                    minion_pool_id, minion_pool.pool_status,
                    acceptable_deletion_statuses))

        LOG.info("Deleting minion pool with ID '%s'" % minion_pool_id)
        db_api.delete_minion_pool(ctxt, minion_pool_id)

    # @minion_pool_synchronized
    # def get_minion_pool_lifecycle_executions(
    #         self, ctxt, minion_pool_id, include_tasks=False):
    #     return db_api.get_minion_pool_lifecycle_executions(
    #         ctxt, minion_pool_id, include_tasks)

    # def _get_minion_pool_lifecycle_execution(
    #         self, ctxt, minion_pool_id, execution_id):
    #     execution = db_api.get_minion_pool_lifecycle_execution(
    #         ctxt, minion_pool_id, execution_id)
    #     if not execution:
    #         raise exception.NotFound(
    #             "Execution with ID '%s' for Minion Pool '%s' not found." % (
    #                 execution_id, minion_pool_id))
    #     return execution

    # @minion_pool_tasks_execution_synchronized
    # def get_minion_pool_lifecycle_execution(
    #         self, ctxt, minion_pool_id, execution_id):
    #     return self._get_minion_pool_lifecycle_execution(
    #         ctxt, minion_pool_id, execution_id).to_dict()

    # @minion_pool_tasks_execution_synchronized
    # def delete_minion_pool_lifecycle_execution(
    #         self, ctxt, minion_pool_id, execution_id):
    #     execution = self._get_minion_pool_lifecycle_execution(
    #         ctxt, minion_pool_id, execution_id)
    #     if execution.status in constants.ACTIVE_EXECUTION_STATUSES:
    #         raise exception.InvalidMigrationState(
    #             "Cannot delete execution '%s' for Minion pool '%s' as it is "
    #             "currently in '%s' state." % (
    #                 execution_id, minion_pool_id, execution.status))
    #     db_api.delete_minion_pool_lifecycle_execution(ctxt, execution_id)

    # @minion_pool_tasks_execution_synchronized
    # def cancel_minion_pool_lifecycle_execution(
    #         self, ctxt, minion_pool_id, execution_id, force):
    #     execution = self._get_minion_pool_lifecycle_execution(
    #         ctxt, minion_pool_id, execution_id)
    #     if execution.status not in constants.ACTIVE_EXECUTION_STATUSES:
    #         raise exception.InvalidMinionPoolState(
    #             "Minion pool '%s' has no running execution to cancel." % (
    #                 minion_pool_id))
    #     if execution.status == constants.EXECUTION_STATUS_CANCELLING and (
    #             not force):
    #         raise exception.InvalidMinionPoolState(
    #             "Execution for Minion Pool '%s' is already being cancelled. "
    #             "Please use the force option if you'd like to force-cancel "
    #             "it." % (minion_pool_id))
    #     self._cancel_tasks_execution(ctxt, execution, force=force)
