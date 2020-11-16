# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import contextlib
import itertools
import uuid

from oslo_concurrency import lockutils
from oslo_config import cfg
from oslo_log import log as logging
from taskflow.patterns import graph_flow
from taskflow.patterns import linear_flow
from taskflow.patterns import unordered_flow

from coriolis import constants
from coriolis import exception
from coriolis import utils
from coriolis.conductor.rpc import client as rpc_conductor_client
from coriolis.db import api as db_api
from coriolis.db.sqlalchemy import models
from coriolis.minion_manager.rpc import tasks as minion_manager_tasks
from coriolis.minion_manager.rpc import utils as minion_manager_utils
from coriolis.scheduler.rpc import client as rpc_scheduler_client
from coriolis.taskflow import runner as taskflow_runner
from coriolis.worker.rpc import client as rpc_worker_client


VERSION = "1.0"

LOG = logging.getLogger(__name__)

MINION_MANAGER_OPTS = []

CONF = cfg.CONF
CONF.register_opts(MINION_MANAGER_OPTS, 'minion_manager')


class MinionManagerServerEndpoint(object):

    @property
    def _taskflow_runner(self):
        return taskflow_runner.TaskFlowRunner(
            constants.MINION_MANAGER_MAIN_MESSAGING_TOPIC,
            max_workers=25)

    @property
    def _rpc_worker_client(self):
        return rpc_worker_client.WorkerClient()

    @property
    def _scheduler_client(self):
        return rpc_scheduler_client.SchedulerClient()

    @property
    def _conductor_client(self):
        return rpc_conductor_client.ConductorClient()

    def get_diagnostics(self, ctxt):
        return utils.get_diagnostics_info()

    def get_endpoint_source_minion_pool_options(
            self, ctxt, endpoint_id, env, option_names):
        endpoint = self._conductor_client.get_endpoint(ctxt, endpoint_id)

        worker_service = self._scheduler_client.get_worker_service_for_specs(
            ctxt, enabled=True,
            region_sets=[[reg['id'] for reg in endpoint['mapped_regions']]],
            provider_requirements={
                endpoint['type']: [
                    constants.PROVIDER_TYPE_SOURCE_MINION_POOL]})
        worker_rpc = rpc_worker_client.WorkerClient.from_service_definition(
            worker_service)

        return worker_rpc.get_endpoint_source_minion_pool_options(
            ctxt, endpoint['type'], endpoint['connection_info'], env,
            option_names)

    def get_endpoint_destination_minion_pool_options(
            self, ctxt, endpoint_id, env, option_names):
        endpoint = self._conductor_client.get_endpoint(ctxt, endpoint_id)

        worker_service = self._scheduler_client.get_worker_service_for_specs(
            ctxt, enabled=True,
            region_sets=[[reg['id'] for reg in endpoint['mapped_regions']]],
            provider_requirements={
                endpoint['type']: [
                    constants.PROVIDER_TYPE_DESTINATION_MINION_POOL]})
        worker_rpc = rpc_worker_client.WorkerClient.from_service_definition(
            worker_service)

        return worker_rpc.get_endpoint_destination_minion_pool_options(
            ctxt, endpoint['type'], endpoint['connection_info'], env,
            option_names)

    def validate_endpoint_source_minion_pool_options(
            self, ctxt, endpoint_id, pool_environment):
        endpoint = self._conductor_client.get_endpoint(ctxt, endpoint_id)

        worker_service = self._scheduler_client.get_worker_service_for_specs(
            ctxt, enabled=True,
            region_sets=[[reg['id'] for reg in endpoint['mapped_regions']]],
            provider_requirements={
                endpoint['type']: [
                    constants.PROVIDER_TYPE_SOURCE_MINION_POOL]})
        worker_rpc = rpc_worker_client.WorkerClient.from_service_definition(
            worker_service)

        return worker_rpc.validate_endpoint_source_minion_pool_options(
            ctxt, endpoint['type'], pool_environment)

    def validate_endpoint_destination_minion_pool_options(
            self, ctxt, endpoint_id, pool_environment):
        endpoint = self._conductor_client.get_endpoint(ctxt, endpoint_id)

        worker_service = self._scheduler_client.get_worker_service_for_specs(
            ctxt, enabled=True,
            region_sets=[[reg['id'] for reg in endpoint['mapped_regions']]],
            provider_requirements={
                endpoint['type']: [
                    constants.PROVIDER_TYPE_DESTINATION_MINION_POOL]})
        worker_rpc = rpc_worker_client.WorkerClient.from_service_definition(
            worker_service)

        return worker_rpc.validate_endpoint_destination_minion_pool_options(
            ctxt, endpoint['type'], pool_environment)

    @minion_manager_utils.minion_pool_synchronized_op
    def add_minion_pool_event(self, ctxt, minion_pool_id, level, message):
        LOG.info(
            "Minion pool event for pool %s: %s", minion_pool_id, message)
        pool = db_api.get_minion_pool(ctxt, minion_pool_id)
        db_api.add_minion_pool_event(ctxt, pool.id, level, message)

    @minion_manager_utils.minion_pool_synchronized_op
    def add_minion_pool_progress_update(
            self, ctxt, minion_pool_id, total_steps, message):
        LOG.info(
            "Adding pool progress update for %s: %s", minion_pool_id, message)
        db_api.add_minion_pool_progress_update(
            ctxt, minion_pool_id, total_steps, message)

    @minion_manager_utils.minion_pool_synchronized_op
    def update_minion_pool_progress_update(
            self, ctxt, minion_pool_id, step, total_steps, message):
        LOG.info("Updating minion pool progress update: %s", minion_pool_id)
        db_api.update_minion_pool_progress_update(
            ctxt, minion_pool_id, step, total_steps, message)

    @minion_manager_utils.minion_pool_synchronized_op
    def get_minion_pool_progress_step(self, ctxt, minion_pool_id):
        return db_api.get_minion_pool_progress_step(ctxt, minion_pool_id)

    def validate_minion_pool_selections_for_action(self, ctxt, action):
        """ Validates the minion pool selections for a given action. """
        if not isinstance(action, dict):
            raise exception.InvalidInput(
                "Action must be a dict, got '%s': %s" % (
                    type(action), action))
        required_action_properties = [
            'id', 'origin_endpoint_id', 'destination_endpoint_id',
            'origin_minion_pool_id', 'destination_minion_pool_id',
            'instance_osmorphing_minion_pool_mappings']
        missing = [
            prop for prop in required_action_properties
            if prop not in action]
        if missing:
            raise exception.InvalidInput(
                "Missing the following required action properties for "
                "minion pool selection validation: %s. Got %s" % (
                    missing, action))

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

        if action['origin_minion_pool_id']:
            origin_pool = _get_pool(action['origin_minion_pool_id'])
            if origin_pool.endpoint_id != action['origin_endpoint_id']:
                raise exception.InvalidMinionPoolSelection(
                    "The selected origin minion pool ('%s') belongs to a "
                    "different Coriolis endpoint ('%s') than the requested "
                    "origin endpoint ('%s')" % (
                        action['origin_minion_pool_id'],
                        origin_pool.endpoint_id,
                        action['origin_endpoint_id']))
            if origin_pool.platform != constants.PROVIDER_PLATFORM_SOURCE:
                raise exception.InvalidMinionPoolSelection(
                    "The selected origin minion pool ('%s') is configured as a"
                    " '%s' pool. The pool must be of type %s to be used for "
                    "data exports." % (
                        action['origin_minion_pool_id'],
                        origin_pool.platform,
                        constants.PROVIDER_PLATFORM_SOURCE))
            if origin_pool.os_type != constants.OS_TYPE_LINUX:
                raise exception.InvalidMinionPoolSelection(
                    "The selected origin minion pool ('%s') is of OS type '%s'"
                    " instead of the Linux OS type required for a source "
                    "transfer minion pool." % (
                        action['origin_minion_pool_id'],
                        origin_pool.os_type))
            LOG.debug(
                "Successfully validated compatibility of origin minion pool "
                "'%s' for use with action '%s'." % (
                    action['origin_minion_pool_id'], action['id']))

        if action['destination_minion_pool_id']:
            destination_pool = _get_pool(action['destination_minion_pool_id'])
            if destination_pool.endpoint_id != (
                    action['destination_endpoint_id']):
                raise exception.InvalidMinionPoolSelection(
                    "The selected destination minion pool ('%s') belongs to a "
                    "different Coriolis endpoint ('%s') than the requested "
                    "destination endpoint ('%s')" % (
                        action['destination_minion_pool_id'],
                        destination_pool.endpoint_id,
                        action['destination_endpoint_id']))
            if destination_pool.platform != (
                    constants.PROVIDER_PLATFORM_DESTINATION):
                raise exception.InvalidMinionPoolSelection(
                    "The selected destination minion pool ('%s') is configured"
                    " as a '%s'. The pool must be of type %s to be used for "
                    "data imports." % (
                        action['destination_minion_pool_id'],
                        destination_pool.platform,
                        constants.PROVIDER_PLATFORM_DESTINATION))
            if destination_pool.os_type != constants.OS_TYPE_LINUX:
                raise exception.InvalidMinionPoolSelection(
                    "The selected destination minion pool ('%s') is of OS type"
                    " '%s' instead of the Linux OS type required for a source "
                    "transfer minion pool." % (
                        action['destination_minion_pool_id'],
                        destination_pool.os_type))
            LOG.debug(
                "Successfully validated compatibility of destination minion "
                "pool '%s' for use with action '%s'." % (
                    action['origin_minion_pool_id'], action['id']))

        if action.get('instance_osmorphing_minion_pool_mappings'):
            osmorphing_pool_mappings = {
                instance_id: pool_id
                for (instance_id, pool_id) in (
                    action.get(
                        'instance_osmorphing_minion_pool_mappings').items())
                if pool_id}
            for (instance, pool_id) in osmorphing_pool_mappings.items():
                osmorphing_pool = _get_pool(pool_id)
                if osmorphing_pool.endpoint_id != (
                        action['destination_endpoint_id']):
                    raise exception.InvalidMinionPoolSelection(
                        "The selected OSMorphing minion pool for instance '%s'"
                        " ('%s') belongs to a different Coriolis endpoint "
                        "('%s') than the destination endpoint ('%s')" % (
                            instance, pool_id,
                            osmorphing_pool.endpoint_id,
                            action['destination_endpoint_id']))
                if osmorphing_pool.platform != (
                        constants.PROVIDER_PLATFORM_DESTINATION):
                    raise exception.InvalidMinionPoolSelection(
                        "The selected OSMorphing minion pool for instance '%s'"
                        "  ('%s') is configured as a '%s' pool. The pool must "
                        "be of type %s to be used for OSMorphing." % (
                            instance, pool_id,
                            osmorphing_pool.platform,
                            constants.PROVIDER_PLATFORM_DESTINATION))
                LOG.debug(
                    "Successfully validated compatibility of destination "
                    "minion pool '%s' for use as OSMorphing minion for "
                    "instance '%s' during action '%s'." % (
                        pool_id, instance, action['id']))

    def allocate_minion_machines_for_replica(
            self, ctxt, replica):
        minion_allocations = self._allocate_minion_machines_for_action(
            ctxt, replica, include_transfer_minions=True,
            include_osmorphing_minions=False)
        try:
            self._conductor_client.confirm_replica_minions_allocation(
                ctxt, replica['id'], minion_allocations)
        except Exception as ex:
            LOG.warn(
                "Error occured while reporting minion pool allocations for "
                "Replica with ID '%s'. Removing all allocations. "
                "Error was: %s" % (
                    replica['id'], utils.get_exception_details()))
            self.deallocate_minion_machines_for_action(
                ctxt, replica['id'])
            raise

    def allocate_minion_machines_for_migration(
            self, ctxt, migration, include_transfer_minions=True,
            include_osmorphing_minions=True):
        minion_allocations = self._allocate_minion_machines_for_action(
            ctxt, migration, include_transfer_minions=include_transfer_minions,
            include_osmorphing_minions=include_osmorphing_minions)
        try:
            self._conductor_client.confirm_migration_minions_allocation(
                ctxt, migration['id'], minion_allocations)
        except Exception as ex:
            LOG.warn(
                "Error occured while reporting minion pool allocations for "
                "Migration with ID '%s'. Removing all allocations. "
                "Error was: %s" % (
                    migration['id'], utils.get_exception_details()))
            self.deallocate_minion_machines_for_action(
                ctxt, migration['id'])
            raise

    def _allocate_minion_machines_for_action(
            self, ctxt, action, include_transfer_minions=True,
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
        if not isinstance(action, dict):
            raise exception.InvalidInput(
                "Action must be a dict, got '%s': %s" % (
                    type(action), action))
        required_action_properties = [
            'id', 'instances', 'origin_minion_pool_id',
            'destination_minion_pool_id',
            'instance_osmorphing_minion_pool_mappings']
        missing = [
            prop for prop in required_action_properties
            if prop not in action]
        if missing:
            raise exception.InvalidInput(
                "Missing the following required action properties for "
                "minion pool machine allocation: %s. Got %s" % (
                    missing, action))

        instance_machine_allocations = {
            instance: {} for instance in action['instances']}

        minion_pool_ids = set()
        if action['origin_minion_pool_id']:
            minion_pool_ids.add(action['origin_minion_pool_id'])
        if action['destination_minion_pool_id']:
            minion_pool_ids.add(action['destination_minion_pool_id'])
        if action['instance_osmorphing_minion_pool_mappings']:
            minion_pool_ids = minion_pool_ids.union(set(
                action['instance_osmorphing_minion_pool_mappings'].values()))
        if None in minion_pool_ids:
            minion_pool_ids.remove(None)

        if not minion_pool_ids:
            LOG.debug(
                "No minion pool settings found for action '%s'. "
                "Skipping minion machine allocations." % (
                    action['id']))
            return instance_machine_allocations

        LOG.debug(
            "All minion pool selections for action '%s': %s",
            action['id'], minion_pool_ids)

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
            action['instance_osmorphing_minion_pool_mappings'])
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
                pool_id: pool.status
                for (pool_id, pool) in minion_pool_id_mappings.items()
                if pool.status != constants.MINION_POOL_STATUS_ALLOCATED}
            if unallocated_pools:
                raise exception.InvalidMinionPoolSelection(
                    "The following minion pools have not had their machines "
                    "allocated and thus cannot be used: %s" % (
                        unallocated_pools))

            allocated_source_machine_ids = set()
            allocated_target_machine_ids = set()
            allocated_osmorphing_machine_ids = set()
            for instance in action['instances']:

                if include_transfer_minions:
                    if action['origin_minion_pool_id']:
                        origin_pool = minion_pool_id_mappings[
                            action['origin_minion_pool_id']]
                        machine = _select_machine(
                            origin_pool, exclude=allocated_source_machine_ids)
                        allocated_source_machine_ids.add(machine.id)
                        instance_machine_allocations[
                            instance]['source_minion'] = machine
                        LOG.debug(
                            "Selected minion machine '%s' for source-side "
                            "syncing of instance '%s' as part of transfer "
                            "action '%s'.", machine.id, instance, action['id'])

                    if action['destination_minion_pool_id']:
                        dest_pool = minion_pool_id_mappings[
                            action['destination_minion_pool_id']]
                        machine = _select_machine(
                            dest_pool, exclude=allocated_target_machine_ids)
                        allocated_target_machine_ids.add(machine.id)
                        instance_machine_allocations[
                            instance]['target_minion'] = machine
                        LOG.debug(
                            "Selected minion machine '%s' for target-side "
                            "syncing of instance '%s' as part of transfer "
                            "action '%s'.", machine.id, instance, action['id'])

                if include_osmorphing_minions:
                    if instance not in osmorphing_pool_map:
                        LOG.debug(
                            "Instance '%s' is not listed in the OSMorphing "
                            "minion pool mappings for action '%s'." % (
                                instance, action['id']))
                    elif osmorphing_pool_map[instance] is None:
                        LOG.debug(
                            "OSMorphing pool ID for instance '%s' is "
                            "None in action '%s'. Ignoring." % (
                                instance, action['id']))
                    else:
                        osmorphing_pool_id = osmorphing_pool_map[instance]
                        # if the selected target and OSMorphing pools
                        # are the same, reuse the same worker:
                        ima = instance_machine_allocations[instance]
                        if osmorphing_pool_id == (
                                action['destination_minion_pool_id']) and (
                                    'target_minion' in ima):
                            allocated_target_machine = ima[
                                'target_minion']
                            LOG.debug(
                                "Reusing disk sync minion '%s' for the "
                                "OSMorphing of instance '%s' as port of "
                                "transfer action '%s'",
                                allocated_target_machine.id, instance,
                                action['id'])
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
                                machine.id, instance, action['id'])

            # mark the selected machines as allocated:
            all_machine_ids = set(itertools.chain(
                allocated_source_machine_ids,
                allocated_target_machine_ids,
                allocated_osmorphing_machine_ids))
            db_api.set_minion_machines_allocation_statuses(
                ctxt, all_machine_ids, action['id'],
                constants.MINION_MACHINE_STATUS_ALLOCATED,
                refresh_allocation_time=True)

        # filter out redundancies:
        instance_machine_allocations = {
            instance: allocations
            for (instance, allocations) in instance_machine_allocations.items()
            if allocations}

        LOG.debug(
            "Allocated the following minion machines for action '%s': %s",
            action['id'], {
                instance: {
                    typ: machine.id
                    for (typ, machine) in allocation.items()}
                for (instance, allocation) in instance_machine_allocations.items()})
        return instance_machine_allocations

    def deallocate_minion_machine(self, ctxt, minion_machine_id):

        minion_machine = db_api.get_minion_machine(
            ctxt, minion_machine_id)
        if not minion_machine:
            LOG.warn(
                "Could not find minion machine with ID '%s' for deallocation. "
                "Presuming it was deleted and returning early",
                minion_machine_id)
            return

        machine_allocated_status = constants.MINION_MACHINE_STATUS_ALLOCATED
        with lockutils.lock(
                constants.MINION_POOL_LOCK_NAME_FORMAT % (
                    minion_machine.pool_id),
                external=True):

            if minion_machine.status != machine_allocated_status or (
                    not minion_machine.allocated_action):
                LOG.warn(
                    "Minion machine '%s' was either in an improper status (%s)"
                    ", or did not have an associated action ('%s') for "
                    "deallocation request. Marking as available anyway.",
                    minion_machine.id, minion_machine.status,
                    minion_machine.allocated_action)
            LOG.debug(
                "Attempting to deallocate all minion pool machine '%s' "
                "(currently allocated to action '%s' with status '%s')",
                minion_machine.id, minion_machine.allocated_action,
                minion_machine.status)
            db_api.update_minion_machine(
                ctxt, minion_machine.id, {
                    "status": constants.MINION_MACHINE_STATUS_AVAILABLE,
                    "allocated_action": None})
            LOG.debug(
                "Successfully deallocated minion machine with '%s'.",
                minion_machine.id)

    def deallocate_minion_machines_for_action(self, ctxt, action_id):

        allocated_minion_machines = db_api.get_minion_machines(
            ctxt, allocated_action_id=action_id)

        if not allocated_minion_machines:
            LOG.debug(
                "No minion machines seem to have been used for action with "
                "base_id '%s'. Skipping minion machine deallocation.",
                action_id)
            return

        minion_pool_ids = {
            machine.pool_id for machine in allocated_minion_machines}

        LOG.debug(
            "Attempting to deallocate all minion pool machine selections "
            "for action '%s'. Afferent pools are: %s",
            action_id, minion_pool_ids)

        with contextlib.ExitStack() as stack:
            _ = [
                stack.enter_context(
                    lockutils.lock(
                        constants.MINION_POOL_LOCK_NAME_FORMAT % pool_id,
                        external=True))
                for pool_id in minion_pool_ids]

            machine_ids = [m.id for m in allocated_minion_machines]
            LOG.info(
                "Releasing the following minion machines for "
                "action '%s': %s", action_id, machine_ids)
            db_api.set_minion_machines_allocation_statuses(
                ctxt, machine_ids, None,
                constants.MINION_MACHINE_STATUS_AVAILABLE,
                refresh_allocation_time=False)
            LOG.debug(
                "Successfully released all minion machines associated "
                "with action with base_id '%s'.", action_id)

    def _get_minion_pool_healthcheck_flow(
            self, ctxt, minion_pool, requery=True):

        if requery:
            minion_pool = self._get_minion_pool(
                ctxt, minion_pool.id, include_machines=True,
                include_progress_updates=False, include_events=False)

        pool_healthcheck_flow = unordered_flow.Flow(
            minion_manager_tasks.MINION_POOL_HEALTHCHECK_FLOW_NAME_FORMAT % (
                minion_pool.id))

        for machine in minion_pool.minion_machines:
            # define healthcheck subflow for each machine:
            machine_healthcheck_subflow = graph_flow.Flow(
                minion_manager_tasks.MINION_POOL_HEALTHCHECK_MACHINE_SUBFLOW_NAME_FORMAT % (
                    minion_pool.id, machine.id))

            # add healtcheck task to healthcheck subflow:
            machine_healthcheck_task = (
                minion_manager_tasks.HealthcheckMinionMachineTask(
                    minion_pool.id, machine.id, minion_pool.platform))
            machine_healthcheck_subflow.add(machine_healthcheck_task)

            # define reallocation subflow:
            machine_reallocation_subflow = linear_flow.Flow(
                minion_manager_tasks.MINION_POOL_REALLOCATE_MACHINE_SUBFLOW_NAME_FORMAT % (
                    minion_pool.id, machine.id))
            machine_reallocation_subflow.add(
                minion_manager_tasks.DeallocateMinionMachineTask(
                    minion_pool.id, machine.id, minion_pool.platform))
            machine_reallocation_subflow.add(
                minion_manager_tasks.AllocateMinionMachineTask(
                    minion_pool.id, machine.id, minion_pool.platform))
            machine_healthcheck_subflow.add(machine_reallocation_subflow)

            # link reallocation subflow to healthcheck task:
            healthcheck_cls = minion_manager_tasks.HealthcheckMinionMachineTask
            machine_healthcheck_subflow.link(
                machine_healthcheck_task, machine_reallocation_subflow,
                decider=healthcheck_cls.make_minion_machine_healtcheck_decider(
                    minion_pool.id, machine.id,
                    on_successful_healthcheck=False))

            # add the healthcheck subflow to the main flow:
            pool_healthcheck_flow.add(machine_healthcheck_subflow)

        return pool_healthcheck_flow

    @minion_manager_utils.minion_pool_synchronized_op
    def healthcheck_minion_pool(self, ctxt, minion_pool_id):
        LOG.info("Attempting to healthcheck Minion Pool '%s'.", minion_pool_id)
        minion_pool = self._get_minion_pool(
            ctxt, minion_pool_id, include_events=False, include_machines=True,
            include_progress_updates=False)
        endpoint_dict = self._conductor_client.get_endpoint(
            ctxt, minion_pool.endpoint_id)
        acceptable_allocation_statuses = [
            constants.MINION_POOL_STATUS_ALLOCATED]
        current_status = minion_pool.status
        if current_status not in acceptable_allocation_statuses:
            raise exception.InvalidMinionPoolState(
                "Minion machines for pool '%s' cannot be healthchecked as the "
                "pool is in '%s' state instead of the expected %s." % (
                    minion_pool_id, current_status,
                    acceptable_allocation_statuses))

        healthcheck_flow = self._get_minion_pool_healthcheck_flow(
            ctxt, minion_pool, requery=False)
        initial_store = self._get_pool_initial_taskflow_store_base(
            ctxt, minion_pool, endpoint_dict)

        try:
            db_api.set_minion_pool_status(
                ctxt, minion_pool_id,
                constants.MINION_POOL_STATUS_POOL_MAINTENANCE)
            self._taskflow_runner.run_flow_in_background(
                healthcheck_flow, store=initial_store)
        except:
            db_api.set_minion_pool_status(
                ctxt, minion_pool_id, current_status)
            raise

        return self._get_minion_pool(ctxt, minion_pool.id)


    def _get_minion_pool_allocation_flow(self, minion_pool):
        """ Returns a taskflow.Flow object pertaining to all the tasks
        required for allocating a minion pool (validation, shared resource
        setup, and actual minion creation)
        """
        # create task flow:
        allocation_flow = linear_flow.Flow(
            minion_manager_tasks.MINION_POOL_ALLOCATION_FLOW_NAME_FORMAT % (
                minion_pool.id))

        # tansition pool to VALIDATING:
        allocation_flow.add(minion_manager_tasks.UpdateMinionPoolStatusTask(
            minion_pool.id, constants.MINION_POOL_STATUS_VALIDATING_INPUTS,
            status_to_revert_to=constants.MINION_POOL_STATUS_ERROR))

        # add pool options validation task:
        allocation_flow.add(minion_manager_tasks.ValidateMinionPoolOptionsTask(
            # NOTE: we pass in the ID of the minion pool itself as both
            # the task ID and the instance ID for tasks which are strictly
            # pool-related.
            minion_pool.id,
            minion_pool.id,
            minion_pool.platform))

        # transition pool to 'DEPLOYING_SHARED_RESOURCES':
        allocation_flow.add(minion_manager_tasks.UpdateMinionPoolStatusTask(
            minion_pool.id,
            constants.MINION_POOL_STATUS_ALLOCATING_SHARED_RESOURCES))

        # add pool shared resources deployment task:
        allocation_flow.add(
            minion_manager_tasks.AllocateSharedPoolResourcesTask(
                minion_pool.id, minion_pool.id, minion_pool.platform,
                # NOTE: the shared resource deployment task will always get
                # run by itself so it is safe to have it override task_info:
                provides='task_info'))

        # add subflow for deploying all of the minion machines:
        fmt = (
            minion_manager_tasks.MINION_POOL_ALLOCATE_MINIONS_SUBFLOW_NAME_FORMAT)
        machines_flow = unordered_flow.Flow(fmt % minion_pool.id)
        pool_machine_ids = []
        for _ in range(minion_pool.minimum_minions):
            machine_id = str(uuid.uuid4())
            pool_machine_ids.append(machine_id)
            machines_flow.add(
                minion_manager_tasks.AllocateMinionMachineTask(
                    minion_pool.id, machine_id, minion_pool.platform))
        # NOTE: bool(flow) == False if the flow has no child flows/tasks:
        if machines_flow:
            allocation_flow.add(minion_manager_tasks.UpdateMinionPoolStatusTask(
                minion_pool.id,
                constants.MINION_POOL_STATUS_ALLOCATING_MACHINES))
            LOG.debug(
                "The following minion machine IDs will be created for "
                "pool with ID '%s': %s" % (minion_pool.id, pool_machine_ids))
            allocation_flow.add(machines_flow)
        else:
            LOG.debug(
                "No upfront minion machine deployments required for minion "
                "pool with ID '%s'", minion_pool.id)

        # transition pool to ALLOCATED:
        allocation_flow.add(minion_manager_tasks.UpdateMinionPoolStatusTask(
            minion_pool.id, constants.MINION_POOL_STATUS_ALLOCATED))

        return allocation_flow


    def create_minion_pool(
            self, ctxt, name, endpoint_id, pool_platform, pool_os_type,
            environment_options, minimum_minions, maximum_minions,
            minion_max_idle_time, minion_retention_strategy, notes=None,
            skip_allocation=False):

        endpoint_dict = self._conductor_client.get_endpoint(
            ctxt, endpoint_id)
        minion_pool = models.MinionPool()
        minion_pool.id = str(uuid.uuid4())
        minion_pool.name = name
        minion_pool.notes = notes
        minion_pool.platform = pool_platform
        minion_pool.os_type = pool_os_type
        minion_pool.endpoint_id = endpoint_id
        minion_pool.environment_options = environment_options
        minion_pool.status = constants.MINION_POOL_STATUS_DEALLOCATED
        minion_pool.minimum_minions = minimum_minions
        minion_pool.maximum_minions = maximum_minions
        minion_pool.minion_max_idle_time = minion_max_idle_time
        minion_pool.minion_retention_strategy = minion_retention_strategy

        db_api.add_minion_pool(ctxt, minion_pool)

        if not skip_allocation:
            allocation_flow = self._get_minion_pool_allocation_flow(
                minion_pool)
            # start the deployment flow:
            initial_store = self._get_pool_initial_taskflow_store_base(
                ctxt, minion_pool, endpoint_dict)
            self._taskflow_runner.run_flow_in_background(
                allocation_flow, store=initial_store)

        return self.get_minion_pool(ctxt, minion_pool.id)

    def _get_pool_initial_taskflow_store_base(
            self, ctxt, minion_pool, endpoint_dict):
        # NOTE: considering pools are associated to strictly one endpoint,
        # we can duplicate the 'origin/destination':
        origin_dest_info = {
            "id": endpoint_dict['id'],
            "connection_info": endpoint_dict['connection_info'],
            "mapped_regions": endpoint_dict['mapped_regions'],
            "type": endpoint_dict['type']}
        initial_store = {
            "context": ctxt,
            "origin": origin_dest_info,
            "destination": origin_dest_info,
            "task_info": {
                "pool_identifier": minion_pool.id,
                "pool_os_type": minion_pool.os_type,
                "pool_environment_options": minion_pool.environment_options}}
        return initial_store

    def _check_pool_machines_in_use(
            self, ctxt, minion_pool, raise_if_in_use=False, requery=False):
        """ Checks whether the given pool has any machines currently in-use.
        Returns a list of the used machines if so, or an empty list of not.
        """
        if requery:
            minion_pool = self._get_minion_pool(
                ctxt, minion_pool.id, include_machines=True,
                include_events=False, include_progress_updates=False)
        unused_machine_states = [
            constants.MINION_MACHINE_STATUS_AVAILABLE,
            constants.MINION_MACHINE_STATUS_ERROR_DEPLOYING,
            constants.MINION_MACHINE_STATUS_ERROR]
        used_machines = {
            mch for mch in minion_pool.minion_machines
            if mch.status not in unused_machine_states}
        if used_machines and raise_if_in_use:
            raise exception.InvalidMinionPoolState(
                "Minion pool '%s' has one or more machines which are in an"
                " active state: %s" % (
                    minion_pool.id, {
                        mch.id: mch.status for mch in used_machines}))
        return used_machines

    @minion_manager_utils.minion_pool_synchronized_op
    def allocate_minion_pool(self, ctxt, minion_pool_id):
        LOG.info("Attempting to allocate Minion Pool '%s'.", minion_pool_id)
        minion_pool = self._get_minion_pool(
            ctxt, minion_pool_id, include_events=False, include_machines=False,
            include_progress_updates=False)
        endpoint_dict = self._conductor_client.get_endpoint(
            ctxt, minion_pool.endpoint_id)
        acceptable_allocation_statuses = [
            constants.MINION_POOL_STATUS_DEALLOCATED]
        current_status = minion_pool.status
        if current_status not in acceptable_allocation_statuses:
            raise exception.InvalidMinionPoolState(
                "Minion machines for pool '%s' cannot be allocated as the pool"
                " is in '%s' state instead of the expected %s. Please "
                "force-deallocate the pool and try again." % (
                    minion_pool_id, minion_pool.status,
                    acceptable_allocation_statuses))

        allocation_flow = self._get_minion_pool_allocation_flow(minion_pool)
        initial_store = self._get_pool_initial_taskflow_store_base(
            ctxt, minion_pool, endpoint_dict)

        try:
            db_api.set_minion_pool_status(
                ctxt, minion_pool_id,
                constants.MINION_POOL_STATUS_POOL_MAINTENANCE)
            self._taskflow_runner.run_flow_in_background(
                allocation_flow, store=initial_store)
        except:
            db_api.set_minion_pool_status(
                ctxt, minion_pool_id, current_status)
            raise

        return self._get_minion_pool(ctxt, minion_pool.id)

    def _get_minion_pool_deallocation_flow(self, minion_pool):
        """ Returns a taskflow.Flow object pertaining to all the tasks
        required for deallocating a minion pool (machines and shared resources)
        """
        # create task flow:
        deallocation_flow = linear_flow.Flow(
            minion_manager_tasks.MINION_POOL_DEALLOCATION_FLOW_NAME_FORMAT % (
                minion_pool.id))

        # add subflow for deallocating all of the minion machines:
        fmt = (
            minion_manager_tasks.MINION_POOL_DEALLOCATE_MACHINES_SUBFLOW_NAME_FORMAT)
        machines_flow = unordered_flow.Flow(fmt % minion_pool.id)
        for machine in minion_pool.minion_machines:
            machines_flow.add(
                minion_manager_tasks.DeallocateMinionMachineTask(
                    minion_pool.id, machine.id, minion_pool.platform))
        # NOTE: bool(flow) == False if the flow has no child flows/tasks:
        if machines_flow:
            # tansition pool to DEALLOCATING_MACHINES:
            deallocation_flow.add(minion_manager_tasks.UpdateMinionPoolStatusTask(
                minion_pool.id,
                constants.MINION_POOL_STATUS_DEALLOCATING_MACHINES,
                status_to_revert_to=constants.MINION_POOL_STATUS_ERROR))
            deallocation_flow.add(machines_flow)
        else:
            LOG.debug(
                "No machines for pool '%s' require deallocating.", minion_pool.id)

        # transition pool to DEALLOCATING_SHARED_RESOURCES:
        deallocation_flow.add(minion_manager_tasks.UpdateMinionPoolStatusTask(
            minion_pool.id,
            constants.MINION_POOL_STATUS_DEALLOCATING_SHARED_RESOURCES,
            status_to_revert_to=constants.MINION_POOL_STATUS_ERROR))

        # add pool shared resources deletion task:
        deallocation_flow.add(
            minion_manager_tasks.DeallocateSharedPoolResourcesTask(
                minion_pool.id, minion_pool.id, minion_pool.platform))

        # transition pool to DEALLOCATED:
        deallocation_flow.add(minion_manager_tasks.UpdateMinionPoolStatusTask(
            minion_pool.id, constants.MINION_POOL_STATUS_DEALLOCATED))

        return deallocation_flow

    def _get_pool_deallocation_initial_store(
            self, ctxt, minion_pool, endpoint_dict):
        base = self._get_pool_initial_taskflow_store_base(
            ctxt, minion_pool, endpoint_dict)
        if 'task_info' not in base:
            base['task_info'] = {}
        base['task_info']['pool_shared_resources'] = (
            minion_pool.shared_resources)
        return base

    @minion_manager_utils.minion_pool_synchronized_op
    def deallocate_minion_pool(self, ctxt, minion_pool_id, force=False):
        LOG.info("Attempting to deallocate Minion Pool '%s'.", minion_pool_id)
        minion_pool = self._get_minion_pool(
            ctxt, minion_pool_id, include_events=False, include_machines=True,
            include_progress_updates=False)
        current_status = minion_pool.status
        if current_status == constants.MINION_POOL_STATUS_DEALLOCATED:
            LOG.debug(
                "Deallocation requested on already deallocated pool '%s'. "
                "Nothing to do so returning early.", minion_pool_id)
            return self._get_minion_pool(ctxt, minion_pool.id)
        acceptable_deallocation_statuses = [
            constants.MINION_POOL_STATUS_ALLOCATED,
            constants.MINION_POOL_STATUS_ERROR]
        if current_status not in acceptable_deallocation_statuses:
            if not force:
                raise exception.InvalidMinionPoolState(
                    "Minion pool '%s' cannot be deallocated as the pool"
                    " is in '%s' state instead of one of the expected %s"% (
                        minion_pool_id, minion_pool.status,
                        acceptable_deallocation_statuses))
            else:
                LOG.warn(
                    "Forcibly deallocating minion pool '%s' at user request.",
                    minion_pool_id)
        self._check_pool_machines_in_use(
            ctxt, minion_pool, raise_if_in_use=not force)
        endpoint_dict = self._conductor_client.get_endpoint(
            ctxt, minion_pool.endpoint_id)

        deallocation_flow = self._get_minion_pool_deallocation_flow(
            minion_pool)
        initial_store = self._get_pool_deallocation_initial_store(
            ctxt, minion_pool, endpoint_dict)

        try:
            db_api.set_minion_pool_status(
                ctxt, minion_pool_id,
                constants.MINION_POOL_STATUS_POOL_MAINTENANCE)
            self._taskflow_runner.run_flow_in_background(
                deallocation_flow, store=initial_store)
        except:
            db_api.set_minion_pool_status(
                ctxt, minion_pool_id, current_status)
            raise

        return self._get_minion_pool(ctxt, minion_pool.id)

    def get_minion_pools(self, ctxt, include_machines=True):
        return db_api.get_minion_pools(
            ctxt, include_machines=include_machines, include_events=False,
            include_progress_updates=False)

    def _get_minion_pool(
            self, ctxt, minion_pool_id, include_machines=True,
            include_events=True, include_progress_updates=True):
        minion_pool = db_api.get_minion_pool(
            ctxt, minion_pool_id, include_machines=include_machines,
            include_events=include_events,
            include_progress_updates=include_progress_updates)
        if not minion_pool:
            raise exception.NotFound(
                "Minion pool with ID '%s' not found." % minion_pool_id)
        return minion_pool

    # @minion_manager_utils.minion_pool_synchronized_op
    # def set_up_shared_minion_pool_resources(self, ctxt, minion_pool_id):
    #     LOG.info(
    #         "Attempting to set up shared resources for Minion Pool '%s'.",
    #         minion_pool_id)
    #     minion_pool = db_api.get_minion_pool_lifecycle(
    #         ctxt, minion_pool_id, include_tasks_executions=False,
    #         include_machines=False)
    #     if minion_pool.status != constants.MINION_POOL_STATUS_UNINITIALIZED:
    #         raise exception.InvalidMinionPoolState(
    #             "Minion Pool '%s' cannot have shared resources set up as it "
    #             "is in '%s' state instead of the expected %s."% (
    #                 minion_pool_id, minion_pool.status,
    #                 constants.MINION_POOL_STATUS_UNINITIALIZED))

    #     execution = models.TasksExecution()
    #     execution.id = str(uuid.uuid4())
    #     execution.action = minion_pool
    #     execution.status = constants.EXECUTION_STATUS_UNEXECUTED
    #     execution.type = (
    #         constants.EXECUTION_TYPE_MINION_POOL_SET_UP_SHARED_RESOURCES)

    #     minion_pool.info[minion_pool_id] = {
    #         "pool_os_type": minion_pool.os_type,
    #         "pool_identifier": minion_pool.id,
    #         # TODO(aznashwan): remove redundancy once transfer
    #         # action DB models have been overhauled:
    #         "pool_environment_options": minion_pool.source_environment}

    #     validate_task_type = (
    #         constants.TASK_TYPE_VALIDATE_DESTINATION_MINION_POOL_OPTIONS)
    #     set_up_task_type = (
    #         constants.TASK_TYPE_SET_UP_DESTINATION_POOL_SHARED_RESOURCES)
    #     if minion_pool.platform == constants.PROVIDER_PLATFORM_SOURCE:
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

    # @minion_manager_utils.minion_pool_synchronized_op
    # def tear_down_shared_minion_pool_resources(
    #         self, ctxt, minion_pool_id, force=False):
    #     minion_pool = db_api.get_minion_pool_lifecycle(
    #         ctxt, minion_pool_id, include_tasks_executions=False,
    #         include_machines=False)
    #     if minion_pool.status != (
    #             constants.MINION_POOL_STATUS_DEALLOCATED) and not force:
    #         raise exception.InvalidMinionPoolState(
    #             "Minion Pool '%s' cannot have shared resources torn down as it"
    #             " is in '%s' state instead of the expected %s. "
    #             "Please use the force flag if you are certain you want "
    #             "to tear down the shared resources for this pool." % (
    #                 minion_pool_id, minion_pool.status,
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
    #     if minion_pool.platform == constants.PROVIDER_PLATFORM_SOURCE:
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

    # @minion_manager_utils.minion_pool_synchronized_op
    # def allocate_minion_pool_machines(self, ctxt, minion_pool_id):
    #     LOG.info("Attempting to allocate Minion Pool '%s'.", minion_pool_id)
    #     minion_pool = self._get_minion_pool(
    #         ctxt, minion_pool_id, include_tasks_executions=False,
    #         include_machines=True)
    #     if minion_pool.status != constants.MINION_POOL_STATUS_DEALLOCATED:
    #         raise exception.InvalidMinionPoolState(
    #             "Minion machines for pool '%s' cannot be allocated as the pool"
    #             " is in '%s' state instead of the expected %s."% (
    #                 minion_pool_id, minion_pool.status,
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
    #     if minion_pool.platform == constants.PROVIDER_PLATFORM_SOURCE:
    #         create_minion_task_type = (
    #             constants.TASK_TYPE_CREATE_SOURCE_MINION_MACHINE)
    #         delete_minion_task_type = (
    #             constants.TASK_TYPE_DELETE_DESTINATION_MINION_MACHINE)

    #     for minion_machine_id in new_minion_machine_ids:
    #         minion_pool.info[minion_machine_id] = {
    #             "pool_identifier": minion_pool_id,
    #             "pool_os_type": minion_pool.os_type,
    #             "pool_shared_resources": minion_pool.shared_resources,
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

    # @minion_manager_utils.minion_pool_synchronized_op
    # def deallocate_minion_pool_machines(self, ctxt, minion_pool_id, force=False):
    #     LOG.info("Attempting to deallocate Minion Pool '%s'.", minion_pool_id)
    #     minion_pool = db_api.get_minion_pool_lifecycle(
    #         ctxt, minion_pool_id, include_tasks_executions=False,
    #         include_machines=True)
    #     if minion_pool.status not in (
    #             constants.MINION_POOL_STATUS_ALLOCATED) and not force:
    #         raise exception.InvalidMinionPoolState(
    #             "Minion Pool '%s' cannot be deallocated as it is in '%s' "
    #             "state instead of the expected '%s'. Please use the "
    #             "force flag if you are certain you want to deallocate "
    #             "the minion pool's machines." % (
    #                 minion_pool_id, minion_pool.status,
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
    #     if minion_pool.platform == constants.PROVIDER_PLATFORM_SOURCE:
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

    @minion_manager_utils.minion_pool_synchronized_op
    def get_minion_pool(self, ctxt, minion_pool_id):
        return self._get_minion_pool(
            ctxt, minion_pool_id, include_machines=True, include_events=True,
            include_progress_updates=True)

    @minion_manager_utils.minion_pool_synchronized_op
    def update_minion_pool(self, ctxt, minion_pool_id, updated_values):
        minion_pool = self._get_minion_pool(
            ctxt, minion_pool_id, include_machines=False)
        if minion_pool.status != constants.MINION_POOL_STATUS_DEALLOCATED:
            raise exception.InvalidMinionPoolState(
                "Minion Pool '%s' cannot be updated as it is in '%s' status "
                "instead of the expected '%s'. Please ensure the pool machines"
                "have been deallocated and the pool's supporting resources "
                "have been torn down before updating the pool." % (
                    minion_pool_id, minion_pool.status,
                    constants.MINION_POOL_STATUS_DEALLOCATED))
        LOG.info(
            "Attempting to update minion_pool '%s' with payload: %s",
            minion_pool_id, updated_values)
        db_api.update_minion_pool(ctxt, minion_pool_id, updated_values)
        LOG.info("Minion Pool '%s' successfully updated", minion_pool_id)
        return db_api.get_minion_pool(ctxt, minion_pool_id)

    @minion_manager_utils.minion_pool_synchronized_op
    def delete_minion_pool(self, ctxt, minion_pool_id):
        minion_pool = self._get_minion_pool(
            ctxt, minion_pool_id, include_machines=True)
        acceptable_deletion_statuses = [
            constants.MINION_POOL_STATUS_DEALLOCATED,
            constants.MINION_POOL_STATUS_ERROR]
        if minion_pool.status not in acceptable_deletion_statuses:
            raise exception.InvalidMinionPoolState(
                "Minion Pool '%s' cannot be deleted as it is in '%s' status "
                "instead of one of the expected '%s'. Please ensure the pool "
                "machines have been deallocated and the pool's supporting "
                "resources have been torn down before deleting the pool." % (
                    minion_pool_id, minion_pool.status,
                    acceptable_deletion_statuses))

        LOG.info("Deleting minion pool with ID '%s'" % minion_pool_id)
        db_api.delete_minion_pool(ctxt, minion_pool_id)

    # @minion_manager_utils.minion_pool_synchronized_op
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

    # @staticmethod
    # def _update_minion_pool_status_for_finished_execution(
    #         ctxt, execution, new_execution_status):
    #     # status map if execution is active:
    #     stat_map = {
    #         constants.EXECUTION_TYPE_MINION_POOL_ALLOCATE_MINIONS:
    #             constants.MINION_POOL_STATUS_ALLOCATING,
    #         constants.EXECUTION_TYPE_MINION_POOL_DEALLOCATE_MINIONS:
    #             constants.MINION_POOL_STATUS_DEALLOCATING,
    #         constants.EXECUTION_TYPE_MINION_POOL_SET_UP_SHARED_RESOURCES:
    #             constants.MINION_POOL_STATUS_INITIALIZING,
    #         constants.EXECUTION_TYPE_MINION_POOL_TEAR_DOWN_SHARED_RESOURCES:
    #             constants.MINION_POOL_STATUS_UNINITIALIZING}
    #     if new_execution_status == constants.EXECUTION_STATUS_COMPLETED:
    #         stat_map = {
    #             constants.EXECUTION_TYPE_MINION_POOL_ALLOCATE_MINIONS:
    #                 constants.MINION_POOL_STATUS_ALLOCATED,
    #             constants.EXECUTION_TYPE_MINION_POOL_DEALLOCATE_MINIONS:
    #                 constants.MINION_POOL_STATUS_DEALLOCATED,
    #             constants.EXECUTION_TYPE_MINION_POOL_SET_UP_SHARED_RESOURCES:
    #                 constants.MINION_POOL_STATUS_DEALLOCATED,
    #             constants.EXECUTION_TYPE_MINION_POOL_TEAR_DOWN_SHARED_RESOURCES:
    #                 constants.MINION_POOL_STATUS_UNINITIALIZED}
    #     elif new_execution_status in constants.FINALIZED_TASK_STATUSES:
    #         stat_map = {
    #             constants.EXECUTION_TYPE_MINION_POOL_ALLOCATE_MINIONS:
    #                 constants.MINION_POOL_STATUS_DEALLOCATED,
    #             constants.EXECUTION_TYPE_MINION_POOL_DEALLOCATE_MINIONS:
    #                 constants.MINION_POOL_STATUS_ALLOCATED,
    #             constants.EXECUTION_TYPE_MINION_POOL_SET_UP_SHARED_RESOURCES:
    #                 constants.MINION_POOL_STATUS_UNINITIALIZED,
    #             constants.EXECUTION_TYPE_MINION_POOL_TEAR_DOWN_SHARED_RESOURCES:
    #                 constants.MINION_POOL_STATUS_UNINITIALIZED}
    #     final_pool_status = stat_map.get(execution.type)
    #     if not final_pool_status:
    #         LOG.error(
    #             "Could not determine pool status following transition of "
    #             "execution '%s' (type '%s') to status '%s'. Presuming error "
    #             "has occured. Marking piil as error'd.",
    #             execution.id, execution.type, new_execution_status)
    #         final_pool_status = constants.MINION_POOL_STATUS_ERROR

    #     LOG.info(
    #         "Marking minion pool '%s' status as '%s' in the DB following the "
    #         "transition of execution '%s' (type '%s') to status '%s'.",
    #         execution.action_id, final_pool_status, execution.id,
    #         execution.type, new_execution_status)
    #     db_api.set_minion_pool_status(
    #         ctxt, execution.action_id, final_pool_status)

    # def deallocate_minion_machines_for_action(self, ctxt, action_id):
    #     if not isinstance(action, dict):
    #         raise exception.InvalidInput(
    #             "Action must be a dict, got '%s': %s" % (
    #                 type(action), action))
    #     required_action_properties = [
    #         'id', 'instances', 'origin_minion_pool_id',
    #         'destination_minion_pool_id',
    #         'instance_osmorphing_minion_pool_mappings']
    #     missing = [
    #         prop for prop in required_action_properties
    #         if prop not in action]
    #     if missing:
    #         raise exception.InvalidInput(
    #             "Missing the following required action properties for "
    #             "minion pool machine deallocation: %s. Got %s" % (
    #                 missing, action))

    #     minion_pool_ids = set()
    #     if action['origin_minion_pool_id']:
    #         minion_pool_ids.add(action['origin_minion_pool_id'])
    #     if action['destination_minion_pool_id']:
    #         minion_pool_ids.add(action['destination_minion_pool_id'])
    #     if action['instance_osmorphing_minion_pool_mappings']:
    #         minion_pool_ids = minion_pool_ids.union(set(
    #             action['instance_osmorphing_minion_pool_mappings'].values()))
    #     if None in minion_pool_ids:
    #         minion_pool_ids.remove(None)

    #     if not minion_pool_ids:
    #         LOG.debug(
    #             "No minion pools seem to have been used for action with "
    #             "base_id '%s'. Skipping minion machine deallocation.",
    #             action['id'])
    #     else:
    #         LOG.debug(
    #             "Attempting to deallocate all minion pool machine selections "
    #             "for action '%s'. Afferent pools are: %s",
    #             action['id'], minion_pool_ids)

    #         with contextlib.ExitStack() as stack:
    #             _ = [
    #                 stack.enter_context(
    #                     lockutils.lock(
    #                         constants.MINION_POOL_LOCK_NAME_FORMAT % pool_id,
    #                         external=True))
    #                 for pool_id in minion_pool_ids]

    #             minion_machines = db_api.get_minion_machines(
    #                 ctxt, allocated_action_id=action['id'])
    #             machine_ids = [m.id for m in minion_machines]
    #             if machine_ids:
    #                 LOG.info(
    #                     "Releasing the following minion machines for "
    #                     "action '%s': %s", action['base_id'], machine_ids)
    #                 db_api.set_minion_machines_allocation_statuses(
    #                     ctxt, machine_ids, None,
    #                     constants.MINION_MACHINE_STATUS_AVAILABLE)
    #             else:
    #                 LOG.debug(
    #                     "No minion machines were found to be associated "
    #                     "with action with base_id '%s'.", action['base_id'])
