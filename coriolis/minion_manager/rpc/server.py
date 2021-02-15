# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import datetime
import math
import uuid

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import timeutils
from taskflow import deciders as taskflow_deciders
from taskflow.patterns import graph_flow
from taskflow.patterns import linear_flow
from taskflow.patterns import unordered_flow

from coriolis import constants
from coriolis import context
from coriolis import exception
from coriolis import keystone
from coriolis import utils
from coriolis.conductor.rpc import client as rpc_conductor_client
from coriolis.cron import cron
from coriolis.db import api as db_api
from coriolis.db.sqlalchemy import models
from coriolis.minion_manager.rpc import client as rpc_minion_manager_client
from coriolis.minion_manager.rpc import tasks as minion_manager_tasks
from coriolis.minion_manager.rpc import utils as minion_manager_utils
from coriolis.scheduler.rpc import client as rpc_scheduler_client
from coriolis.taskflow import runner as taskflow_runner
from coriolis.taskflow import utils as taskflow_utils
from coriolis.worker.rpc import client as rpc_worker_client


VERSION = "1.0"

LOG = logging.getLogger(__name__)

MINION_MANAGER_OPTS = [
    cfg.IntOpt(
        "minion_pool_default_refresh_period_minutes",
        default=10,
        help="Number of minutes in which to refresh minion pools."
             "Set to 0 to completely disable automatic refreshing.")]

CONF = cfg.CONF
CONF.register_opts(MINION_MANAGER_OPTS, 'minion_manager')

MINION_POOL_REFRESH_JOB_PREFIX_FORMAT = "pool-%s-refresh"
MINION_POOL_REFRESH_CRON_JOB_NAME_FORMAT = "pool-%s-refresh-minute-%d"
MINION_POOL_REFRESH_CRON_JOB_DESCRIPTION_FORMAT = (
    "Regularly scheduled refresh job for minion pool '%s' on minute %d.")


def _trigger_pool_refresh(ctxt, minion_manager_client, minion_pool_id):
    try:
        minion_manager_client.refresh_minion_pool(
            ctxt, minion_pool_id)
    except exception.InvalidMinionPoolState as ex:
        LOG.warn(
            "Minion Pool '%s' is in an invalid state for having a refresh run."
            " Skipping for now. Error was: %s", minion_pool_id, str(ex))


class MinionManagerServerEndpoint(object):

    def __init__(self):
        self._admin_ctxt = context.get_admin_context()
        self._scheduler_client_instance = None
        self._worker_client_instance = None
        self._conductor_client_instance = None
        self._replica_cron_client_instance = None
        self._minion_manager_client_instance = None
        try:
            self._cron = cron.Cron()
            self._init_pools_refresh_cron_jobs()
            self._cron.start()
        except Exception as ex:
            LOG.warn(
                "A fatal exception occurred while attempting to set up cron "
                "jobs for automatic pool refreshing. Automatic refreshing will"
                " not be perfomed until the issue is fixed and the service is "
                "restarted. Exception details were: %s",
                utils.get_exception_details())

    def _init_pools_refresh_cron_jobs(self):
        minion_pools = db_api.get_minion_pools(
            self._admin_ctxt, include_machines=False,
            include_progress_updates=False, include_events=False,
            to_dict=False)

        for minion_pool in minion_pools:
            active_pool_statuses = [constants.MINION_POOL_STATUS_ALLOCATED]
            if minion_pool.status not in active_pool_statuses:
                LOG.debug(
                    "Not setting any refresh schedules for minion pool '%s' "
                    "as it is in an inactive status '%s'.",
                    minion_pool.id, minion_pool.status)
                continue

            if not minion_pool.maintenance_trust_id:
                LOG.warn(
                    "Minion Pool with ID '%s' had no maintenance trust "
                    "ID associated with it. Cannot set up automatic "
                    "refreshing during startup. Skipping.",
                    minion_pool.id)
                continue

            LOG.debug(
                "Adding refresh schedule for minion pool '%s' as part of "
                "server startup.", minion_pool.id)
            try:
                self._register_refresh_jobs_for_minion_pool(minion_pool)
            except Exception as ex:
                LOG.warn(
                    "An Exception occurred while setting up automatic "
                    "refreshing for minion pool with ID '%s'. Error was: %s",
                    minion_pool.id, utils.get_exception_details())

    def _register_refresh_jobs_for_minion_pool(
            self, minion_pool, period_minutes=None):
        if period_minutes is None:
            period_minutes = CONF.minion_manager.minion_pool_default_refresh_period_minutes

        if period_minutes < 0:
            LOG.warn(
                "Got negative pool refresh period %s. Defaulting to 0.",
                period_minutes)
            period_minutes = 0

        if period_minutes == 0:
            LOG.info(
                "Minon pool refresh period is set to zero. Not setting up "
                "any automatic refresh jobs for Minion Pool '%s'.",
                minion_pool.id)
            return

        if period_minutes > 60:
            LOG.warn(
                "Selected pool refresh period_minutes is greater than 60, defaulting "
                "to 10. Original value was: %s", period_minutes)
            period_minutes = 10
        admin_ctxt = context.get_admin_context(
            minion_pool.maintenance_trust_id)
        description = (
            "Scheduled refresh job for minion pool '%s'" % minion_pool.id)

        # NOTE: we need to generate hourly schedules for each minute in
        # the hour we would like the refresh to be triggered:
        for minute in [
                period_minutes * i for i in range(
                    math.ceil(60 / period_minutes))]:
            name = MINION_POOL_REFRESH_CRON_JOB_NAME_FORMAT % (
                minion_pool.id, minute)
            description = MINION_POOL_REFRESH_CRON_JOB_DESCRIPTION_FORMAT % (
                minion_pool.id, minute)
            self._cron.register(
                cron.CronJob(
                    name, description, {"minute": minute}, True, None, None,
                    None, _trigger_pool_refresh, admin_ctxt,
                    self._rpc_minion_manager_client, minion_pool.id))

    def _unregister_refresh_jobs_for_minion_pool(
            self, minion_pool, raise_on_error=True):
        job_prefix = MINION_POOL_REFRESH_JOB_PREFIX_FORMAT % (
            minion_pool.id)
        try:
            self._cron.unregister_jobs_with_prefix(job_prefix)
        except Exception as ex:
            if not raise_on_error:
                LOG.warn(
                    "Exception occurred while unregistering minion pool "
                    "refresh  cron jobs for pool with ID '%s'. "
                    "Exception was: %s",
                    minion_pool.id, utils.get_exception_details())
            else:
                raise

    @property
    def _taskflow_runner(self):
        return taskflow_runner.TaskFlowRunner(
            constants.MINION_MANAGER_MAIN_MESSAGING_TOPIC,
            max_workers=25)

    # NOTE(aznashwan): it is unsafe to fork processes with pre-instantiated
    # oslo_messaging clients as the underlying eventlet thread queues will
    # be invalidated. Considering this class both serves from a "main
    # process" as well as forking child processes, it is safest to
    # instantiate the clients only when needed:
    @property
    def _rpc_worker_client(self):
        if not getattr(self, '_worker_client_instance', None):
            self._worker_client_instance = (
                rpc_worker_client.WorkerClient())
        return self._worker_client_instance

    @property
    def _rpc_scheduler_client(self):
        if not getattr(self, '_scheduler_client_instance', None):
            self._scheduler_client_instance = (
                rpc_scheduler_client.SchedulerClient())
        return self._scheduler_client_instance

    @property
    def _rpc_conductor_client(self):
        if not getattr(self, '_conductor_client_instance', None):
            self._conductor_client_instance = (
                rpc_conductor_client.ConductorClient())
        return self._conductor_client_instance

    @property
    def _rpc_minion_manager_client(self):
        if not getattr(self, '_minion_manager_client_instance', None):
            self._minion_manager_client_instance = (
                rpc_minion_manager_client.MinionManagerClient())
        return self._minion_manager_client_instance

    def get_diagnostics(self, ctxt):
        return utils.get_diagnostics_info()

    def get_endpoint_source_minion_pool_options(
            self, ctxt, endpoint_id, env, option_names):
        endpoint = self._rpc_conductor_client.get_endpoint(ctxt, endpoint_id)

        worker_service = self._rpc_scheduler_client.get_worker_service_for_specs(
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
        endpoint = self._rpc_conductor_client.get_endpoint(ctxt, endpoint_id)

        worker_service = self._rpc_scheduler_client.get_worker_service_for_specs(
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
        endpoint = self._rpc_conductor_client.get_endpoint(ctxt, endpoint_id)

        worker_service = self._rpc_scheduler_client.get_worker_service_for_specs(
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
        endpoint = self._rpc_conductor_client.get_endpoint(ctxt, endpoint_id)

        worker_service = self._rpc_scheduler_client.get_worker_service_for_specs(
            ctxt, enabled=True,
            region_sets=[[reg['id'] for reg in endpoint['mapped_regions']]],
            provider_requirements={
                endpoint['type']: [
                    constants.PROVIDER_TYPE_DESTINATION_MINION_POOL]})
        worker_rpc = rpc_worker_client.WorkerClient.from_service_definition(
            worker_service)

        return worker_rpc.validate_endpoint_destination_minion_pool_options(
            ctxt, endpoint['type'], pool_environment)

    def _add_minion_pool_event(self, ctxt, minion_pool_id, level, message):
        LOG.info(
            "Minion pool event for pool %s: %s", minion_pool_id, message)
        pool = db_api.get_minion_pool(ctxt, minion_pool_id)
        db_api.add_minion_pool_event(ctxt, pool.id, level, message)

    @minion_manager_utils.minion_pool_synchronized_op
    def add_minion_pool_event(self, ctxt, minion_pool_id, level, message):
        self._add_minion_pool_event(ctxt, minion_pool_id, level, message)

    def _add_minion_pool_progress_update(
            self, ctxt, minion_pool_id, message, initial_step=0, total_steps=0):
        LOG.info(
            "Adding pool progress update for %s: %s", minion_pool_id, message)
        db_api.add_minion_pool_progress_update(
            ctxt, minion_pool_id, message, initial_step=initial_step,
            total_steps=total_steps)

    @minion_manager_utils.minion_pool_synchronized_op
    def add_minion_pool_progress_update(
            self, ctxt, minion_pool_id, message, initial_step=0, total_steps=0):
        self._add_minion_pool_progress_update(
            ctxt, minion_pool_id, message, initial_step=initial_step,
            total_steps=total_steps)

    @minion_manager_utils.minion_pool_synchronized_op
    def update_minion_pool_progress_update(
            self, ctxt, minion_pool_id, progress_update_index,
            new_current_step, new_total_steps=None, new_message=None):
        LOG.info(
            "Updating minion pool '%s' progress update '%s': %s",
            minion_pool_id, progress_update_index, new_current_step)
        db_api.update_minion_pool_progress_update(
            ctxt, minion_pool_id, progress_update_index, new_current_step,
            new_total_steps=new_total_steps, new_message=new_message)

    def _check_keys_for_action_dict(
            self, action, required_action_properties, operation=None):
        if not isinstance(action, dict):
            raise exception.InvalidInput(
                "Action must be a dict, got '%s': %s" % (
                    type(action), action))
        missing = [
            prop for prop in required_action_properties
            if prop not in action]
        if missing:
            raise exception.InvalidInput(
                "Missing the following required action properties for "
                "%s: %s. Got %s" % (
                    operation, missing, action))

    def validate_minion_pool_selections_for_action(self, ctxt, action):
        """ Validates the minion pool selections for a given action. """
        required_action_properties = [
            'id', 'origin_endpoint_id', 'destination_endpoint_id',
            'origin_minion_pool_id', 'destination_minion_pool_id',
            'instance_osmorphing_minion_pool_mappings', 'instances']
        self._check_keys_for_action_dict(
            action, required_action_properties,
            operation="minion pool selection validation")

        minion_pools = {
            pool.id: pool
            # NOTE: we can just load all the pools in one go to
            # avoid extraneous DB queries:
            for pool in db_api.get_minion_pools(
                ctxt, include_machines=False, include_events=False,
                include_progress_updates=False, to_dict=False)}
        def _get_pool(pool_id):
            pool = minion_pools.get(pool_id)
            if not pool:
                raise exception.NotFound(
                    "Could not find minion pool with ID '%s'." % pool_id)
            return pool
        def _check_pool_minion_count(
                minion_pool, instances, minion_pool_type=""):
            desired_minion_count = len(instances)
            if minion_pool.status != constants.MINION_POOL_STATUS_ALLOCATED:
                raise exception.InvalidMinionPoolState(
                    "Minion Pool '%s' is an invalid state ('%s') to be "
                    "used as a %s pool for action '%s'. The pool must be "
                    "in '%s' status."  % (
                        minion_pool.id, minion_pool.status,
                        minion_pool_type.lower(), action['id'],
                        constants.MINION_POOL_STATUS_ALLOCATED))
            if desired_minion_count > minion_pool.maximum_minions:
                msg = (
                    "Minion Pool with ID '%s' has a lower maximum minion "
                    "count (%d) than the requested number of minions "
                    "(%d) to handle all of the instances of action '%s': "
                    "%s" % (
                        minion_pool.id, minion_pool.maximum_minions,
                        desired_minion_count, action['id'], instances))
                if minion_pool_type:
                    msg = "%s %s" % (minion_pool_type, msg)
                raise exception.InvalidMinionPoolSelection(msg)

        # check source pool:
        instances = action['instances']
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
            _check_pool_minion_count(
                origin_pool, instances, minion_pool_type="Source")
            LOG.debug(
                "Successfully validated compatibility of origin minion pool "
                "'%s' for use with action '%s'." % (
                    action['origin_minion_pool_id'], action['id']))

        # check destination pool:
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
            _check_pool_minion_count(
                destination_pool, instances,
                minion_pool_type="Destination")
            LOG.debug(
                "Successfully validated compatibility of destination minion "
                "pool '%s' for use with action '%s'." % (
                    action['origin_minion_pool_id'], action['id']))

        # check OSMorphing pool(s):
        instance_osmorphing_minion_pool_mappings = action.get(
            'instance_osmorphing_minion_pool_mappings')
        if instance_osmorphing_minion_pool_mappings:
            osmorphing_pool_mappings = {}
            for (instance_id, pool_id) in (
                    instance_osmorphing_minion_pool_mappings).items():
                if instance_id not in instances:
                    LOG.warn(
                        "Ignoring OSMorphing pool validation for instance with"
                        " ID '%s' (mapped pool '%s') as it is not part of  "
                        "action '%s's declared instances: %s",
                        instance_id, pool_id, action['id'], instances)
                    continue
                if pool_id not in osmorphing_pool_mappings:
                    osmorphing_pool_mappings[pool_id] = [instance_id]
                else:
                    osmorphing_pool_mappings[pool_id].append(instance_id)

            for (pool_id, instances_to_osmorph) in osmorphing_pool_mappings.items():
                osmorphing_pool = _get_pool(pool_id)
                if osmorphing_pool.endpoint_id != (
                        action['destination_endpoint_id']):
                    raise exception.InvalidMinionPoolSelection(
                        "The selected OSMorphing minion pool for instances %s"
                        " ('%s') belongs to a different Coriolis endpoint "
                        "('%s') than the destination endpoint ('%s')" % (
                            instances_to_osmorph, pool_id,
                            osmorphing_pool.endpoint_id,
                            action['destination_endpoint_id']))
                if osmorphing_pool.platform != (
                        constants.PROVIDER_PLATFORM_DESTINATION):
                    raise exception.InvalidMinionPoolSelection(
                        "The selected OSMorphing minion pool for instances %s "
                        "('%s') is configured as a '%s' pool. The pool must "
                        "be of type %s to be used for OSMorphing." % (
                            instances_to_osmorph, pool_id,
                            osmorphing_pool.platform,
                            constants.PROVIDER_PLATFORM_DESTINATION))
                _check_pool_minion_count(
                    osmorphing_pool, instances_to_osmorph,
                    minion_pool_type="OSMorphing")
                LOG.debug(
                    "Successfully validated compatibility of destination "
                    "minion pool '%s' for use as OSMorphing minion for "
                    "instances %s during action '%s'." % (
                        pool_id, instances_to_osmorph, action['id']))
        LOG.debug(
            "Successfully validated minion pool selections for action '%s' "
            "with properties: %s", action['id'], action)

    def allocate_minion_machines_for_replica(
            self, ctxt, replica):
        try:
            minion_allocations = self._run_machine_allocation_subflow_for_action(
                ctxt, replica, constants.TRANSFER_ACTION_TYPE_REPLICA,
                include_transfer_minions=True,
                include_osmorphing_minions=False)
        except Exception as ex:
            LOG.warn(
                "Error occurred while allocating minion machines for "
                "Replica with ID '%s'. Removing all allocations. "
                "Error was: %s" % (
                    replica['id'], utils.get_exception_details()))
            self._cleanup_machines_with_statuses_for_action(
                ctxt, replica['id'],
                [constants.MINION_MACHINE_STATUS_UNINITIALIZED])
            self.deallocate_minion_machines_for_action(
                ctxt, replica['id'])
            self._rpc_conductor_client.report_replica_minions_allocation_error(
                ctxt, replica['id'], str(ex))
            raise

    def allocate_minion_machines_for_migration(
            self, ctxt, migration, include_transfer_minions=True,
            include_osmorphing_minions=True):
        try:
            self._run_machine_allocation_subflow_for_action(
                ctxt, migration,
                constants.TRANSFER_ACTION_TYPE_MIGRATION,
                include_transfer_minions=include_transfer_minions,
                include_osmorphing_minions=include_osmorphing_minions)
        except Exception as ex:
            LOG.warn(
                "Error occurred while allocating minion machines for "
                "Migration with ID '%s'. Removing all allocations. "
                "Error was: %s" % (
                    migration['id'], utils.get_exception_details()))
            self._cleanup_machines_with_statuses_for_action(
                ctxt, migration['id'],
                [constants.MINION_MACHINE_STATUS_UNINITIALIZED])
            self.deallocate_minion_machines_for_action(
                ctxt, migration['id'])
            self._rpc_conductor_client.report_migration_minions_allocation_error(
                ctxt, migration['id'], str(ex))
            raise

    def _make_minion_machine_allocation_subflow_for_action(
            self, ctxt, minion_pool, action_id, action_instances,
            subflow_name, inject_for_tasks=None):
        """ Creates a subflow for allocating minion machines from the
        provided minion pool to the given action (one for each instance)

        Returns a mapping between the action's instaces' IDs and the minion
        machine ID, as well as the subflow to execute for said machines.

        Returns dict of the form: {
            "flow": TheFlowClass(),
            "action_instance_minion_allocation_mappings": {
                "<action_instance_id>": "<allocated_minion_id>"}}
        """
        currently_available_machines = [
            machine for machine in minion_pool.minion_machines
            if machine.allocation_status == constants.MINION_MACHINE_STATUS_AVAILABLE]
        extra_available_machine_slots = (
            minion_pool.maximum_minions - len(minion_pool.minion_machines))
        num_instances = len(action_instances)
        num_currently_available_machines = len(currently_available_machines)
        if num_instances > (len(currently_available_machines) + (
                                extra_available_machine_slots)):
            raise exception.InvalidMinionPoolState(
                "Minion pool '%s' is unable to accommodate the requested "
                "number of machines (%s) for transfer action '%s', as it only "
                "has %d currently available machines, with room to upscale a "
                "further %d until the maximum is reached. Please either "
                "increase the number of maximum machines for the pool "
                "or wait for other minions to become available before "
                "retrying." % (
                    minion_pool.id, num_instances, action_id,
                    num_currently_available_machines,
                    extra_available_machine_slots))

        def _select_machine(minion_pool, exclude=None):
            selected_machine = None
            # NOTE(aznashwan): this will iterate through machines in a set
            # order every time, thus ensuring that some are preferred over
            # others and facilitating some to be left unused and thus torn
            # down during the periodic refreshes:
            for machine in minion_pool.minion_machines:
                if exclude and machine.id in exclude:
                    LOG.debug(
                        "Excluding minion machine '%s' from search for use "
                        "action '%s'", machine.id, action_id)
                    continue
                if machine.allocation_status != constants.MINION_MACHINE_STATUS_AVAILABLE:
                    LOG.debug(
                        "Minion machine with ID '%s' is in status '%s' "
                        "instead of the expected '%s'. Skipping for use "
                        "with action '%s'.",
                        machine.id, machine.allocation_status,
                        constants.MINION_MACHINE_STATUS_AVAILABLE, action_id)
                    continue
                selected_machine = machine
                break
            return selected_machine

        allocation_subflow = unordered_flow.Flow(subflow_name)
        instance_minion_allocations = {}
        machine_db_entries_to_add = []
        existing_machines_to_allocate = {}
        for instance in action_instances:

            if instance in instance_minion_allocations:
                raise exception.InvalidInput(
                    "Instance with identifier '%s' passed twice for "
                    "minion machine allocation from pool '%s' for action "
                    "'%s'. Full instances list was: %s" % (
                        instance, minion_pool.id, action_id, action_instances))
            minion_machine = _select_machine(
                minion_pool, exclude=instance_minion_allocations.values())
            if minion_machine:
                # take note of the machine and setup a healthcheck:
                instance_minion_allocations[instance] = minion_machine.id
                existing_machines_to_allocate[minion_machine.id] = instance
                LOG.debug(
                    "Allocating pre-existing machine '%s' from pool '%s' for "
                    "use with action with ID '%s'.",
                    minion_machine.id, minion_pool.id, action_id)
                allocation_subflow.add(
                    self._get_healtchcheck_flow_for_minion_machine(
                        minion_pool, minion_machine,
                        allocate_to_action=action_id,
                        power_on_machine=True,
                        inject_for_tasks=inject_for_tasks,
                        machine_status_on_success=(
                            constants.MINION_MACHINE_STATUS_IN_USE)))
            else:
                # add task which creates the new machine:
                new_machine_id = str(uuid.uuid4())
                LOG.debug(
                    "New minion machine with ID '%s' will be created for "
                    "minion pool '%s' for use with action '%s'.",
                    new_machine_id, minion_pool.id, action_id)

                new_minion_machine = models.MinionMachine()
                new_minion_machine.id = new_machine_id
                new_minion_machine.pool_id = minion_pool.id
                new_minion_machine.allocation_status = (
                    constants.MINION_MACHINE_STATUS_UNINITIALIZED)
                new_minion_machine.power_status = (
                    constants.MINION_MACHINE_POWER_STATUS_UNINITIALIZED)
                new_minion_machine.allocated_action = action_id
                machine_db_entries_to_add.append(new_minion_machine)

                instance_minion_allocations[instance] = new_machine_id
                allocation_subflow.add(
                    minion_manager_tasks.AllocateMinionMachineTask(
                        minion_pool.id, new_machine_id, minion_pool.platform,
                        allocate_to_action=action_id,
                        raise_on_cleanup_failure=False,
                        inject=inject_for_tasks))

        new_machine_db_entries_added = []
        try:
            if existing_machines_to_allocate:
                # mark any existing machines as allocated:
                LOG.debug(
                    "Marking the following pre-existing minion machines "
                    "from pool '%s' of action '%s' for each instance as "
                    "allocated with the DB: %s",
                    minion_pool.id, action_id, existing_machines_to_allocate)
                db_api.set_minion_machines_allocation_statuses(
                    ctxt, list(existing_machines_to_allocate.keys()),
                    action_id, constants.MINION_MACHINE_STATUS_RESERVED,
                    refresh_allocation_time=True)
                self._add_minion_pool_event(
                    ctxt, minion_pool.id, constants.TASK_EVENT_INFO,
                    "The following pre-existing minion machines will be "
                    "allocated to transfer action '%s': %s" % (
                        action_id, list(existing_machines_to_allocate.keys())))

            # add any new machine entries to the DB:
            if machine_db_entries_to_add:
                for new_machine in machine_db_entries_to_add:
                    LOG.info(
                        "Adding new minion machine with ID '%s' to the DB for pool "
                        "'%s' for use with action '%s'.",
                        new_machine_id, minion_pool.id, action_id)
                    db_api.add_minion_machine(ctxt, new_machine)
                    new_machine_db_entries_added.append(new_machine.id)
                self._add_minion_pool_event(
                    ctxt, minion_pool.id, constants.TASK_EVENT_INFO,
                    "The following new minion machines will be created for use"
                    " in transfer action '%s': %s" % (
                        action_id, [m.id for m in machine_db_entries_to_add]))
        except Exception as ex:
            LOG.warn(
                "Exception occurred while adding new minion machine entries to"
                " the DB for pool '%s' for use with action '%s'. Clearing "
                "any DB entries added so far (%s). Error was: %s",
                minion_pool.id, action_id,
                [m.id for m in new_machine_db_entries_added],
                utils.get_exception_details())
            try:
                LOG.debug(
                    "Reverting the following pre-existing minion machines from"
                    " pool '%s' to '%s' due to allocation error for action "
                    "'%s': %s",
                    minion_pool.id,
                    constants.MINION_MACHINE_STATUS_AVAILABLE,
                    action_id,
                    list(existing_machines_to_allocate.keys()))
                db_api.set_minion_machines_allocation_statuses(
                    ctxt, list(existing_machines_to_allocate.keys()),
                    None, constants.MINION_MACHINE_STATUS_AVAILABLE,
                    refresh_allocation_time=False)
            except Exception:
                LOG.warn(
                    "Failed to deallocate the following machines from pool "
                    "'%s' following allocation error for action '%s': %s. "
                    "Error trace was: %s",
                    minion_pool.id, action_id, existing_machines_to_allocate,
                    utils.get_exception_details())
            for new_machine in new_machine_db_entries_added:
                try:
                    db_api.delete_minion_machine(ctxt, new_machine.id)
                except Exception as ex:
                    LOG.warn(
                        "Error occurred while removing minion machine entry "
                        "'%s' from the DB. This may leave the pool in an "
                        "inconsistent state. Error trace was: %s" % (
                            new_machine.id, utils.get_exception_details()))
                    continue
            raise

        LOG.debug(
            "The following minion machine allocation from pool '%s' were or "
            "will be made for action '%s': %s",
            minion_pool.id, action_id, instance_minion_allocations)
        return {
            "flow": allocation_subflow,
            "action_instance_minion_allocation_mappings": (
                instance_minion_allocations)}

    def _run_machine_allocation_subflow_for_action(
            self, ctxt, action, action_type, include_transfer_minions=True,
            include_osmorphing_minions=True):
        """ Defines and starts a taskflow subflow for allocating minion
        machines for the given action.
        If there are no more minion machines available, upscaling will occur.
        Also adds to the DB/marks as allocated any minion machines on the
        spot.
        """
        required_action_properties = [
            'id', 'instances', 'origin_minion_pool_id',
            'destination_minion_pool_id',
            'instance_osmorphing_minion_pool_mappings']
        self._check_keys_for_action_dict(
            action, required_action_properties,
            operation="minion machine selection")

        allocation_flow_name_format = None
        machines_allocation_subflow_name_format = None
        machine_action_allocation_subflow_name_format = None
        allocation_failure_reporting_task_class = None
        allocation_confirmation_reporting_task_class = None
        if action_type == constants.TRANSFER_ACTION_TYPE_MIGRATION:
            allocation_flow_name_format = (
                minion_manager_tasks.MINION_POOL_MIGRATION_ALLOCATION_FLOW_NAME_FORMAT)
            allocation_failure_reporting_task_class = (
                minion_manager_tasks.ReportMinionAllocationFailureForMigrationTask)
            allocation_confirmation_reporting_task_class = (
                minion_manager_tasks.ConfirmMinionAllocationForMigrationTask)
            machines_allocation_subflow_name_format = (
                minion_manager_tasks.MINION_POOL_MIGRATION_ALLOCATION_SUBFLOW_NAME_FORMAT)
            machine_action_allocation_subflow_name_format = (
                minion_manager_tasks.MINION_POOL_ALLOCATE_MACHINES_FOR_MIGRATION_SUBFLOW_NAME_FORMAT)
        elif action_type == constants.TRANSFER_ACTION_TYPE_REPLICA:
            allocation_flow_name_format = (
                minion_manager_tasks.MINION_POOL_REPLICA_ALLOCATION_FLOW_NAME_FORMAT)
            allocation_failure_reporting_task_class = (
                minion_manager_tasks.ReportMinionAllocationFailureForReplicaTask)
            allocation_confirmation_reporting_task_class = (
                minion_manager_tasks.ConfirmMinionAllocationForReplicaTask)
            machines_allocation_subflow_name_format = (
                minion_manager_tasks.MINION_POOL_REPLICA_ALLOCATION_SUBFLOW_NAME_FORMAT)
            machine_action_allocation_subflow_name_format = (
                minion_manager_tasks.MINION_POOL_ALLOCATE_MACHINES_FOR_REPLICA_SUBFLOW_NAME_FORMAT)
        else:
            raise exception.InvalidInput(
                "Unknown transfer action type '%s'" % action_type)

        # define main flow:
        main_allocation_flow_name = (
            allocation_flow_name_format % action['id'])
        main_allocation_flow = linear_flow.Flow(main_allocation_flow_name)
        instance_machine_allocations = {
            instance: {} for instance in action['instances']}

        # add allocation failure reporting task:
        main_allocation_flow.add(
            allocation_failure_reporting_task_class(
                action['id']))

        # define subflow for all the pool minions allocations:
        machines_subflow = unordered_flow.Flow(
            machines_allocation_subflow_name_format % action['id'])
        new_pools_machines_db_entries = {}
        pools_used = []

        # add subflow for origin pool:
        if include_transfer_minions and action['origin_minion_pool_id']:
            pools_used.append(action['origin_minion_pool_id'])
            with minion_manager_utils.get_minion_pool_lock(
                    action['origin_minion_pool_id'], external=True):
                # fetch pool, origin endpoint, and initial store:
                minion_pool = self._get_minion_pool(
                    ctxt, action['origin_minion_pool_id'],
                    include_machines=True, include_events=False,
                    include_progress_updates=False)
                endpoint_dict = self._rpc_conductor_client.get_endpoint(
                    ctxt, minion_pool.endpoint_id)
                origin_pool_store = self._get_pool_initial_taskflow_store_base(
                    ctxt, minion_pool, endpoint_dict)

                # add subflow for machine allocations from origin pool:
                subflow_name = machine_action_allocation_subflow_name_format % (
                    minion_pool.id, action['id'])
                # NOTE: required to avoid internal taskflow conflicts
                subflow_name = "origin-%s" % subflow_name
                allocations_subflow_result = (
                    self._make_minion_machine_allocation_subflow_for_action(
                        ctxt, minion_pool, action['id'], action['instances'],
                        subflow_name, inject_for_tasks=origin_pool_store))
                machines_subflow.add(allocations_subflow_result['flow'])

                # register each instances' origin minion:
                source_machine_allocations = allocations_subflow_result[
                    'action_instance_minion_allocation_mappings']
                for (action_instance_id, allocated_minion_id) in (
                        source_machine_allocations.items()):
                    instance_machine_allocations[
                        action_instance_id]['origin_minion_id'] = (
                            allocated_minion_id)

        # add subflow for destination pool:
        if include_transfer_minions and action['destination_minion_pool_id']:
            pools_used.append(action['destination_minion_pool_id'])
            with minion_manager_utils.get_minion_pool_lock(
                    action['destination_minion_pool_id'], external=True):
                # fetch pool, destination endpoint, and initial store:
                minion_pool = self._get_minion_pool(
                    ctxt, action['destination_minion_pool_id'],
                    include_machines=True, include_events=False,
                    include_progress_updates=False)
                endpoint_dict = self._rpc_conductor_client.get_endpoint(
                    ctxt, minion_pool.endpoint_id)
                destination_pool_store = (
                    self._get_pool_initial_taskflow_store_base(
                        ctxt, minion_pool, endpoint_dict))

                # add subflow for machine allocations from destination pool:
                subflow_name = machine_action_allocation_subflow_name_format % (
                    minion_pool.id, action['id'])
                # NOTE: required to avoid internal taskflow conflicts
                subflow_name = "destination-%s" % subflow_name
                allocations_subflow_result = (
                    self._make_minion_machine_allocation_subflow_for_action(
                        ctxt, minion_pool, action['id'], action['instances'],
                        subflow_name,
                        inject_for_tasks=destination_pool_store))
                machines_subflow.add(allocations_subflow_result['flow'])
                destination_machine_allocations = allocations_subflow_result[
                    'action_instance_minion_allocation_mappings']

                # register each instances' destination minion:
                for (action_instance_id, allocated_minion_id) in (
                        destination_machine_allocations.items()):
                    instance_machine_allocations[
                        action_instance_id]['destination_minion_id'] = (
                            allocated_minion_id)

        # add subflow for OSMorphing minions:
        osmorphing_pool_instance_mappings = {}
        for (action_instance_id, mapped_pool_id) in action[
                'instance_osmorphing_minion_pool_mappings'].items():
            if mapped_pool_id not in osmorphing_pool_instance_mappings:
                osmorphing_pool_instance_mappings[
                    mapped_pool_id] = [action_instance_id]
            else:
                osmorphing_pool_instance_mappings[mapped_pool_id].append(
                    action_instance_id)
        if include_osmorphing_minions and osmorphing_pool_instance_mappings:
            for (osmorphing_pool_id, action_instance_ids) in (
                    osmorphing_pool_instance_mappings.items()):
                # if the destination pool was selected as an OSMorphing pool
                # for any instances, we simply re-use all of the destination
                # minions for said instances:
                if action['destination_minion_pool_id'] and (
                        include_osmorphing_minions and (
                            osmorphing_pool_id == (
                                action['destination_minion_pool_id']) and (
                                    include_transfer_minions))):
                    LOG.debug(
                        "Reusing destination minion pool with ID '%s' for the "
                        "following instances which had it selected as an "
                        "OSMorphing pool for action '%s': %s",
                        osmorphing_pool_id, action['id'], action_instance_ids)
                    for instance in action_instance_ids:
                        instance_machine_allocations[
                            instance]['osmorphing_minion_id'] = (
                                instance_machine_allocations[
                                    instance]['destination_minion_id'])
                    continue

                with minion_manager_utils.get_minion_pool_lock(
                        osmorphing_pool_id, external=True):
                    pools_used.append(osmorphing_pool_id)
                    # fetch pool, destination endpoint, and initial store:
                    minion_pool = self._get_minion_pool(
                        ctxt, osmorphing_pool_id,
                        include_machines=True, include_events=False,
                        include_progress_updates=False)
                    endpoint_dict = self._rpc_conductor_client.get_endpoint(
                        ctxt, minion_pool.endpoint_id)
                    osmorphing_pool_store = self._get_pool_initial_taskflow_store_base(
                        ctxt, minion_pool, endpoint_dict)

                    # add subflow for machine allocations from osmorphing pool:
                    subflow_name = machine_action_allocation_subflow_name_format % (
                        minion_pool.id, action['id'])
                    # NOTE: required to avoid internal taskflow conflicts
                    subflow_name = "osmorphing-%s" % subflow_name
                    allocations_subflow_result = (
                        self._make_minion_machine_allocation_subflow_for_action(
                            ctxt, minion_pool, action['id'],
                            action_instance_ids,
                            subflow_name, inject_for_tasks=osmorphing_pool_store))
                    machines_subflow.add(allocations_subflow_result['flow'])

                    # register each instances' osmorphing minion:
                    osmorphing_machine_allocations = allocations_subflow_result[
                        'action_instance_minion_allocation_mappings']
                    for (action_instance_id, allocated_minion_id) in (
                            osmorphing_machine_allocations.items()):
                        instance_machine_allocations[
                            action_instance_id]['osmorphing_minion_id'] = (
                                allocated_minion_id)

        # add the machines subflow to the main flow:
        main_allocation_flow.add(machines_subflow)

        # add final task to report minion machine availablity
        # to the conductor at the end of the flow:
        main_allocation_flow.add(
            allocation_confirmation_reporting_task_class(
                action['id'], instance_machine_allocations))

        LOG.info(
            "Starting main minion allocation flow '%s' for with ID '%s'. "
            "The minion allocations will be: %s" % (
                main_allocation_flow_name, action['id'],
                instance_machine_allocations))

        try:
            self._taskflow_runner.run_flow_in_background(
                main_allocation_flow, store={"context": ctxt})
        except Exception as ex:
            minion_pool_id = None
            try:
                for minion_pool_id in pools_used:
                    self._add_minion_pool_event(
                        ctxt, minion_pool.id, constants.TASK_EVENT_ERROR,
                        "A fatal exception occurred while attempting to start "
                        "the task flow for allocating machines for %s '%s'. "
                        "Forced deallocation and reallocation may be required."
                        " Please review the minion manager logs for additional"
                        " details. Error was: %s" % (
                            action_type, action['id'], str(ex)))
            except Exception:
                LOG.warn(
                    "Failed to add minion pool error event for pool '%s' "
                    "during allocation of machines for %s '%s'. Ignoring. "
                    "Exception was: %s",
                    minion_pool_id, action_type, action['id'],
                    utils.get_exception_details())
            raise

        return main_allocation_flow

    def _cleanup_machines_with_statuses_for_action(
            self, ctxt, action_id, targeted_statuses, exclude_pools=None):
        """ Deletes all minion machines which are marked with the given
        from the DB.
        """
        if exclude_pools is None:
            exclude_pools = []
        machines = db_api.get_minion_machines(ctxt, action_id)
        if not machines:
            LOG.debug(
                "No minion machines allocated to action '%s'. Returning.",
                action_id)
            return

        pool_machine_mappings = {}
        for machine in machines:
            if machine.allocation_status not in targeted_statuses:
                LOG.debug(
                    "Skipping deletion of machine '%s' from pool '%s' as "
                    "its status (%s) is not one of the targeted statuses (%s)",
                    machine.id, machine.pool_id, machine.allocation_status,
                    targeted_statuses)
                continue
            if machine.pool_id in exclude_pools:
                LOG.debug(
                    "Skipping deletion of machine '%s' (status '%s') from "
                    "whitelisted pool '%s'", machine.id, machine.allocation_status,
                    machine.pool_id)
                continue

            if machine.pool_id not in pool_machine_mappings:
                pool_machine_mappings[machine.pool_id] = [machine]
            else:
                pool_machine_mappings[machine.pool_id].append(machine)

        for (pool_id, machines) in pool_machine_mappings.items():
            with minion_manager_utils.get_minion_pool_lock(
                   pool_id, external=True):
                for machine in machines:
                    LOG.debug(
                        "Deleting machine with ID '%s' (pool '%s', status '%s') "
                        "from the DB.", machine.id, pool_id, machine.allocation_status)
                    db_api.delete_minion_machine(ctxt, machine.id)

    def deallocate_minion_machine(self, ctxt, minion_machine_id):

        minion_machine = db_api.get_minion_machine(
            ctxt, minion_machine_id)
        if not minion_machine:
            LOG.warn(
                "Could not find minion machine with ID '%s' for deallocation. "
                "Presuming it was deleted and returning early",
                minion_machine_id)
            return

        machine_allocated_status = constants.MINION_MACHINE_STATUS_IN_USE
        with minion_manager_utils.get_minion_pool_lock(
                minion_machine.pool_id, external=True):
            if minion_machine.allocation_status != machine_allocated_status or (
                    not minion_machine.allocated_action):
                LOG.warn(
                    "Minion machine '%s' was either in an improper status (%s)"
                    ", or did not have an associated action ('%s') for "
                    "deallocation request. Marking as available anyway.",
                    minion_machine.id, minion_machine.allocation_status,
                    minion_machine.allocated_action)
            LOG.debug(
                "Attempting to deallocate all minion pool machine '%s' "
                "(currently allocated to action '%s' with status '%s')",
                minion_machine.id, minion_machine.allocated_action,
                minion_machine.allocation_status)
            db_api.update_minion_machine(
                ctxt, minion_machine.id, {
                    "allocation_status": constants.MINION_MACHINE_STATUS_AVAILABLE,
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

        # categorise machine objects by pool:
        pool_machine_mappings = {}
        for machine in allocated_minion_machines:
            if machine.pool_id not in pool_machine_mappings:
                pool_machine_mappings[machine.pool_id] = []
            pool_machine_mappings[machine.pool_id].append(machine)

        # iterate over each pool and its machines allocated to this action:
        for (pool_id, pool_machines) in pool_machine_mappings.items():
            with minion_manager_utils.get_minion_pool_lock(
                    pool_id, external=True):
                machine_ids_to_deallocate = []
                # NOTE: this is a workaround in case some crash/restart happens
                # in the minion-manager service while new machine DB entries
                # are added to the DB without their point of deployment being
                # reached for them to ever get out of 'UNINITIALIZED' status:
                for machine in pool_machines:
                    if machine.allocation_status == (
                            constants.MINION_MACHINE_STATUS_UNINITIALIZED):
                        LOG.warn(
                            "Found minion machine '%s' in pool '%s' which "
                            "is in '%s' status. Removing from the DB "
                            "entirely." % (
                                machine.id, pool_id, machine.allocation_status))
                        db_api.delete_minion_machine(
                            ctxt, machine.id)
                        LOG.info(
                            "Successfully deleted minion machine entry '%s' "
                            "from pool '%s' from the DB.", machine.id, pool_id)
                        continue
                    LOG.debug(
                        "Going to mark minion machine '%s' (current status "
                        "'%s') of pool '%s' as available following machine "
                        "deallocation request for action '%s'.",
                        machine.id, machine.allocation_status, pool_id, action_id)
                    machine_ids_to_deallocate.append(machine.id)

                LOG.info(
                    "Marking minion machines '%s' from pool '%s' for "
                    "as available after having been allocated to action '%s'.",
                    machine_ids_to_deallocate, pool_id, action_id)
                db_api.set_minion_machines_allocation_statuses(
                    ctxt, machine_ids_to_deallocate, None,
                    constants.MINION_MACHINE_STATUS_AVAILABLE,
                    refresh_allocation_time=False)

        LOG.debug(
            "Successfully released all minion machines associated "
            "with action with base_id '%s'.", action_id)

    def _get_healtchcheck_flow_for_minion_machine(
            self, minion_pool, minion_machine, allocate_to_action=None,
            machine_status_on_success=constants.MINION_MACHINE_STATUS_AVAILABLE,
            power_on_machine=True, inject_for_tasks=None):
        """ Returns a taskflow graph flow with a healtcheck task
        and redeployment subflow on error. """
        # define healthcheck subflow for each machine:
        machine_healthcheck_subflow = graph_flow.Flow(
            minion_manager_tasks.MINION_POOL_HEALTHCHECK_MACHINE_SUBFLOW_NAME_FORMAT % (
                minion_pool.id, minion_machine.id))

        # add healtcheck task to healthcheck subflow:
        machine_healthcheck_task = (
            minion_manager_tasks.HealthcheckMinionMachineTask(
                minion_pool.id, minion_machine.id, minion_pool.platform,
                machine_status_on_success=machine_status_on_success,
                inject=inject_for_tasks,
                # we prevent a raise here as the healthcheck subflow
                # will take care of redeploying the instance later:
                fail_on_error=False))
        machine_healthcheck_subflow.add(machine_healthcheck_task)

        # optionally add minion machine power on task:
        if power_on_machine:
            if minion_machine.power_status == (
                    constants.MINION_MACHINE_POWER_STATUS_POWERED_OFF):
                power_on_task = minion_manager_tasks.PowerOnMinionMachineTask(
                    minion_pool.id, minion_machine.id, minion_pool.platform,
                    inject=inject_for_tasks,
                    # we prevent a raise here as the healthcheck subflow
                    # will take care of redeploying the instance later:
                    fail_on_error=False)
                machine_healthcheck_subflow.add(
                    power_on_task,
                    # NOTE: this is required to not have taskflow attempt
                    # (and fail) to automatically link the above Healthcheck
                    # task to the power on task based on inputs/outputs alone:
                    resolve_existing=False)
                machine_healthcheck_subflow.link(
                    power_on_task, machine_healthcheck_task,
                    # NOTE: taskflow gets confused when a task in the graph
                    # flow is linked to two others with no decider for each
                    # so we have to add a dummy decider which will always
                    # greenlight the rest of the execution here:
                    decider=taskflow_utils.DummyDecider(allow=True),
                    decider_depth=taskflow_deciders.Depth.FLOW)
            else:
                LOG.debug(
                    "Minion Machine with ID '%s' of pool '%s' is in power "
                    "state '%s' during healtchcheck subflow definition. "
                    "Not adding any power on task for it.",
                    minion_machine.id, minion_machine.pool_id,
                    minion_machine.power_status)

        # define reallocation subflow:
        machine_reallocation_subflow = linear_flow.Flow(
            minion_manager_tasks.MINION_POOL_REALLOCATE_MACHINE_SUBFLOW_NAME_FORMAT % (
                minion_pool.id, minion_machine.id))
        machine_reallocation_subflow.add(
            minion_manager_tasks.DeallocateMinionMachineTask(
                minion_pool.id, minion_machine.id, minion_pool.platform,
                inject=inject_for_tasks))
        machine_reallocation_subflow.add(
            minion_manager_tasks.AllocateMinionMachineTask(
                minion_pool.id, minion_machine.id, minion_pool.platform,
                allocate_to_action=allocate_to_action,
                inject=inject_for_tasks))
        machine_healthcheck_subflow.add(
            machine_reallocation_subflow,
            # NOTE: this is required to not have taskflow attempt (and fail)
            # to automatically link the above Healthcheck task to the
            # new subflow based on inputs/outputs alone:
            resolve_existing=False)

        # link reallocation subflow to healthcheck task:
        machine_healthcheck_subflow.link(
            machine_healthcheck_task, machine_reallocation_subflow,
            # NOTE: this is required to prevent any parent flows from skipping:
            decider_depth=taskflow_deciders.Depth.FLOW,
            decider=minion_manager_tasks.MinionMachineHealtchcheckDecider(
                minion_pool.id, minion_machine.id,
                on_successful_healthcheck=False))

        return machine_healthcheck_subflow

    def _get_minion_pool_refresh_flow(
            self, ctxt, minion_pool, requery=True):

        if requery:
            minion_pool = self._get_minion_pool(
                ctxt, minion_pool.id, include_machines=True,
                include_progress_updates=False, include_events=False)

        # determine how many machines could be feasibily downscaled:
        machine_statuses = {
            machine.id: machine.allocation_status
            for machine in minion_pool.minion_machines}
        ignorable_machine_statuses = [
            constants.MINION_MACHINE_STATUS_DEALLOCATING,
            constants.MINION_MACHINE_STATUS_POWERING_OFF,
            constants.MINION_MACHINE_STATUS_ERROR,
            constants.MINION_MACHINE_STATUS_POWER_ERROR,
            constants.MINION_MACHINE_STATUS_ERROR_DEPLOYING]
        max_minions_to_deallocate = (
            len([
                mid for mid in machine_statuses
                if machine_statuses[mid] not in ignorable_machine_statuses]) - (
                    minion_pool.minimum_minions))
        LOG.debug(
            "Determined minion pool '%s' machine deallocation number to be %d "
            "(pool minimum is '%d') based on current machines stauses: %s",
            minion_pool.id, max_minions_to_deallocate,
            minion_pool.minimum_minions, machine_statuses)

        # define refresh flow and process all relevant machines:
        pool_refresh_flow = unordered_flow.Flow(
            minion_manager_tasks.MINION_POOL_REFRESH_FLOW_NAME_FORMAT % (
                minion_pool.id))
        now = timeutils.utcnow()
        machines_to_deallocate = []
        machines_to_healthcheck = []
        skipped_machines = {}
        healthcheckable_machine_statuses = [
            constants.MINION_MACHINE_STATUS_AVAILABLE,
            # NOTE(aznashwan): this should help account for 'transient' issues
            # where a minion which may have been marked as error'd at some
            # point may be back online. Event if it isn't, the
            # sublow redeploy it after the healthcheck fails:
            constants.MINION_MACHINE_STATUS_ERROR,
            constants.MINION_MACHINE_STATUS_POWER_ERROR,
            constants.MINION_MACHINE_STATUS_ERROR_DEPLOYING]

        for machine in minion_pool.minion_machines:
            if machine.allocation_status not in (
                    healthcheckable_machine_statuses):
                skipped_machines[machine.id] = (
                    machine.allocation_status, machine.power_status)
                continue

            minion_expired = True
            if machine.last_used_at:
                expiry_time = (
                    machine.last_used_at + datetime.timedelta(
                        seconds=minion_pool.minion_max_idle_time))
                minion_expired = expiry_time <= now

            # deallocate the machine if it is expired:
            if max_minions_to_deallocate > 0 and minion_expired:
                if minion_pool.minion_retention_strategy == (
                        constants.MINION_POOL_MACHINE_RETENTION_STRATEGY_POWEROFF):
                    if machine.power_status in (
                            constants.MINION_MACHINE_POWER_STATUS_POWERED_OFF,
                            constants.MINION_MACHINE_POWER_STATUS_POWERING_OFF):
                        LOG.debug(
                            "Skipping powering off minion machine '%s' of pool"
                            " '%s' as it is already in powered off state.",
                            machine.id, minion_pool.id)
                        # NOTE: we count this machine out of the downscaling:
                        max_minions_to_deallocate = (
                            max_minions_to_deallocate - 1)
                        continue
                    LOG.debug(
                        "Minion machine '%s' of pool '%s' will be powered off "
                        "as part of the pool refresh process (current "
                        "deallocation count %d excluding the current machine)",
                        machine.id, minion_pool.id, max_minions_to_deallocate)
                    pool_refresh_flow.add(
                        minion_manager_tasks.PowerOffMinionMachineTask(
                            minion_pool.id, machine.id, minion_pool.platform,
                            fail_on_error=False,
                            status_once_powered_off=(
                                constants.MINION_MACHINE_STATUS_AVAILABLE)))
                elif minion_pool.minion_retention_strategy == (
                        constants.MINION_POOL_MACHINE_RETENTION_STRATEGY_DELETE):
                    pool_refresh_flow.add(
                        minion_manager_tasks.DeallocateMinionMachineTask(
                                minion_pool.id, machine.id,
                                minion_pool.platform))
                else:
                    raise exception.InvalidMinionPoolState(
                        "Unknown minion pool retention strategy '%s' for pool "
                        "'%s'" % (
                            minion_pool.minion_retention_strategy,
                            minion_pool.id))
                max_minions_to_deallocate = max_minions_to_deallocate - 1
                machines_to_deallocate.append(machine.id)
            # else, perform a healthcheck on the machine if it is powered on:
            elif machine.power_status == (
                    constants.MINION_MACHINE_POWER_STATUS_POWERED_ON):
                pool_refresh_flow.add(
                    self._get_healtchcheck_flow_for_minion_machine(
                        minion_pool, machine, allocate_to_action=None,
                        machine_status_on_success=(
                            constants.MINION_MACHINE_STATUS_AVAILABLE)))
                machines_to_healthcheck.append(machine.id)
            else:
                skipped_machines[machine.id] = (
                    machine.allocation_status, machine.power_status)

        # update DB entried for all machines and emit relevant events:
        if skipped_machines:
            base_msg =  (
                "The following minion machines were skipped during the "
                "refreshing of the minion pool as they were in other "
                "statuses than the serviceable ones: %s")
            LOG.debug(
                "[Pool '%s'] %s: %s",
                minion_pool.id, base_msg, skipped_machines)
            self._add_minion_pool_event(
                ctxt, minion_pool.id, constants.TASK_EVENT_INFO,
                base_msg % list(skipped_machines.keys()))

        if machines_to_deallocate:
            deallocation_action = "deallocated"
            status_for_deallocated_machines = (
                constants.MINION_MACHINE_STATUS_DEALLOCATING)
            if minion_pool.minion_retention_strategy == (
                    constants.MINION_POOL_MACHINE_RETENTION_STRATEGY_POWEROFF):
                deallocation_action = "powered off"
                status_for_deallocated_machines = (
                    constants.MINION_MACHINE_STATUS_POWERING_OFF)
            self._add_minion_pool_event(
                ctxt, minion_pool.id, constants.TASK_EVENT_INFO,
                "The following minion machines will be %s as part "
                "of the refreshing of the minion pool: %s" % (
                    deallocation_action, machines_to_deallocate))
            for machine in machines_to_deallocate:
                db_api.set_minion_machine_allocation_status(
                    ctxt, machine, status_for_deallocated_machines)
        else:
            self._add_minion_pool_event(
                ctxt, minion_pool.id, constants.TASK_EVENT_INFO,
                "No minion machines require deallocation during pool refresh")

        if machines_to_healthcheck:
            self._add_minion_pool_event(
                ctxt, minion_pool.id, constants.TASK_EVENT_INFO,
                "The following minion machines will be healthchecked as part "
                "of the refreshing of the minion pool: %s" % (
                    machines_to_healthcheck))
            for machine in machines_to_healthcheck:
                db_api.set_minion_machine_allocation_status(
                    ctxt, machine,
                    constants.MINION_MACHINE_STATUS_HEALTHCHECKING)
        else:
            self._add_minion_pool_event(
                ctxt, minion_pool.id, constants.TASK_EVENT_INFO,
                "No minion machines require healthchecking during "
                "pool refresh")

        return pool_refresh_flow

    @minion_manager_utils.minion_pool_synchronized_op
    def refresh_minion_pool(self, ctxt, minion_pool_id):
        LOG.info("Attempting to healthcheck Minion Pool '%s'.", minion_pool_id)
        minion_pool = self._get_minion_pool(
            ctxt, minion_pool_id, include_events=False, include_machines=True,
            include_progress_updates=False)
        endpoint_dict = self._rpc_conductor_client.get_endpoint(
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

        refresh_flow = self._get_minion_pool_refresh_flow(
            ctxt, minion_pool, requery=False)
        if not refresh_flow:
            msg = (
                "There are no minion machine refresh operations to be performed "
                "at this time")
            db_api.add_minion_pool_event(
                ctxt, minion_pool.id, constants.TASK_EVENT_INFO, msg)
            return self._get_minion_pool(ctxt, minion_pool.id)

        initial_store = self._get_pool_initial_taskflow_store_base(
            ctxt, minion_pool, endpoint_dict)
        self._taskflow_runner.run_flow_in_background(
            refresh_flow, store=initial_store)
        self._add_minion_pool_event(
            ctxt, minion_pool.id, constants.TASK_EVENT_INFO,
            "Begun minion pool refreshing process")

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

        endpoint_dict = self._rpc_conductor_client.get_endpoint(
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

        cleanup_trust = not bool(ctxt.trust_id)
        try:
            keystone.create_trust(ctxt)
            minion_pool.maintenance_trust_id = ctxt.trust_id
            db_api.add_minion_pool(ctxt, minion_pool)
            self._add_minion_pool_event(
                ctxt, minion_pool.id, constants.TASK_EVENT_INFO,
                "Successfully added minion pool to the DB")
            self._register_refresh_jobs_for_minion_pool(
                minion_pool)
        except Exception:
            if cleanup_trust:
                keystone.delete_trust(ctxt)
            raise

        if not skip_allocation:
            allocation_flow = self._get_minion_pool_allocation_flow(
                minion_pool)
            # start the deployment flow:
            initial_store = self._get_pool_initial_taskflow_store_base(
                ctxt, minion_pool, endpoint_dict)
            try:
                self._taskflow_runner.run_flow_in_background(
                    allocation_flow, store=initial_store)
            except Exception as ex:
                self._add_minion_pool_event(
                    ctxt, minion_pool.id, constants.TASK_EVENT_ERROR,
                    "A fatal exception occurred while attempting to start the "
                    "task flow for allocating the minion pool. Forced "
                    "deallocation and reallocation may be required. Please "
                    "review the manager logs for additional details. "
                    "Error was: %s" % str(ex))
                raise

            self._add_minion_pool_event(
                ctxt, minion_pool.id, constants.TASK_EVENT_INFO,
                "Begun minion pool allocation process")

        return self.get_minion_pool(ctxt, minion_pool.id)

    def _get_pool_initial_taskflow_store_base(
            self, ctxt, minion_pool, endpoint_dict):
        # NOTE: considering pools are associated to strictly one endpoint,
        # we can duplicate the 'origin/destination':
        origin_info = {
            "id": endpoint_dict['id'],
            "connection_info": endpoint_dict['connection_info'],
            "mapped_regions": endpoint_dict['mapped_regions'],
            "type": endpoint_dict['type']}
        initial_store = {
            "context": ctxt,
            "origin": origin_info,
            "destination": origin_info,
            "task_info": {
                "pool_identifier": minion_pool.id,
                "pool_os_type": minion_pool.os_type,
                "pool_environment_options": minion_pool.environment_options}}
        shared_resources = minion_pool.shared_resources
        if shared_resources is None:
            shared_resources = {}
        initial_store['task_info']['pool_shared_resources'] = shared_resources
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
            constants.MINION_MACHINE_STATUS_POWER_ERROR,
            constants.MINION_MACHINE_STATUS_ERROR]
        used_machines = {
            mch for mch in minion_pool.minion_machines
            if mch.allocation_status not in unused_machine_states}
        if used_machines and raise_if_in_use:
            raise exception.InvalidMinionPoolState(
                "Minion pool '%s' has one or more machines which are in an"
                " active state: %s" % (
                    minion_pool.id, {
                        mch.id: mch.allocation_status for mch in used_machines}))
        return used_machines

    @minion_manager_utils.minion_pool_synchronized_op
    def allocate_minion_pool(self, ctxt, minion_pool_id):
        LOG.info("Attempting to allocate Minion Pool '%s'.", minion_pool_id)
        minion_pool = self._get_minion_pool(
            ctxt, minion_pool_id, include_events=False, include_machines=False,
            include_progress_updates=False)
        endpoint_dict = self._rpc_conductor_client.get_endpoint(
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
            self._register_refresh_jobs_for_minion_pool(
                minion_pool)
            self._add_minion_pool_event(
                ctxt, minion_pool.id, constants.TASK_EVENT_INFO,
                "Begun minion pool allocation process")
        except Exception as ex:
            self._add_minion_pool_event(
                ctxt, minion_pool.id, constants.TASK_EVENT_ERROR,
                "A fatal exception occurred while attempting to start the "
                "task flow for allocating the minion pool. Forced "
                "deallocation and reallocation may be required. Please "
                "review the manager logs for additional details. "
                "Error was: %s" % str(ex))
            db_api.set_minion_pool_status(
                ctxt, minion_pool_id, current_status)
            raise

        return self._get_minion_pool(ctxt, minion_pool.id)

    def _get_minion_pool_deallocation_flow(
            self, minion_pool, raise_on_error=True):
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
                    minion_pool.id, machine.id, minion_pool.platform,
                    raise_on_cleanup_failure=raise_on_error))
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
        endpoint_dict = self._rpc_conductor_client.get_endpoint(
            ctxt, minion_pool.endpoint_id)

        deallocation_flow = self._get_minion_pool_deallocation_flow(
            minion_pool, raise_on_error=not force)
        initial_store = self._get_pool_deallocation_initial_store(
            ctxt, minion_pool, endpoint_dict)

        try:
            db_api.set_minion_pool_status(
                ctxt, minion_pool_id,
                constants.MINION_POOL_STATUS_POOL_MAINTENANCE)
            self._taskflow_runner.run_flow_in_background(
                deallocation_flow, store=initial_store)
            self._unregister_refresh_jobs_for_minion_pool(
                minion_pool, raise_on_error=False)
            self._add_minion_pool_event(
                ctxt, minion_pool.id, constants.TASK_EVENT_INFO,
                "Begun minion pool deallocation process")
        except Exception as ex:
            self._add_minion_pool_event(
                ctxt, minion_pool.id, constants.TASK_EVENT_ERROR,
                "A fatal exception occurred while attempting to start the "
                "task flow for deallocating the minion pool. Forced "
                "deallocation and reallocation may be required. Please "
                "review the manager logs for additional details. "
                "Error was: %s" % str(ex))
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
        self._add_minion_pool_event(
            ctxt, minion_pool.id, constants.TASK_EVENT_INFO,
            "Successfully updated minion pool properties")
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
        self._unregister_refresh_jobs_for_minion_pool(
            minion_pool, raise_on_error=False)

        LOG.info("Deleting minion pool with ID '%s'" % minion_pool_id)
        db_api.delete_minion_pool(ctxt, minion_pool_id)
        if minion_pool.maintenance_trust_id:
            maintenance_ctxt = context.get_admin_context(
                minion_pool.maintenance_trust_id)
            keystone.delete_trust(maintenance_ctxt)
