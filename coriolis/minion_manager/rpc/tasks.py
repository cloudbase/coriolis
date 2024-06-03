# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.
# pylint: disable=line-too-long

import abc
import copy

from oslo_log import log as logging
from oslo_utils import timeutils

from coriolis.conductor.rpc import client as rpc_conductor_client
from coriolis import constants
from coriolis.db import api as db_api
from coriolis.db.sqlalchemy import models
from coriolis import exception
from coriolis.minion_manager.rpc import client as rpc_minion_manager_client
from coriolis.minion_manager.rpc import utils as minion_manager_utils
from coriolis.taskflow import base as coriolis_taskflow_base
from coriolis import utils

from taskflow.types import failure


LOG = logging.getLogger(__name__)

MINION_POOL_MIGRATION_ALLOCATION_FLOW_NAME_FORMAT = (
    "migration-%s-minions-allocation")
MINION_POOL_REPLICA_ALLOCATION_FLOW_NAME_FORMAT = (
    "replica-%s-minions-allocation")
MINION_POOL_MIGRATION_ALLOCATION_SUBFLOW_NAME_FORMAT = (
    "migration-%s-minions-machines-allocation")
MINION_POOL_REPLICA_ALLOCATION_SUBFLOW_NAME_FORMAT = (
    "replica-%s-minions-machines-allocation")
MINION_POOL_ALLOCATION_FLOW_NAME_FORMAT = "pool-%s-allocation"
MINION_POOL_DEALLOCATION_FLOW_NAME_FORMAT = "pool-%s-deallocation"
MINION_POOL_REFRESH_FLOW_NAME_FORMAT = "pool-%s-refresh"
MINION_POOL_VALIDATION_TASK_NAME_FORMAT = "pool-%s-validation"
MINION_POOL_UPDATE_STATUS_TASK_NAME_FORMAT = "pool-%s-update-status-%s"
MINION_POOL_HEALTHCHECK_MACHINE_TASK_NAME_FORMAT = (
    "pool-%s-machine-%s-healthcheck")
MINION_POOL_ALLOCATE_SHARED_RESOURCES_TASK_NAME_FORMAT = (
    "pool-%s-allocate-shared-resources")
MINION_POOL_DEALLOCATE_SHARED_RESOURCES_TASK_NAME_FORMAT = (
    "pool-%s-deallocate-shared-resources")
MINION_POOL_ALLOCATE_MINIONS_SUBFLOW_NAME_FORMAT = (
    "pool-%s-machines-allocation")
MINION_POOL_DEALLOCATE_MACHINES_SUBFLOW_NAME_FORMAT = (
    "pool-%s-machines-deallocation")
MINION_POOL_HEALTHCHECK_MACHINE_SUBFLOW_NAME_FORMAT = (
    "pool-%s-machine-%s-healthcheck")
MINION_POOL_REALLOCATE_MACHINE_SUBFLOW_NAME_FORMAT = (
    "pool-%s-machine-%s-reallocation")
MINION_POOL_ALLOCATE_MACHINES_FOR_REPLICA_SUBFLOW_NAME_FORMAT = (
    "pool-%s-allocate-replica-%s-machines")
MINION_POOL_ALLOCATE_MACHINES_FOR_MIGRATION_SUBFLOW_NAME_FORMAT = (
    "pool-%s-allocate-migration-%s-machines")
MINION_POOL_ALLOCATE_MACHINE_TASK_NAME_FORMAT = (
    "pool-%s-machine-%s-allocation")
MINION_POOL_DEALLOCATE_MACHINE_TASK_NAME_FORMAT = (
    "pool-%s-machine-%s-deallocation")
MINION_POOL_CONFIRM_MIGRATION_MINION_ALLOCATION_TASK_NAME_FORMAT = (
    "migration-%s-minion-allocation-confirmation")
MINION_POOL_CONFIRM_REPLICA_MINION_ALLOCATION_TASK_NAME_FORMAT = (
    "replica-%s-minion-allocation-confirmation")
MINION_POOL_REPORT_MIGRATION_ALLOCATION_FAILURE_TASK_NAME_FORMAT = (
    "migration-%s-minion-allocation-failure")
MINION_POOL_REPORT_REPLICA_ALLOCATION_FAILURE_TASK_NAME_FORMAT = (
    "replica-%s-minion-allocation-failure")
MINION_POOL_POWER_ON_MACHINE_TASK_NAME_FORMAT = (
    "pool-%s-machine-%s-power-on")
MINION_POOL_POWER_OFF_MACHINE_TASK_NAME_FORMAT = (
    "pool-%s-machine-%s-power-off")


class MinionManagerTaskEventMixin(object):

    # NOTE(aznashwan): it is unsafe to fork processes with pre-instantiated
    # oslo_messaging clients as the underlying eventlet thread queues will
    # be invalidated. Considering this class both serves from a "main
    # process" as well as forking child processes, it is safest to
    # re-instantiate the clients every time:
    @property
    def _conductor_client(self):
        if not getattr(self, '_conductor_client_instance', None):
            self._conductor_client_instance = (
                rpc_conductor_client.ConductorClient())
        return self._conductor_client_instance

    @property
    def _minion_manager_client(self):
        if not getattr(self, '_minion_manager_client_instance', None):
            self._minion_manager_client_instance = (
                rpc_minion_manager_client.MinionManagerClient())
        return self._minion_manager_client_instance

    def _add_minion_pool_event(
            self, context, message, level=constants.TASK_EVENT_INFO):
        LOG.debug("Minion pool '%s' event: %s", self._minion_pool_id, message)
        db_api.add_minion_pool_event(
            context, self._minion_pool_id, level, message)

    def _get_minion_machine(
            self, context, minion_machine_id,
            raise_if_not_found=False):
        machine = db_api.get_minion_machine(context, minion_machine_id)
        if not machine and raise_if_not_found:
            raise exception.NotFound(
                "Could not find minion machine with ID '%s'" % (
                    minion_machine_id))
        return machine

    def _set_minion_pool_status(self, ctxt, minion_pool_id, new_status):
        with minion_manager_utils.get_minion_pool_lock(
                minion_pool_id, external=True):
            db_api.set_minion_pool_status(ctxt, minion_pool_id, new_status)

    def _update_minion_machine(
            self, ctxt, minion_pool_id, minion_machine_id, updated_values):
        with minion_manager_utils.get_minion_pool_lock(
                minion_pool_id, external=True):
            db_api.update_minion_machine(
                ctxt, minion_machine_id, updated_values)

    def _set_minion_machine_allocation_status(
            self, ctxt, minion_pool_id, minion_machine_id, new_status):
        with minion_manager_utils.get_minion_pool_lock(
                minion_pool_id, external=True):
            db_api.set_minion_machine_allocation_status(
                ctxt, minion_machine_id, new_status)

    def _set_minion_machine_power_status(
            self, ctxt, minion_pool_id, minion_machine_id, new_status):
        self._update_minion_machine(
            ctxt, minion_pool_id, minion_machine_id,
            {"power_status": new_status})


class _BaseReportMinionAllocationFailureForActionTask(
        coriolis_taskflow_base.BaseCoriolisTaskflowTask,
        MinionManagerTaskEventMixin):
    """ Task with no operation on `execute()`, but whose `revert()` method
    reports a minion allocation failure to the conductor for the afferent
    transfer action. """

    def __init__(self, action_id, **kwargs):
        self._action_id = action_id
        self._task_name = self._get_task_name(action_id)
        super(_BaseReportMinionAllocationFailureForActionTask, self).__init__(
            name=self._task_name, **kwargs)

    @abc.abstractmethod
    def _get_task_name(self, action_id):
        raise NotImplementedError(
            "No allocation failure task name provided")

    @abc.abstractmethod
    def _report_machine_allocation_failure(
            self, context, action_id, failure_str):
        raise NotImplementedError(
            "No allocation failure operation defined")

    def execute(self, context):
        super(
            _BaseReportMinionAllocationFailureForActionTask, self).execute()
        LOG.debug(
            "Nothing to execute for task '%s'", self._task_name)

    def revert(self, context, *args, **kwargs):
        super(
            _BaseReportMinionAllocationFailureForActionTask, self).revert(
                *args, **kwargs)
        flow_failures = kwargs.get('flow_failures', {})
        flow_failures_str = self._get_error_str_for_flow_failures(
            flow_failures, full_tracebacks=False)
        LOG.info(
            "Reporting minion allocation failure for action '%s': %s",
            self._action_id, flow_failures_str)
        self._minion_manager_client.deallocate_minion_machines_for_action(
            context, self._action_id)
        self._report_machine_allocation_failure(
            context, self._action_id, flow_failures_str)


class ReportMinionAllocationFailureForMigrationTask(
        _BaseReportMinionAllocationFailureForActionTask):

    def _get_task_name(self, action_id):
        return (
            MINION_POOL_REPORT_MIGRATION_ALLOCATION_FAILURE_TASK_NAME_FORMAT
            % (action_id))

    def _report_machine_allocation_failure(
            self, context, action_id, failure_str):
        self._conductor_client.report_migration_minions_allocation_error(
            context, action_id, failure_str)


class ReportMinionAllocationFailureForReplicaTask(
        _BaseReportMinionAllocationFailureForActionTask):

    def _get_task_name(self, action_id):
        return (
            MINION_POOL_REPORT_REPLICA_ALLOCATION_FAILURE_TASK_NAME_FORMAT
            % (action_id))

    def _report_machine_allocation_failure(
            self, context, action_id, failure_str):
        self._conductor_client.report_replica_minions_allocation_error(
            context, action_id, failure_str)


class _BaseConfirmMinionAllocationForActionTask(
        coriolis_taskflow_base.BaseCoriolisTaskflowTask,
        MinionManagerTaskEventMixin):
    """ Task which confirms the minion machine allocations for the given action
    to the conductor.
    """

    def __init__(self, action_id, allocated_machine_id_mappings, **kwargs):
        """
        param allocated_machine_id_mappings: dict of the form:
        {
            "<action_instance_identifier>": {
                "origin_minion_id": "<origin_minion_id>",
                "destination_minion_id": "<destination_minion_id>",
                "osmorphing_minion_id": "<osmorphing_minion_id>"}}
        """
        self._action_id = action_id
        self._task_name = self._get_task_name(action_id)
        self._allocated_machine_id_mappings = allocated_machine_id_mappings
        super(_BaseConfirmMinionAllocationForActionTask, self).__init__(
            name=self._task_name, **kwargs)

    @abc.abstractmethod
    def _get_action_label(self):
        raise NotImplementedError(
            "No minion allocation confirmation task action label defined")

    @abc.abstractmethod
    def _get_task_name(self, action_id):
        raise NotImplementedError(
            "No minion allocation confirmation task name defined")

    @abc.abstractmethod
    def _confirm_machine_allocation_for_action(
            self, context, action_id, machine_allocations):
        raise NotImplementedError(
            "No minion allocation confrimation operation defined")

    def execute(self, context):
        machines_cache = {}
        machine_allocations = {}

        def _check_minion_properties(
                minion_machine, instance, minion_purpose="unknown"):
            if minion_machine.allocation_status != (
                    constants.MINION_MACHINE_STATUS_IN_USE):
                raise exception.InvalidMinionMachineState(
                    "Minion machine with ID '%s' of pool '%s' is in '%s' "
                    "status instead of the expected '%s' for it to be used "
                    "as a '%s' minion for instance '%s' of transfer "
                    "action '%s'." % (
                        minion_machine.id, minion_machine.pool_id,
                        minion_machine.allocation_status,
                        constants.MINION_MACHINE_STATUS_IN_USE,
                        minion_purpose, instance, self._action_id))

            if minion_machine.allocated_action != self._action_id:
                raise exception.InvalidMinionMachineState(
                    "Minion machine with ID '%s' of pool '%s' appears to be "
                    "allocated to action with ID '%s' instead of the expected"
                    " '%s' for it to be used as a '%s' minion for instance "
                    "'%s'." % (
                        minion_machine.id, minion_machine.pool_id,
                        minion_machine.allocated_action, self._action_id,
                        minion_purpose, instance))

            if minion_machine.power_status != (
                    constants.MINION_MACHINE_POWER_STATUS_POWERED_ON):
                raise exception.InvalidMinionMachineState(
                    "Minion machine with ID '%s' of pool '%s' is in '%s' "
                    "power status instead of the expected '%s' for it to be "
                    "used as a '%s' minion for instance '%s' of transfer "
                    "action '%s'." % (
                        minion_machine.id, minion_machine.pool_id,
                        minion_machine.power_status,
                        constants.MINION_MACHINE_POWER_STATUS_POWERED_ON,
                        minion_purpose, instance, self._action_id))

            # TODO(aznashwan): add extra checks for conn info schemas here?
            required_props = {
                "provider_properties": minion_machine.provider_properties,
                "connection_info": minion_machine.connection_info}
            if not all(required_props.values()):
                raise exception.InvalidMinionMachineState(
                    "One or more required paroperties for minion machine '%s' "
                    "(to be used as a '%s' minion for instance '%s' of action "
                    "'%s') were missing: %s" % (
                        minion_machine.id, minion_purpose, instance,
                        self._action_id, required_props))

        for (instance, allocated_machines_for_instance) in (
                self._allocated_machine_id_mappings.items()):

            if not allocated_machines_for_instance:
                LOG.warn(
                    "No machine allocations were provided for instance '%s' "
                    "for action '%s'. Skipping. mappings were: %s",
                    instance, self._action_id, allocated_machines_for_instance)
                continue

            if instance not in machine_allocations:
                machine_allocations[instance] = {}

            # check for and fetch the source minion:
            origin_minion_id = allocated_machines_for_instance.get(
                'origin_minion_id')
            if origin_minion_id:
                origin_minion_machine = machines_cache.get(origin_minion_id)
                if not origin_minion_machine:
                    origin_minion_machine = self._get_minion_machine(
                        context, origin_minion_id, raise_if_not_found=True)
                    machines_cache[origin_minion_id] = origin_minion_machine
                    _check_minion_properties(
                        origin_minion_machine, instance,
                        minion_purpose="source")
                machine_allocations[instance]['origin_minion'] = (
                    origin_minion_machine.to_dict())

            # check for and fetch the destination minion:
            destination_minion_id = allocated_machines_for_instance.get(
                'destination_minion_id')
            if destination_minion_id:
                destination_minion_machine = machines_cache.get(
                    destination_minion_id)
                if not destination_minion_machine:
                    destination_minion_machine = self._get_minion_machine(
                        context, destination_minion_id,
                        raise_if_not_found=True)
                    _check_minion_properties(
                        destination_minion_machine, instance,
                        minion_purpose="destination")
                    machines_cache[destination_minion_id] = (
                        destination_minion_machine)
                machine_allocations[instance]['destination_minion'] = (
                    destination_minion_machine.to_dict())

            # check for and fetch the OSMorphing minion:
            osmorphing_minion_id = allocated_machines_for_instance.get(
                'osmorphing_minion_id')
            if osmorphing_minion_id:
                osmorphing_minion_machine = machines_cache.get(
                    osmorphing_minion_id)
                if not osmorphing_minion_machine:
                    osmorphing_minion_machine = self._get_minion_machine(
                        context, osmorphing_minion_id, raise_if_not_found=True)
                    _check_minion_properties(
                        osmorphing_minion_machine, instance,
                        minion_purpose="OSMorphing")
                    machines_cache[osmorphing_minion_id] = (
                        osmorphing_minion_machine)
                machine_allocations[instance]['osmorphing_minion'] = (
                    osmorphing_minion_machine.to_dict())

        try:
            self._confirm_machine_allocation_for_action(
                context, self._action_id, machine_allocations)
        except exception.NotFound as ex:
            msg = (
                "The Conductor has refused minion machine allocations for "
                "%s with ID '%s' as it has purportedly been deleted."
                " Please check both the Conductor and Minion Manager "
                "service logs for more details." % (
                    self._get_action_label().lower().capitalize(),
                    self._action_id))
            LOG.error(
                "%s. Allocations were: %s. Original trace was: %s",
                msg, machine_allocations, utils.get_exception_details())
            raise exception.MinionMachineAllocationFailure(
                msg) from ex
        except (
                exception.InvalidMigrationState,
                exception.InvalidReplicaState) as ex:
            msg = (
                "The Conductor has refused minion machine allocations for "
                "%s with ID '%s' as it is purportedly in an invalid state "
                "to have minions allocated for it. It is possible "
                "that the transfer had been user-cancelled or had "
                "otherwise been halted. Please check both the Conductor "
                "and Minion Manager service logs for more details." % (
                    self._get_action_label().lower().capitalize(),
                    self._action_id))
            LOG.error(
                "%s. Allocations were: %s. Original trace was: %s",
                msg, machine_allocations, utils.get_exception_details())
            raise exception.MinionMachineAllocationFailure(
                msg) from ex


class ConfirmMinionAllocationForMigrationTask(
        _BaseConfirmMinionAllocationForActionTask):

    def _get_action_label(self):
        return "migration"

    def _get_task_name(self, action_id):
        return (
            MINION_POOL_CONFIRM_MIGRATION_MINION_ALLOCATION_TASK_NAME_FORMAT
            % (action_id))

    def _confirm_machine_allocation_for_action(
            self, context, action_id, machine_allocations):
        self._conductor_client.confirm_migration_minions_allocation(
            context, action_id, machine_allocations)


class ConfirmMinionAllocationForReplicaTask(
        _BaseConfirmMinionAllocationForActionTask):

    def _get_action_label(self):
        return "replica"

    def _get_task_name(self, action_id):
        return (
            MINION_POOL_CONFIRM_REPLICA_MINION_ALLOCATION_TASK_NAME_FORMAT
            % (action_id))

    def _confirm_machine_allocation_for_action(
            self, context, action_id, machine_allocations):
        self._conductor_client.confirm_replica_minions_allocation(
            context, action_id, machine_allocations)


class UpdateMinionPoolStatusTask(
        coriolis_taskflow_base.BaseCoriolisTaskflowTask,
        MinionManagerTaskEventMixin):
    """ Task which updates the status of the given pool.
    Is capable of recording and reverting the state.
    """

    default_provides = ["latest_status"]

    def __init__(
            self, minion_pool_id, target_status,
            status_to_revert_to=None, **kwargs):

        self._target_status = target_status
        self._minion_pool_id = minion_pool_id
        self._task_name = (MINION_POOL_UPDATE_STATUS_TASK_NAME_FORMAT % (
            self._minion_pool_id, self._target_status)).lower()
        self._previous_status = None
        self._status_to_revert_to = status_to_revert_to

        super(UpdateMinionPoolStatusTask, self).__init__(
            name=self._task_name, **kwargs)

    def execute(self, context, *args):
        super(UpdateMinionPoolStatusTask, self).execute(*args)

        if not self._previous_status:
            minion_pool = db_api.get_minion_pool(
                context, self._minion_pool_id, include_machines=False,
                include_events=False, include_progress_updates=False)
            self._previous_status = minion_pool.status

        if self._previous_status == self._target_status:
            LOG.debug(
                "[Task '%s'] Minion pool '%s' already in status '%s'. "
                "Nothing to do." % (
                    self._task_name, self._minion_pool_id,
                    self._target_status))
        else:
            LOG.debug(
                "[Task '%s'] Transitioning minion pool '%s' from status '%s' "
                "to '%s'." % (
                    self._task_name, self._minion_pool_id,
                    self._previous_status, self._target_status))
            self._set_minion_pool_status(
                context, self._minion_pool_id, self._target_status)
            self._add_minion_pool_event(
                context,
                "Pool status transitioned from '%s' to '%s'" % (
                    self._previous_status, self._target_status))

        return self._target_status

    def revert(self, context, *args, **kwargs):
        super(UpdateMinionPoolStatusTask, self).revert(*args, **kwargs)

        minion_pool = db_api.get_minion_pool(
            context, self._minion_pool_id, include_machines=False,
            include_events=False, include_progress_updates=False)
        if not minion_pool:
            LOG.debug(
                "[Task '%s'] Could not find pool with ID '%s' for status "
                "reversion." % (self._task_name, self._minion_pool_id))
            return

        previous_status = self._previous_status
        if self._status_to_revert_to:
            LOG.debug(
                "Forcibly reverting pool to status '%s' despite previous "
                "status being '%s'",
                self._status_to_revert_to, self._previous_status)
            previous_status = self._status_to_revert_to
        if minion_pool.status == previous_status:
            LOG.debug(
                "[Task '%s'] Minion pool '%s' is/was already reverted to "
                "'%s'." % (
                    self._task_name, self._minion_pool_id,
                    previous_status))
        else:
            if minion_pool.status != self._target_status:
                LOG.warn(
                    "[Task %s] Minion pool '%s' is in status '%s', which is "
                    "neither the previous status ('%s'), nor the newly-set "
                    "status ('%s'). Reverting to '%s' anyway.",
                    self._task_name, self._minion_pool_id, minion_pool.status,
                    previous_status, self._target_status, previous_status)
            LOG.debug(
                "[Task '%s'] Reverting pool '%s' status from '%s' to "
                "'%s'" % (
                    self._task_name, self._minion_pool_id, minion_pool.status,
                    previous_status))
            self._set_minion_pool_status(
                context, self._minion_pool_id, previous_status)
            self._add_minion_pool_event(
                context,
                "Pool status reverted from '%s' to '%s'" % (
                    minion_pool.status, previous_status))


class BaseMinionManangerTask(
        coriolis_taskflow_base.BaseRunWorkerTask,
        MinionManagerTaskEventMixin):

    """Base taskflow.Task implementation for Minion Mananger tasks.

    Acts as a simple adapter between minion-pool-specific params and the
    BaseRunWorkerTask.
    """

    default_provides = 'task_info'

    def __init__(
            self, minion_pool_id, minion_machine_id,
            main_task_runner_type, **kwargs):
        self._minion_pool_id = minion_pool_id
        self._minion_machine_id = minion_machine_id

        super(BaseMinionManangerTask, self).__init__(
            self._get_task_name(minion_pool_id, minion_machine_id),
            # TODO(aznashwan): passing the minion pool ID as the task ID is
            # required to allow for the Minion pool event manager in the worker
            # service to know what pool to emit events for.
            minion_pool_id, minion_machine_id, main_task_runner_type, **kwargs)

    @abc.abstractmethod
    def _get_task_name(self, minion_pool_id, minion_machine_id):
        raise NotImplementedError("No task name providable")

    def execute(self, context, origin, destination, task_info):
        LOG.info(
            "Starting minion pool task '%s' (runner type '%s')",
            self._task_name, self._main_task_runner_type)
        res = super(BaseMinionManangerTask, self).execute(
            context, origin, destination, task_info)
        LOG.info(
            "Completed minion pool task '%s' (runner type '%s')",
            self._task_name, self._main_task_runner_type)
        return res

    def revert(self, context, origin, destination, task_info, **kwargs):
        flow_failures = kwargs.get('flow_failures', {})
        self._add_minion_pool_event(
            context,
            "Failure occurred for one or more operations on minion pool '%s'. "
            "Please check the logs for additional details. Error messages "
            "were:\n%s" % (
                self._minion_pool_id,
                self._get_error_str_for_flow_failures(
                    flow_failures, full_tracebacks=False)),
            level=constants.TASK_EVENT_ERROR)
        super(BaseMinionManangerTask, self).revert(
            context, origin, destination, task_info, **kwargs)


class ValidateMinionPoolOptionsTask(BaseMinionManangerTask):

    def __init__(
            self, minion_pool_id, minion_machine_id, minion_pool_type,
            **kwargs):
        task_type = constants.TASK_TYPE_VALIDATE_SOURCE_MINION_POOL_OPTIONS
        if minion_pool_type != constants.PROVIDER_PLATFORM_SOURCE:
            task_type = (
                constants.TASK_TYPE_VALIDATE_DESTINATION_MINION_POOL_OPTIONS)
        super(ValidateMinionPoolOptionsTask, self).__init__(
            minion_pool_id, minion_machine_id, task_type, **kwargs)

    def _get_task_name(self, minion_pool_id, minion_machine_id):
        return MINION_POOL_VALIDATION_TASK_NAME_FORMAT % minion_pool_id

    def execute(self, context, origin, destination, task_info):
        self._add_minion_pool_event(
            context, "Validating minion pool options")
        _ = super(ValidateMinionPoolOptionsTask, self).execute(
            context, origin, destination, task_info)
        self._add_minion_pool_event(
            context, "Successfully validated minion pool options")

    def revert(self, context, origin, destination, task_info, **kwargs):
        LOG.debug("[%s] Nothing to revert for validation", self._task_name)
        super(ValidateMinionPoolOptionsTask, self).revert(
            context, origin, destination, task_info, **kwargs)


class AllocateSharedPoolResourcesTask(BaseMinionManangerTask):

    def __init__(
            self, minion_pool_id, minion_machine_id, minion_pool_type,
            **kwargs):

        resource_deployment_task_type = (
            constants.TASK_TYPE_SET_UP_SOURCE_POOL_SHARED_RESOURCES)
        resource_cleanup_task_type = (
            constants.TASK_TYPE_TEAR_DOWN_SOURCE_POOL_SHARED_RESOURCES)
        if minion_pool_type != constants.PROVIDER_PLATFORM_SOURCE:
            resource_deployment_task_type = (
                constants.TASK_TYPE_SET_UP_DESTINATION_POOL_SHARED_RESOURCES)
            resource_cleanup_task_type = (
                constants.TASK_TYPE_TEAR_DOWN_DESTINATION_POOL_SHARED_RESOURCES)  # noqa: E501
        super(AllocateSharedPoolResourcesTask, self).__init__(
            minion_pool_id, minion_machine_id, resource_deployment_task_type,
            cleanup_task_runner_type=resource_cleanup_task_type, **kwargs)

    def _get_task_name(self, minion_pool_id, minion_machine_id):
        return MINION_POOL_ALLOCATE_SHARED_RESOURCES_TASK_NAME_FORMAT % (
            minion_pool_id)

    def execute(self, context, origin, destination, task_info):
        with minion_manager_utils.get_minion_pool_lock(
                self._minion_pool_id, external=True):
            minion_pool = db_api.get_minion_pool(
                context, self._minion_pool_id)
            if not minion_pool:
                raise exception.InvalidMinionPoolSelection(
                    "[Task '%s'] Minion pool '%s' doesn't exist in the DB. "
                    "It cannot have shared resources deployed for it." % (
                        self._task_name, self._minion_pool_id))
            if minion_pool.shared_resources:
                raise exception.InvalidMinionPoolState(
                    "[Task '%s'] Minion pool already has shared resources "
                    "defined for it. Cannot re-deploy shared resources. "
                    "DB entry is: %s" % (
                        self._task_name, minion_pool.shared_resources))

        self._add_minion_pool_event(
            context, "Deploying shared pool resources")
        res = super(AllocateSharedPoolResourcesTask, self).execute(
            context, origin, destination, task_info)
        pool_shared_resources = res['pool_shared_resources']

        updated_values = {
            "shared_resources": pool_shared_resources}

        self._add_minion_pool_event(
            context, "Successfully deployed shared pool resources")
        with minion_manager_utils.get_minion_pool_lock(
                self._minion_pool_id, external=True):
            db_api.update_minion_pool(
                context, self._minion_pool_id, updated_values)

        task_info['pool_shared_resources'] = res['pool_shared_resources']
        return task_info

    def revert(self, context, origin, destination, task_info, **kwargs):
        if 'pool_shared_resources' not in task_info:
            LOG.debug(
                "[Task '%s'] Failed to find 'pool_shared_resources' in "
                "provided task_info from original execution of allocation "
                "task for pool '%s'. Defaulting to None.",
                self._task_name, self._minion_pool_id)
            task_info['pool_shared_resources'] = {}

        super(AllocateSharedPoolResourcesTask, self).revert(
            context, origin, destination, task_info, **kwargs)

        with minion_manager_utils.get_minion_pool_lock(
                self._minion_pool_id, external=True):
            updated_values = {
                "pool_shared_resources": None}
            db_api.update_minion_pool(
                context, self._minion_pool_id, updated_values)


class DeallocateSharedPoolResourcesTask(BaseMinionManangerTask):

    def __init__(
            self, minion_pool_id, minion_machine_id, minion_pool_type,
            **kwargs):

        resource_deallocation_task = (
            constants.TASK_TYPE_TEAR_DOWN_SOURCE_POOL_SHARED_RESOURCES)
        if minion_pool_type != constants.PROVIDER_PLATFORM_SOURCE:
            resource_deallocation_task = (
                constants.TASK_TYPE_TEAR_DOWN_DESTINATION_POOL_SHARED_RESOURCES)  # noqa: E501
        super(DeallocateSharedPoolResourcesTask, self).__init__(
            minion_pool_id, minion_machine_id, resource_deallocation_task,
            **kwargs)

    def _get_task_name(self, minion_pool_id, minion_machine_id):
        return MINION_POOL_DEALLOCATE_SHARED_RESOURCES_TASK_NAME_FORMAT % (
            minion_pool_id)

    def execute(self, context, origin, destination, task_info):
        self._add_minion_pool_event(
            context, "Deallocating shared pool resources")
        if 'pool_shared_resources' not in task_info:
            raise exception.InvalidInput(
                "[Task '%s'] No 'pool_shared_resources' provided in the "
                "task_info." % self._task_name)
        execution_info = {
            "pool_environment_options": task_info.get(
                'pool_environment_options', {}),
            "pool_shared_resources": task_info['pool_shared_resources']}
        res = super(DeallocateSharedPoolResourcesTask, self).execute(
            context, origin, destination, execution_info)
        if res:
            LOG.warn(
                "[Task '%s'] Pool '%s' shared resource deallocation task "
                "returned non-void values: %s" % (
                    self._task_name, self._minion_pool_id, res))
        updated_values = {
            "shared_resources": None}
        db_api.update_minion_pool(
            context, self._minion_pool_id, updated_values)
        self._add_minion_pool_event(
            context, "Successfully deallocated shared pool resources")
        return task_info


class AllocateMinionMachineTask(BaseMinionManangerTask):

    def __init__(
            self, minion_pool_id, minion_machine_id, minion_pool_type,
            raise_on_cleanup_failure=True, allocate_to_action=None, **kwargs):
        resource_deployment_task_type = (
            constants.TASK_TYPE_CREATE_SOURCE_MINION_MACHINE)
        resource_cleanup_task_type = (
            constants.TASK_TYPE_DELETE_SOURCE_MINION_MACHINE)
        if minion_pool_type != constants.PROVIDER_PLATFORM_SOURCE:
            resource_deployment_task_type = (
                constants.TASK_TYPE_CREATE_DESTINATION_MINION_MACHINE)
            resource_cleanup_task_type = (
                constants.TASK_TYPE_DELETE_DESTINATION_MINION_MACHINE)
        self._allocate_to_action = allocate_to_action
        self._raise_on_cleanup_failure = raise_on_cleanup_failure
        super(AllocateMinionMachineTask, self).__init__(
            minion_pool_id, minion_machine_id, resource_deployment_task_type,
            cleanup_task_runner_type=resource_cleanup_task_type,
            raise_on_cleanup_failure=raise_on_cleanup_failure, **kwargs)

    def _get_task_name(self, minion_pool_id, minion_machine_id):
        return MINION_POOL_ALLOCATE_MACHINE_TASK_NAME_FORMAT % (
            minion_pool_id, minion_machine_id)

    def execute(self, context, origin, destination, task_info):
        minion_machine = self._get_minion_machine(
            context, self._minion_machine_id, raise_if_not_found=False)
        if minion_machine:
            if minion_machine.allocation_status != (
                    constants.MINION_MACHINE_STATUS_UNINITIALIZED):
                raise exception.InvalidMinionMachineState(
                    "Minion machine entry with ID '%s' already exists within "
                    "the DB and it is in '%s' status instead of the expected "
                    "'%s' status. Existing machine's properties are: %s" % (
                        self._minion_machine_id,
                        minion_machine.allocation_status,
                        constants.MINION_MACHINE_STATUS_UNINITIALIZED,
                        minion_machine.to_dict()))
            if minion_machine.pool_id != self._minion_pool_id:
                raise exception.InvalidMinionMachineState(
                    "Minion machine entry with ID '%s' already exists within "
                    "the DB but it belongs to a different minion pool ('%s') "
                    "from the one requested by this task ('%s')." % (
                        self._minion_machine_id, minion_machine.pool_id,
                        self._minion_pool_id))
            if self._allocate_to_action and (
                    minion_machine.allocated_action and (
                        self._allocate_to_action != (
                            minion_machine.allocated_action))):
                raise exception.InvalidMinionMachineState(
                    "Minion machine entry with ID '%s' already exists in the "
                    "DB but it is already allocated to a different action "
                    "('%s') from the one requested by the task ('%s')." % (
                        self._minion_machine_id,
                        minion_machine.allocated_action,
                        self._allocate_to_action))
            LOG.info(
                "[Task '%s'] Found existing entry in DB for minion machine "
                "'%s'. Reusing that for deployment task.",
                self._task_name, self._minion_machine_id)
            self._set_minion_machine_allocation_status(
                context, self._minion_pool_id, self._minion_machine_id,
                constants.MINION_MACHINE_STATUS_ALLOCATING)
        else:
            minion_machine = models.MinionMachine()
            minion_machine.id = self._minion_machine_id
            minion_machine.pool_id = self._minion_pool_id
            minion_machine.allocation_status = (
                constants.MINION_MACHINE_STATUS_ALLOCATING)
            minion_machine.power_status = (
                constants.MINION_MACHINE_POWER_STATUS_UNINITIALIZED)
            log_msg = (
                "[Task '%s'] Adding new minion machine with ID '%s' "
                "to the DB" % (self._task_name, self._minion_machine_id))
            if self._allocate_to_action:
                minion_machine.allocated_action = self._allocate_to_action
                log_msg = "%s (allocated to action '%s')" % (
                    log_msg, self._allocate_to_action)
            LOG.info(log_msg)
            db_api.add_minion_machine(context, minion_machine)

        execution_info = {
            "pool_environment_options": task_info["pool_environment_options"],
            "pool_identifier": task_info["pool_identifier"],
            "pool_shared_resources": task_info["pool_shared_resources"],
            "pool_os_type": task_info["pool_os_type"]}

        event_message = (
            "Allocating minion machine with internal pool ID '%s'" % (
                self._minion_machine_id))
        if self._allocate_to_action:
            event_message = (
                "%s to be used for transfer action with ID '%s'" % (
                    event_message, self._allocate_to_action))
        self._add_minion_pool_event(
            context, event_message)

        try:
            res = super(AllocateMinionMachineTask, self).execute(
                context, origin, destination, execution_info)
        except Exception:
            self._set_minion_machine_allocation_status(
                context, self._minion_pool_id, self._minion_machine_id,
                constants.MINION_MACHINE_STATUS_ERROR_DEPLOYING)
            raise

        self._add_minion_pool_event(
            context,
            "Successfully allocated minion machine with internal pool "
            "ID '%s'" % (self._minion_machine_id))

        updated_values = {
            "last_used_at": timeutils.utcnow(),
            "allocation_status": constants.MINION_MACHINE_STATUS_AVAILABLE,
            "power_status": constants.MINION_MACHINE_POWER_STATUS_POWERED_ON,
            "connection_info": res['minion_connection_info'],
            "provider_properties": res['minion_provider_properties'],
            "backup_writer_connection_info": res[
                "minion_backup_writer_connection_info"]}
        if self._allocate_to_action:
            updated_values["allocated_action"] = self._allocate_to_action
            updated_values["allocation_status"] = (
                constants.MINION_MACHINE_STATUS_IN_USE)
        self._update_minion_machine(
            context, self._minion_pool_id, self._minion_machine_id,
            updated_values)

        return task_info

    def revert(self, context, origin, destination, task_info, **kwargs):

        minion_provider_properties = None
        task_info_minion_provider_properties = task_info.get(
            'minion_provider_properties')

        # check if the original result is a taskflow Failure object:
        original_result = kwargs.get('result', {})
        if isinstance(original_result, failure.Failure):
            LOG.debug(
                "[Task '%s'] Reversion for allocation Minion Machine '%s' "
                "(pool '%s') received a failure as the original result. "
                "Presuming the original execution failed and found the "
                "following 'machine_properties' key in task info: %s",
                self._task_name, self._minion_machine_id,
                self._minion_pool_id, task_info_minion_provider_properties)
            LOG.warn(
                "[Task '%s'] Allocation failed for machine '%s'. Error "
                "details were: %s",
                self._task_name, self._minion_machine_id,
                original_result.traceback_str)
        # else, if it's a dict, fetch it:
        elif isinstance(original_result, dict):
            minion_provider_properties = original_result.get(
                'minion_provider_properties', None)
        else:
            LOG.warn(
                "[Task '%s'] Allocation task reversion for machine '%s' "
                "of pool '%s' got an unexpected task result type (%s): %s",
                self._task_name, self._minion_machine_id,
                self._minion_pool_id, type(original_result), original_result)
            minion_provider_properties = None

        # default to any minion properties found in the task_info:
        task_info_minion_provider_properties = task_info.get(
            'minion_provider_properties')
        if not minion_provider_properties:
            LOG.debug(
                "[Task '%s'] Reversion for Minion Machine '%s' (pool '%s')"
                " did not return any 'minion_provider_properties' after "
                "its initial execution. Defaulting to task_info value: %s",
                self._task_name, self._minion_machine_id,
                self._minion_pool_id,
                task_info_minion_provider_properties)
            minion_provider_properties = task_info_minion_provider_properties

        # lastly, if the machine entry exists in the DB:
        with minion_manager_utils.get_minion_pool_lock(
            self._minion_pool_id, external=True):
            machine_db_entry = (
                db_api.get_minion_machine(context, self._minion_machine_id))
            if machine_db_entry:
                LOG.debug(
                    "[Task %s] Removing minion machine entry with ID '%s' for "
                    "minion pool '%s' from the DB as part of reversion of its "
                    "allocation task. Machine properties at deletion time "
                    "were: %s", self._task_name, self._minion_machine_id,
                    self._minion_pool_id, machine_db_entry.to_dict())
                if not minion_provider_properties and (
                        machine_db_entry.provider_properties):
                    minion_provider_properties = (
                        machine_db_entry.provider_properties)
                    LOG.debug(
                        "[Task '%s'] Using minion provider properties of "
                        "minion machine with ID '%s' from DB entry during the "
                        "reversion of its allocation task. DB props are: %s",
                        self._task_name, self._minion_machine_id,
                        minion_provider_properties)

            LOG.debug(
                "[Task %s] Deleting minion machine with ID '%s' from the DB.",
                self._task_name, self._minion_machine_id)
            try:
                db_api.delete_minion_machine(context, self._minion_machine_id)
            except Exception:
                LOG.warn(
                    "[Task '%s'] Failed to delete DB entry for minion machine "
                    "'%s' following reversion of its allocation task. Error "
                    "trace was: %s",
                    self._task_name, self._minion_machine_id,
                    utils.get_exception_details())

        if not minion_provider_properties:
            LOG.debug(
                "[Task '%s'] Reversion for Minion Machine '%s' (pool '%s') "
                "found no 'minion_provider_properties'. Presuming the machine "
                "never got created on the cloud and skiping any deletion task",
                self._task_name, self._minion_machine_id,
                self._minion_pool_id)
        else:
            cleanup_info = copy.deepcopy(task_info)
            cleanup_info['minion_provider_properties'] = (
                minion_provider_properties)
            try:
                super(AllocateMinionMachineTask, self).revert(
                    context, origin, destination, cleanup_info, **kwargs)
            except Exception:
                log_msg = (
                    "[Task '%s'] Exception occurred while attempting to "
                    "revert deployment of minion machine with ID '%s' "
                    "for pool '%s'." % (
                        self._task_name, self._minion_machine_id,
                        self._minion_pool_id))
                if not self._raise_on_cleanup_failure:
                    log_msg = (
                        "%s Ignoring exception." % log_msg)
                log_msg = (
                    "%s Exception details were: %s" % (
                        log_msg, utils.get_exception_details))
                LOG.warn(log_msg)
                if self._raise_on_cleanup_failure:
                    raise


class DeallocateMinionMachineTask(BaseMinionManangerTask):

    def __init__(
            self, minion_pool_id, minion_machine_id, minion_pool_type,
            raise_on_cleanup_failure=True, **kwargs):
        resource_deletion_task_type = (
            constants.TASK_TYPE_DELETE_SOURCE_MINION_MACHINE)
        self._raise_on_cleanup_failure = raise_on_cleanup_failure
        if minion_pool_type != constants.PROVIDER_PLATFORM_SOURCE:
            resource_deletion_task_type = (
                constants.TASK_TYPE_DELETE_DESTINATION_MINION_MACHINE)
        super(DeallocateMinionMachineTask, self).__init__(
            minion_pool_id, minion_machine_id, resource_deletion_task_type,
            raise_on_cleanup_failure=raise_on_cleanup_failure, **kwargs)

    def _get_task_name(self, minion_pool_id, minion_machine_id):
        return MINION_POOL_DEALLOCATE_MACHINE_TASK_NAME_FORMAT % (
            minion_pool_id, minion_machine_id)

    def execute(self, context, origin, destination, task_info):
        machine = self._get_minion_machine(context, self._minion_machine_id)
        if not machine:
            LOG.info(
                "[Task '%s'] Could not find machine with ID '%s' in the DB. "
                "Presuming it was already deleted and returning early.",
                self._task_name, self._minion_machine_id)
            return task_info

        self._add_minion_pool_event(
            context,
            "Deallocating minion machine with internal pool ID '%s'" % (
                self._minion_machine_id))

        if machine.provider_properties:
            self._set_minion_machine_allocation_status(
                context, self._minion_pool_id, self._minion_machine_id,
                constants.MINION_MACHINE_STATUS_DEALLOCATING)
            execution_info = {
                "minion_provider_properties": machine.provider_properties}
            try:
                _ = super(DeallocateMinionMachineTask, self).execute(
                    context, origin, destination, execution_info)
            except Exception:
                base_msg = (
                    "Exception occured while deallocating minion machine '%s' "
                    "There might be leftover instance resources requiring "
                    "manual cleanup" % self._minion_machine_id)
                LOG.warn(
                    "[Task '%s'] %s. Error was: %s",
                    self._task_name, base_msg, utils.get_exception_details())
                event_level = constants.TASK_EVENT_INFO
                if self._raise_on_cleanup_failure:
                    event_level = constants.TASK_EVENT_ERROR
                self._add_minion_pool_event(
                    context, base_msg, level=event_level)
                if self._raise_on_cleanup_failure:
                    raise
        else:
            self._add_minion_pool_event(
                context,
                "Minion machine with ID '%s' had no provider properties set. "
                "Presuming it failed to deploy in the first place and simply "
                "removing the machine's entry from the DB" % (
                    self._minion_machine_id))

        LOG.debug(
            "[Task '%s'] Deleting minion machine with ID '%s' from the DB",
            self._task_name, self._minion_machine_id)
        with minion_manager_utils.get_minion_pool_lock(
                self._minion_pool_id, external=True):
            db_api.delete_minion_machine(context, self._minion_machine_id)

        self._add_minion_pool_event(
            context,
            "Successfully deallocated minion machine with internal pool "
            "ID '%s'" % (self._minion_machine_id))

        return task_info


class HealthcheckMinionMachineTask(BaseMinionManangerTask):
    """ Task which healthchecks the given minion machine. """

    def __init__(
            self, minion_pool_id, minion_machine_id, minion_pool_type,
            fail_on_error=False,
            machine_status_on_success=constants.MINION_MACHINE_STATUS_AVAILABLE,  # noqa: E501
            **kwargs):
        self._fail_on_error = fail_on_error
        self._machine_status_on_success = machine_status_on_success
        resource_healthcheck_task = (
            constants.TASK_TYPE_HEALTHCHECK_SOURCE_MINION)
        if minion_pool_type != constants.PROVIDER_PLATFORM_SOURCE:
            resource_healthcheck_task = (
                constants.TASK_TYPE_HEALTHCHECK_DESTINATION_MINION)
        super(HealthcheckMinionMachineTask, self).__init__(
            minion_pool_id, minion_machine_id, resource_healthcheck_task,
            **kwargs)

    def execute(self, context, origin, destination, task_info):
        res = {
            "healthy": True,
            "error": None}

        machine = self._get_minion_machine(
            context, self._minion_machine_id, raise_if_not_found=False)
        if not machine:
            LOG.info(
                "[Task '%s'] Could not find machine with ID '%s' in the DB. "
                "Presuming it was already deleted so healthcheck failed.",
                self._task_name, self._minion_machine_id)
            base_msg = (
                "Could not find minion machine DB entry with ID '%s' for "
                "healtcheck." % self._minion_machine_id)
            self._add_minion_pool_event(
                context,
                "%s Reporting healthcheck as failed" % base_msg,
                level=constants.TASK_EVENT_WARNING)

            if self._fail_on_error:
                raise exception.InvalidMinionMachineState(base_msg)
            return {"healthy": False, "error": base_msg}

        machine_error_statuses = [
            constants.MINION_MACHINE_STATUS_ERROR,
            constants.MINION_MACHINE_STATUS_POWER_ERROR,
            constants.MINION_MACHINE_STATUS_ERROR_DEPLOYING]
        if machine.allocation_status in machine_error_statuses:
            base_msg = (
                "Minion Machine with ID '%s' is marked as '%s' in the DB." % (
                    self._minion_machine_id, machine.allocation_status))
            LOG.debug(
                "[Task '%s'] %s" % (self._task_name, base_msg))
            self._add_minion_pool_event(
                context,
                "%s Reporting healthcheck as failed" % base_msg,
                level=constants.TASK_EVENT_WARNING)

            if self._fail_on_error:
                raise exception.InvalidMinionMachineState(base_msg)
            return {"healthy": False, "error": base_msg}

        self._add_minion_pool_event(
            context,
            "Healthchecking  minion machine with internal pool ID '%s'" % (
                self._minion_machine_id))

        execution_info = {
            "minion_provider_properties": machine.provider_properties,
            "minion_connection_info": machine.connection_info}
        try:
            _ = super(HealthcheckMinionMachineTask, self).execute(
                context, origin, destination, execution_info)
            self._add_minion_pool_event(
                context,
                "Successfully healtchecked minion machine with internal "
                "pool ID '%s'" % self._minion_machine_id)
            self._set_minion_machine_allocation_status(
                context, self._minion_pool_id, self._minion_machine_id,
                self._machine_status_on_success)
        except Exception as ex:
            self._add_minion_pool_event(
                context,
                "Healtcheck for machine with internal pool ID '%s' has "
                "failed." % (self._minion_machine_id),
                level=constants.TASK_EVENT_WARNING)
            LOG.debug(
                "[Task '%s'] Healtcheck failed for machine '%s' of pool '%s'. "
                "Full trace was:\n%s", self._task_name,
                self._minion_machine_id, self._minion_pool_id,
                utils.get_exception_details())
            self._set_minion_machine_allocation_status(
                context, self._minion_pool_id, self._minion_machine_id,
                constants.MINION_MACHINE_STATUS_ERROR)
            if not self._fail_on_error:
                res = {
                    "healthy": False,
                    "error": str(ex)}
            else:
                raise

        return res

    def _get_task_name(self, minion_pool_id, minion_machine_id):
        return self.get_healthcheck_task_name(
            minion_pool_id, minion_machine_id)

    @classmethod
    def get_healthcheck_task_name(cls, minion_pool_id, minion_machine_id):
        return MINION_POOL_HEALTHCHECK_MACHINE_TASK_NAME_FORMAT % (
            minion_pool_id, minion_machine_id)


class MinionMachineHealtchcheckDecider(object):
    """ A callable to green/redlight further execution based on the result. """

    def __init__(
            self, minion_pool_id, minion_machine_id,
            on_successful_healthcheck=True):
        self._minion_pool_id = minion_pool_id
        self._minion_machine_id = minion_machine_id
        self._on_success = on_successful_healthcheck

    def __call__(self, history):
        healthcheck_task_name = (
            HealthcheckMinionMachineTask.get_healthcheck_task_name(
                self._minion_pool_id, self._minion_machine_id))

        if not history and healthcheck_task_name not in history:
            LOG.warn(
                "Could not find healthceck result for minion machine '%s' "
                "of pool '%s' (task name '%s'). NOT greenlighting futher "
                "tasks.", self._minion_machine_id, self._minion_pool_id,
                healthcheck_task_name)
            return False

        healtcheck_result = history[healthcheck_task_name]
        if healtcheck_result.get('healthy'):
            LOG.debug(
                "Healtcheck task '%s' confirmed worker health. Decider "
                "returning %s", healthcheck_task_name,
                self._on_success)
            return self._on_success
        else:
            LOG.debug(
                "Healtcheck task '%s' denied worker health. Decider "
                "returning %s. Error mesage was: %s",
                healthcheck_task_name, not self._on_success,
                healtcheck_result.get('error'))
            return not self._on_success


class PowerOnMinionMachineTask(BaseMinionManangerTask):

    def __init__(
            self, minion_pool_id, minion_machine_id, minion_pool_type,
            fail_on_error=True, **kwargs):
        self._fail_on_error = fail_on_error
        power_on_task_type = (
            constants.TASK_TYPE_POWER_ON_SOURCE_MINION)
        if minion_pool_type != constants.PROVIDER_PLATFORM_SOURCE:
            power_on_task_type = (
                constants.TASK_TYPE_POWER_ON_DESTINATION_MINION)
        super(PowerOnMinionMachineTask, self).__init__(
            minion_pool_id, minion_machine_id, power_on_task_type,
            **kwargs)

    def _get_task_name(self, minion_pool_id, minion_machine_id):
        return MINION_POOL_POWER_ON_MACHINE_TASK_NAME_FORMAT % (
            minion_pool_id, minion_machine_id)

    def execute(self, context, origin, destination, task_info):
        machine = self._get_minion_machine(
            context, self._minion_machine_id, raise_if_not_found=True)

        if (machine.power_status ==
                constants.MINION_MACHINE_POWER_STATUS_POWERED_ON):
            LOG.debug(
                "[Task '%s'] Minion machine with ID '%s' from pool '%s' is "
                "already marked as powered on. Returning early." % (
                    self._task_name, self._minion_machine_id,
                    self._minion_pool_id))
            return task_info

        if (machine.power_status !=
                constants.MINION_MACHINE_POWER_STATUS_POWERED_OFF):
            raise exception.InvalidMinionMachineState(
                "Minion machine with ID '%s' from pool '%s' is in '%s' state "
                "instead of the expected '%s' required for it to be powered "
                "on." % (
                    self._minion_machine_id, self._minion_pool_id,
                    machine.power_status,
                    constants.MINION_MACHINE_POWER_STATUS_POWERED_OFF))

        execution_info = {
            "minion_provider_properties": machine.provider_properties}
        try:
            self._set_minion_machine_power_status(
                context, self._minion_pool_id,
                self._minion_machine_id,
                constants.MINION_MACHINE_POWER_STATUS_POWERING_ON)
            _ = super(PowerOnMinionMachineTask, self).execute(
                context, origin, destination, execution_info)
            self._set_minion_machine_power_status(
                context, self._minion_pool_id,
                self._minion_machine_id,
                constants.MINION_MACHINE_POWER_STATUS_POWERED_ON)
            self._add_minion_pool_event(
                context,
                "Successfully powered on minion machine with internal pool "
                "ID '%s'" % self._minion_machine_id)
        except Exception:
            base_msg = (
                "[Task '%s'] Exception occurred while powering on minion "
                "machine with ID '%s' of pool '%s'." % (
                    self._task_name, self._minion_machine_id,
                    self._minion_pool_id))
            LOG.warn(
                "%s Error details were: %s" % (
                    base_msg, utils.get_exception_details()))
            self._add_minion_pool_event(
                context,
                "Exception occurred while powering on minion machine with "
                "internal pool ID '%s'. The minion machine will be marked "
                "as ERROR'd and automatically redeployed later" % (
                    self._minion_machine_id),
                level=constants.TASK_EVENT_ERROR)
            self._set_minion_machine_allocation_status(
                context, self._minion_pool_id, self._minion_machine_id,
                constants.MINION_MACHINE_STATUS_POWER_ERROR)
            if self._fail_on_error:
                raise exception.CoriolisException(base_msg)

        return task_info


class PowerOffMinionMachineTask(BaseMinionManangerTask):

    def __init__(
            self, minion_pool_id, minion_machine_id, minion_pool_type,
            fail_on_error=True,
            status_once_powered_off=constants.MINION_MACHINE_STATUS_AVAILABLE,
            **kwargs):
        self._fail_on_error = fail_on_error
        self._status_once_powered_off = status_once_powered_off
        power_on_task_type = (
            constants.TASK_TYPE_POWER_OFF_SOURCE_MINION)
        if minion_pool_type != constants.PROVIDER_PLATFORM_SOURCE:
            power_on_task_type = (
                constants.TASK_TYPE_POWER_OFF_DESTINATION_MINION)
        super(PowerOffMinionMachineTask, self).__init__(
            minion_pool_id, minion_machine_id, power_on_task_type,
            **kwargs)

    def _get_task_name(self, minion_pool_id, minion_machine_id):
        return MINION_POOL_POWER_OFF_MACHINE_TASK_NAME_FORMAT % (
            minion_pool_id, minion_machine_id)

    def execute(self, context, origin, destination, task_info):
        machine = self._get_minion_machine(
            context, self._minion_machine_id, raise_if_not_found=True)

        if machine.power_status == (
                constants.MINION_MACHINE_POWER_STATUS_POWERED_OFF):
            LOG.debug(
                "[Task '%s'] Minion machine with ID '%s' from pool '%s' is "
                "already marked as powered off. Returning early." % (
                    self._task_name, self._minion_machine_id,
                    self._minion_pool_id))
            return task_info

        execution_info = {
            "minion_provider_properties": machine.provider_properties}
        try:
            self._set_minion_machine_power_status(
                context, self._minion_pool_id,
                self._minion_machine_id,
                constants.MINION_MACHINE_POWER_STATUS_POWERING_OFF)
            _ = super(PowerOffMinionMachineTask, self).execute(
                context, origin, destination, execution_info)
            self._set_minion_machine_power_status(
                context, self._minion_pool_id,
                self._minion_machine_id,
                constants.MINION_MACHINE_POWER_STATUS_POWERED_OFF)
            self._set_minion_machine_allocation_status(
                context, self._minion_pool_id, self._minion_machine_id,
                self._status_once_powered_off)
            self._add_minion_pool_event(
                context,
                "Successfully powered off minion machine with internal pool "
                "ID '%s'" % self._minion_machine_id)
        except Exception:
            base_msg = (
                "[Task '%s'] Exception occurred while powering off minion "
                "machine with ID '%s' of pool '%s'." % (
                    self._task_name, self._minion_machine_id,
                    self._minion_pool_id))
            self._add_minion_pool_event(
                context,
                "Exception occurred while powering off minion machine with "
                "internal pool ID '%s'. The minion machine will be marked "
                "as ERROR'd and automatically redeployed later." % (
                    self._minion_machine_id),
                level=constants.TASK_EVENT_ERROR)
            self._set_minion_machine_allocation_status(
                context, self._minion_pool_id, self._minion_machine_id,
                constants.MINION_MACHINE_STATUS_POWER_ERROR)
            LOG.warn(
                "%s Error details were: %s" % (
                    base_msg, utils.get_exception_details()))
            if self._fail_on_error:
                raise exception.CoriolisException(base_msg)

        return task_info
