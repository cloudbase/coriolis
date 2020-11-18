# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import abc
import copy

from oslo_log import log as logging
from oslo_utils import timeutils

from coriolis import constants
from coriolis import exception
from coriolis import utils
from coriolis.db import api as db_api
from coriolis.db.sqlalchemy import models
from coriolis.minion_manager.rpc import utils as minion_manager_utils
from coriolis.taskflow import base as coriolis_taskflow_base


LOG = logging.getLogger(__name__)

MINION_POOL_ALLOCATION_FLOW_NAME_FORMAT = "pool-%s-allocation"
MINION_POOL_DEALLOCATION_FLOW_NAME_FORMAT = "pool-%s-deallocation"
MINION_POOL_HEALTHCHECK_FLOW_NAME_FORMAT = "pool-%s-healthcheck"
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
MINION_POOL_ALLOCATE_MACHINE_TASK_NAME_FORMAT = (
    "pool-%s-machine-%s-allocation")
MINION_POOL_DEALLOCATE_MACHINE_TASK_NAME_FORMAT = (
    "pool-%s-machine-%s-deallocation")


class UpdateMinionPoolStatusTask(
        coriolis_taskflow_base.BaseCoriolisTaskflowTask):
    """Task which updates the status of the given pool.
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

    def _add_minion_pool_event(
            self, ctxt, message, level=constants.TASK_EVENT_INFO):
        LOG.debug("Minion pool '%s' event: %s", self._minion_pool_id, message)
        db_api.add_minion_pool_event(
            ctxt, self._minion_pool_id, level, message)

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
            db_api.set_minion_pool_status(
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
            db_api.set_minion_pool_status(
                context, self._minion_pool_id, previous_status)
            self._add_minion_pool_event(
                context,
                "Pool status reverted from '%s' to '%s'" % (
                    minion_pool.status, previous_status))


class BaseMinionManangerTask(coriolis_taskflow_base.BaseRunWorkerTask):

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

    def _get_minion_machine(
            self, ctxt, minion_machine_id,
            raise_if_not_found=False):
        machine = db_api.get_minion_machine(ctxt, minion_machine_id)
        if not machine and raise_if_not_found:
            raise exception.NotFound(
                "Could not find minion machine with ID '%s'" % (
                    minion_machine_id))
        return machine

    def _add_minion_pool_event(
            self, ctxt, message, level=constants.TASK_EVENT_INFO):
        LOG.debug("Minion pool '%s' event: %s", self._minion_pool_id, message)
        db_api.add_minion_pool_event(
            ctxt, self._minion_pool_id, level, message)

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
        return super(BaseMinionManangerTask, self).revert(
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
        res = super(ValidateMinionPoolOptionsTask, self).execute(
            context, origin, destination, task_info)
        self._add_minion_pool_event(
            context, "Successfully validated minion pool options")

    def revert(self, context, origin, destination, task_info, **kwargs):
        LOG.debug("[%s] Nothing to revert for validation", self._task_name)
        res = super(ValidateMinionPoolOptionsTask, self).revert(
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
                constants.TASK_TYPE_TEAR_DOWN_DESTINATION_POOL_SHARED_RESOURCES)
        super(AllocateSharedPoolResourcesTask, self).__init__(
            minion_pool_id, minion_machine_id, resource_deployment_task_type,
            cleanup_task_runner_type=resource_cleanup_task_type, **kwargs)

    def _get_task_name(self, minion_pool_id, minion_machine_id):
        return MINION_POOL_ALLOCATE_SHARED_RESOURCES_TASK_NAME_FORMAT % (
            minion_pool_id)

    def execute(self, context, origin, destination, task_info):
        self._add_minion_pool_event(
            context, "Deploying shared pool resources")
        res = super(AllocateSharedPoolResourcesTask, self).execute(
            context, origin, destination, task_info)
        pool_shared_resources = res['pool_shared_resources']
        self._add_minion_pool_event(
            context, "Successfully deployed shared pool resources")

        updated_values = {
            "shared_resources": pool_shared_resources}
        db_api.add_minion_pool_event(
            context, self._minion_pool_id, constants.TASK_EVENT_INFO,
            "Successfully deployed shared pool resources" % (
                pool_shared_resources))
        db_api.update_minion_pool(
            context, self._minion_pool_id, updated_values)

        task_info['pool_shared_resources'] = res['pool_shared_resources']
        return task_info

    def revert(self, context, origin, destination, task_info, **kwargs):
        if 'pool_shared_resources' not in task_info:
            task_info['pool_shared_resources'] = {}

        res = super(AllocateSharedPoolResourcesTask, self).revert(
            context, origin, destination, task_info, **kwargs)

        if res and res.get('pool_shared_resources'):
            LOG.warn(
                "Pool shared resources cleanup task has returned non-void "
                "resources dict: %s", res.get['pool_shared_resources'])

        updated_values = {
            "pool_shared_resources": None}
        db_api.update_minion_pool(
            context, self._minion_pool_id, updated_values)

        task_info['pool_shared_resources'] = None
        return task_info



class DeallocateSharedPoolResourcesTask(BaseMinionManangerTask):

    def __init__(
            self, minion_pool_id, minion_machine_id, minion_pool_type,
            **kwargs):

        resource_deallocation_task = (
            constants.TASK_TYPE_TEAR_DOWN_SOURCE_POOL_SHARED_RESOURCES)
        if minion_pool_type != constants.PROVIDER_PLATFORM_SOURCE:
            resource_deallocation_task = (
                constants.TASK_TYPE_TEAR_DOWN_DESTINATION_POOL_SHARED_RESOURCES)
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
            allocate_to_action=None, **kwargs):
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
        super(AllocateMinionMachineTask, self).__init__(
            minion_pool_id, minion_machine_id, resource_deployment_task_type,
            cleanup_task_runner_type=resource_cleanup_task_type, **kwargs)

    def _get_task_name(self, minion_pool_id, minion_machine_id):
        return MINION_POOL_ALLOCATE_MACHINE_TASK_NAME_FORMAT % (
            minion_pool_id, minion_machine_id)

    def execute(self, context, origin, destination, task_info):
        minion_machine = models.MinionMachine()
        minion_machine.id = self._minion_machine_id
        minion_machine.pool_id = self._minion_pool_id
        minion_machine.status = (
            constants.MINION_MACHINE_STATUS_DEPLOYING)
        db_api.add_minion_machine(context, minion_machine)

        execution_info = {
            "pool_environment_options": task_info["pool_environment_options"],
            "pool_identifier": task_info["pool_identifier"],
            "pool_shared_resources": task_info["pool_shared_resources"],
            "pool_os_type": task_info["pool_os_type"]}

        self._add_minion_pool_event(
            context,
            "Allocating minion machine with internal pool ID '%s'" % (
                self._minion_machine_id))

        try:
            res = super(AllocateMinionMachineTask, self).execute(
                context, origin, destination, execution_info)
        except:
            db_api.update_minion_machine(
                context, self._minion_machine_id, {
                    "status": constants.MINION_MACHINE_STATUS_ERROR_DEPLOYING})
            raise

        self._add_minion_pool_event(
            context,
            "Successfully allocated minion machine with internal pool "
            "ID '%s'" % (self._minion_machine_id))

        updated_values = {
            "allocated_at": timeutils.utcnow(),
            "status": constants.MINION_MACHINE_STATUS_AVAILABLE,
            "connection_info": res['minion_connection_info'],
            "provider_properties": res['minion_provider_properties'],
            "backup_writer_connection_info": res[
                "minion_backup_writer_connection_info"]}
        if self._allocate_to_action:
            updated_values["allocated_action"] = self._allocate_to_action
            updated_values["status"] = (
                constants.MINION_MACHINE_STATUS_RESERVED)
        db_api.update_minion_machine(
            context, self._minion_machine_id, updated_values)

        return task_info

    def revert(self, context, origin, destination, task_info, **kwargs):
        original_result = kwargs.get('result', {})
        if original_result:
            if not isinstance(original_result, dict):
                LOG.debug(
                    "Reversion for Minion Machine '%s' (pool '%s') did not "
                    "receive any dict result from the original run. Presuming "
                    "that the task had not initially run successfully. "
                    "Result was: %s",
                    self._minion_machine_id, self._minion_pool_id,
                    original_result)
                return task_info
            elif 'minion_provider_properties' not in original_result:
                LOG.debug(
                    "Reversion for Minion Machine '%s' (pool '%s') did not "
                    "receive any result from the original run. Presuming "
                    "that the task had not initially run successfully. "
                    "Result was: %s",
                    self._minion_machine_id, self._minion_pool_id,
                    original_result)
                return task_info

        cleanup_info = copy.deepcopy(task_info)
        cleanup_info['minion_provider_properties'] = original_result[
            'minion_provider_properties']
        _ = super(AllocateMinionMachineTask, self).revert(
            context, origin, destination, cleanup_info, **kwargs)

        if db_api.get_minion_machine(context, self._minion_machine_id):
            LOG.debug(
                "Removing minion machine entry with ID '%s' for minion pool "
                "'%s' from the DB.", self._minion_machine_id, self._minion_pool_id)
            db_api.delete_minion_machine(context, self._minion_machine_id)

        return task_info


class DeallocateMinionMachineTask(BaseMinionManangerTask):

    def __init__(
            self, minion_pool_id, minion_machine_id, minion_pool_type,
            **kwargs):
        resource_deletion_task_type = (
            constants.TASK_TYPE_DELETE_SOURCE_MINION_MACHINE)
        if minion_pool_type != constants.PROVIDER_PLATFORM_SOURCE:
            resource_deletion_task_type = (
                constants.TASK_TYPE_DELETE_DESTINATION_MINION_MACHINE)
        super(DeallocateMinionMachineTask, self).__init__(
            minion_pool_id, minion_machine_id, resource_deletion_task_type,
            **kwargs)

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
            execution_info = {
                "minion_provider_properties": machine.provider_properties}
            _ = super(DeallocateMinionMachineTask, self).execute(
                context, origin, destination, execution_info)
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
            **kwargs):
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

        machine = self._get_minion_machine(context, self._minion_machine_id)
        if not machine:
            LOG.info(
                "[Task '%s'] Could not find machine with ID '%s' in the DB. "
                "Presuming it was already deleted so healthcheck failed.",
                self._task_name, self._minion_machine_id)
            return {
                "healthy": False,
                "error": "Machine not found."}

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
        except Exception as ex:
            self._add_minion_pool_event(
                context,
                "Healtcheck for machine '%s' has failed." % (
                    self._minion_machine_id),
                level=constants.TASK_EVENT_WARNING)
            LOG.debug(
                "[Task '%s'] Healtcheck failed for machine '%s' of pool '%s'. "
                "Full trace was:\n%s", self._task_name,
                self._minion_machine_id, self._minion_pool_id,
                utils.get_exception_details())
            res = {
                "healthy": False,
                "error": str(ex)}

        return res

    def _get_task_name(self, minion_pool_id, minion_machine_id):
        return self.get_healthcheck_task_name(
            minion_pool_id, minion_machine_id)

    @classmethod
    def get_healthcheck_task_name(cls, minion_pool_id, minion_machine_id):
        return MINION_POOL_HEALTHCHECK_MACHINE_TASK_NAME_FORMAT % (
            minion_pool_id, minion_machine_id)

    @classmethod
    def make_minion_machine_healtcheck_decider(
            cls, minion_pool_id, minion_machine_id,
            on_successful_healthcheck=True):
        def _healthcheck_decider(history):
            healthcheck_task_name = (
                cls.get_healthcheck_task_name(
                    minion_pool_id, minion_machine_id))

            if not history and healthcheck_task_name not in history:
                LOG.warn(
                    "Could not find healthceck result for minion machine '%s' "
                    "of pool '%s' (task name '%s'). NOT grennlighting futher "
                    "tasks.", minion_machine_id, minion_pool_id,
                    healthcheck_task_name)

            healtcheck_result = history[healthcheck_task_name]
            if healtcheck_result['healthy']:
                LOG.debug(
                    "Healtcheck task '%s' confirmed worker health. Decider "
                    "returning %s", healthcheck_task_name,
                    on_successful_healthcheck)
                return on_successful_healthcheck
            else:
                LOG.debug(
                    "Healtcheck task '%s' denied worker health. Decider "
                    "returning %s", healthcheck_task_name,
                    not on_successful_healthcheck)
                return not on_successful_healthcheck

        return _healthcheck_decider
