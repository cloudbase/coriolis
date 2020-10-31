# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import abc
import copy

from oslo_log import log as logging

from coriolis import constants
from coriolis.db import api as db_api
from coriolis.db.sqlalchemy import models
from coriolis.taskflow import base as coriolis_taskflow_base


LOG = logging.getLogger(__name__)

MINION_POOL_DEPLOYMENT_FLOW_NAME_FORMAT = "pool-%s-deployment"
MINION_POOL_VALIDATION_TASK_NAME_FORMAT = "pool-%s-validation"
MINION_POOL_UPDATE_STATUS_TASK_NAME_FORMAT = "pool-%s-update-status-%s"
MINION_POOL_SET_UP_SHARED_RESOURCES_TASK_NAME_FORMAT = (
    "pool-%s-set-up-shared-resources")
MINION_POOL_TEAR_DOWN_SHARED_RESOURCES_TASK_NAME_FORMAT = (
    "pool-%s-tear-down-shared-resources")
MINION_POOL_CREATE_MINIONS_SUBFLOW_NAME_FORMAT = (
    "pool-%s-machines-deployment")
MINION_POOL_CREATE_MINION_TASK_NAME_FORMAT = (
    "pool-%s-machine-%s-deployment")
MINION_POOL_DELETE_MINION_TASK_NAME_FORMAT = (
    "pool-%s-machine-%s-deletion")



class UpdateMinionPoolStatusTask(coriolis_taskflow_base.BaseCoriolisTaskflowTask):
    """Task which updates the status of the given pool.
    Is capable of recording and reverting the state.
    """

    def __init__(
            self, minion_pool_id, target_status,
            status_to_revert_to=None, **kwargs):

        self._target_status = target_status
        self._minion_pool_id = minion_pool_id
        self._task_name = (MINION_POOL_UPDATE_STATUS_TASK_NAME_FORMAT % (
            self._minion_pool_id, self._target_status)).lower()
        self._previous_status = status_to_revert_to

        super(UpdateMinionPoolStatusTask, self).__init__(
            name=self._task_name, **kwargs)

    def _add_minion_pool_event(
            self, ctxt, message, level=constants.TASK_EVENT_INFO):
        db_api.add_minion_pool_event(
            ctxt, self._minion_pool_id, level, message)

    def execute(self, context, *args, **kwargs):
        super(UpdateMinionPoolStatusTask, self).execute(*args, **kwargs)

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

        if minion_pool.status == self._previous_status:
            LOG.debug(
                "[Task '%s'] Minion pool '%s' is/was already reverted to "
                "'%s'." % (
                    self._task_name, self._minion_pool_id,
                    self._previous_status))
        else:
            if minion_pool.status != self._target_status:
                LOG.warn(
                    "[Task %s] Minion pool '%s' is in status '%s', which is "
                    "neither the previous status ('%s'), nor the newly-set "
                    "status ('%s'). Reverting to '%s' anyway.",
                    self._task_name, self._minion_pool_id, minion_pool.status,
                    self._previous_status, self._target_status,
                    self._previous_status)
            LOG.debug(
                "[Task '%s'] Reverting pool '%s' status from '%s' to "
                "'%s'" % (
                    self._task_name, self._minion_pool_id, minion_pool.status,
                    self._previous_status))
            db_api.set_minion_pool_status(
                context, self._minion_pool_id, self._previous_status)
            self._add_minion_pool_event(
                context,
                "Pool status reverted from '%s' to '%s'" % (
                    minion_pool.status, self._previous_status))


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

    def _add_minion_pool_event(
            self, ctxt, message, level=constants.TASK_EVENT_INFO):
        db_api.add_minion_pool_event(
            ctxt, self._minion_pool_id, level, message)

    def execute(self, context, origin, destination, task_info, **kwargs):
        LOG.info(
            "Starting minion pool task '%s' (runner type '%s')",
            self._task_name, self._main_task_runner_type)
        res = super(BaseMinionManangerTask, self).execute(
            context, origin, destination, task_info, **kwargs)
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
            "were: %s" % (
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


class DeploySharedPoolResourcesTask(BaseMinionManangerTask):

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
        super(DeploySharedPoolResourcesTask, self).__init__(
            minion_pool_id, minion_machine_id, resource_deployment_task_type,
            cleanup_task_runner_type=resource_cleanup_task_type)

    def _get_task_name(self, minion_pool_id, minion_machine_id):
        return MINION_POOL_SET_UP_SHARED_RESOURCES_TASK_NAME_FORMAT % (
            minion_pool_id)

    def execute(self, context, origin, destination, task_info):
        self._add_minion_pool_event(
            context, "Deploying shared pool resources")
        res = super(DeploySharedPoolResourcesTask, self).execute(
            context, origin, destination, task_info)
        pool_shared_resources = res['pool_shared_resources']
        self._add_minion_pool_event(
            context, "Successfully deployed shared pool resources: %s" % (
                pool_shared_resources))

        updated_values = {
            "shared_resources": pool_shared_resources}
        db_api.add_minion_pool_event(
            context, self._minion_pool_id, constants.TASK_EVENT_INFO,
            "Successfully deployed shared pool resources: %s" % (
                pool_shared_resources))
        db_api.update_minion_pool(
            context, self._minion_pool_id, updated_values)

        task_info['pool_shared_resources'] = res['pool_shared_resources']
        return task_info

    def revert(self, context, origin, destination, task_info, **kwargs):
        if 'pool_shared_resources' not in task_info:
            task_info['pool_shared_resources'] = {}

        res = super(DeploySharedPoolResourcesTask, self).revert(
            context, origin, destination, task_info)

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


class DeployMinionMachineTask(BaseMinionManangerTask):

    def __init__(
            self, minion_pool_id, minion_machine_id, minion_pool_type,
            **kwargs):
        resource_deployment_task_type = (
            constants.TASK_TYPE_CREATE_SOURCE_MINION_MACHINE)
        resource_cleanup_task_type = (
            constants.TASK_TYPE_DELETE_SOURCE_MINION_MACHINE)
        if minion_pool_type != constants.PROVIDER_PLATFORM_SOURCE:
            resource_deployment_task_type = (
                constants.TASK_TYPE_DELETE_SOURCE_MINION_MACHINE)
            resource_cleanup_task_type = (
                constants.TASK_TYPE_DELETE_DESTINATION_MINION_MACHINE)
        super(DeployMinionMachineTask, self).__init__(
            minion_pool_id, minion_machine_id, resource_deployment_task_type,
            cleanup_task_runner_type=resource_cleanup_task_type)

    def _get_task_name(self, minion_pool_id, minion_machine_id):
        return MINION_POOL_CREATE_MINION_TASK_NAME_FORMAT % (
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

        res = super(DeployMinionMachineTask, self).execute(
            context, origin, destination, execution_info)

        updated_values = {
            "status": constants.MINION_MACHINE_STATUS_AVAILABLE,
            "connection_info": res['minion_connection_info'],
            "provider_properties": res['minion_provider_properties'],
            "backup_writer_connection_info": res[
                "minion_backup_writer_connection_info"]}
        db_api.update_minion_machine(
            context, self._minion_machine_id, updated_values)

        return task_info

    def revert(self, context, origin, destination, task_info, **kwargs):
        original_result = kwargs.get('result', {})
        if original_result and (
                isinstance(original_result, dict) and (
                    'minion_provider_properties' not in original_result)):
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
        _ = super(DeployMinionMachineTask, self).revert(
            context, origin, destination, cleanup_info)

        if db_api.get_minion_machine(context, self._minion_machine_id):
            LOG.debug(
                "Removing minion machine entry with ID '%s' for minion pool "
                "'%s' from DB.", self._minion_machine_id, self._minion_pool_id)
            db_api.delete_minion_machine(context, self._minion_machine_id)

        return task_info
