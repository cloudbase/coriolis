# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import contextlib
import copy
import functools
import itertools
import random
import time
import uuid

from oslo_concurrency import lockutils
from oslo_config import cfg
from oslo_log import log as logging

from coriolis import constants
from coriolis import context
from coriolis.db import api as db_api
from coriolis.db.sqlalchemy import models
from coriolis import exception
from coriolis import keystone
from coriolis.licensing import client as licensing_client
from coriolis.minion_manager.rpc import client as minion_manager_client
from coriolis.replica_cron.rpc import client as rpc_cron_client
from coriolis.scheduler.rpc import client as rpc_scheduler_client
from coriolis import schemas
from coriolis.tasks import factory as tasks_factory
from coriolis import utils
from coriolis.worker.rpc import client as rpc_worker_client


VERSION = "1.0"

LOG = logging.getLogger(__name__)


CONDUCTOR_OPTS = [
    cfg.BoolOpt("debug_os_morphing_errors",
                default=False,
                help="If set, any OSMorphing task which errors out will have "
                     "all of its following tasks unscheduled so as to allow "
                     "for live debugging of the OSMorphing setup.")
]

CONF = cfg.CONF
CONF.register_opts(CONDUCTOR_OPTS, 'conductor')

TASK_DEADLOCK_ERROR_MESSAGE = (
    "A fatal deadlock has occurred. Further debugging is required. "
    "Please review the Conductor logs and contact support for assistance.")

RPC_TOPIC_TO_CLIENT_CLASS_MAP = {
    constants.WORKER_MAIN_MESSAGING_TOPIC: rpc_worker_client.WorkerClient,
    constants.SCHEDULER_MAIN_MESSAGING_TOPIC: (
        rpc_scheduler_client.SchedulerClient),
    constants.REPLICA_CRON_MAIN_MESSAGING_TOPIC: (
        rpc_cron_client.ReplicaCronClient)}


def endpoint_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, endpoint_id, *args, **kwargs):
        @lockutils.synchronized(
            constants.ENDPOINT_LOCK_NAME_FORMAT % endpoint_id,
            external=True)
        def inner():
            return func(self, ctxt, endpoint_id, *args, **kwargs)
        return inner()
    return wrapper


def replica_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, replica_id, *args, **kwargs):
        @lockutils.synchronized(
            constants.REPLICA_LOCK_NAME_FORMAT % replica_id,
            external=True)
        def inner():
            return func(self, ctxt, replica_id, *args, **kwargs)
        return inner()
    return wrapper


def schedule_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, replica_id, schedule_id, *args, **kwargs):
        @lockutils.synchronized(
            constants.SCHEDULE_LOCK_NAME_FORMAT % schedule_id,
            external=True)
        def inner():
            return func(self, ctxt, replica_id, schedule_id, *args, **kwargs)
        return inner()
    return wrapper


def task_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, task_id, *args, **kwargs):
        @lockutils.synchronized(
            constants.TASK_LOCK_NAME_FORMAT % task_id,
            external=True)
        def inner():
            return func(self, ctxt, task_id, *args, **kwargs)
        return inner()
    return wrapper


def parent_tasks_execution_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, task_id, *args, **kwargs):
        task = db_api.get_task(ctxt, task_id)
        @lockutils.synchronized(
            constants.EXECUTION_LOCK_NAME_FORMAT % task.execution_id,
            external=True)
        @lockutils.synchronized(
            constants.TASK_LOCK_NAME_FORMAT % task_id,
            external=True)
        def inner():
            return func(self, ctxt, task_id, *args, **kwargs)
        return inner()
    return wrapper


def migration_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, migration_id, *args, **kwargs):
        @lockutils.synchronized(
            constants.MIGRATION_LOCK_NAME_FORMAT % migration_id,
            external=True)
        def inner():
            return func(self, ctxt, migration_id, *args, **kwargs)
        return inner()
    return wrapper


def tasks_execution_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, replica_id, execution_id, *args, **kwargs):
        @lockutils.synchronized(
            constants.EXECUTION_LOCK_NAME_FORMAT % execution_id,
            external=True)
        def inner():
            return func(self, ctxt, replica_id, execution_id, *args, **kwargs)
        return inner()
    return wrapper


def region_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, region_id, *args, **kwargs):
        @lockutils.synchronized(
            constants.REGION_LOCK_NAME_FORMAT % region_id,
            external=True)
        def inner():
            return func(self, ctxt, region_id, *args, **kwargs)
        return inner()
    return wrapper


def service_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, service_id, *args, **kwargs):
        @lockutils.synchronized(
            constants.SERVICE_LOCK_NAME_FORMAT % service_id,
            external=True)
        def inner():
            return func(self, ctxt, service_id, *args, **kwargs)
        return inner()
    return wrapper


class ConductorServerEndpoint(object):
    def __init__(self):
        self._licensing_client = licensing_client.LicensingClient.from_env()
        self._worker_client_instance = None
        self._scheduler_client_instance = None
        self._replica_cron_client_instance = None

    # NOTE(aznashwan): it is unsafe to fork processes with pre-instantiated
    # oslo_messaging clients as the underlying eventlet thread queues will
    # be invalidated. Considering this class both serves from a "main
    # process" as well as forking child processes, it is safest to
    # instantiate the clients only when needed:
    @property
    def _worker_client(self):
        if not self._worker_client_instance:
            self._worker_client_instance = (
                rpc_worker_client.WorkerClient())
        return self._worker_client_instance

    @property
    def _scheduler_client(self):
        if not self._scheduler_client_instance:
            self._scheduler_client_instance = (
                rpc_scheduler_client.SchedulerClient())
        return self._scheduler_client_instance

    @property
    def _replica_cron_client(self):
        if not self._replica_cron_client_instance:
            self._replica_cron_client_instance = (
                rpc_cron_client.ReplicaCronClient())
        return self._replica_cron_client_instance

    @property
    def _minion_manager_client(self):
        return minion_manager_client.MinionManagerClient()

    def get_all_diagnostics(self, ctxt):
        diagnostics = [
            self.get_diagnostics(ctxt),
            self._replica_cron_client.get_diagnostics(ctxt),
            self._scheduler_client.get_diagnostics(ctxt)]
        worker_diagnostics = []
        for worker_service in self._scheduler_client.get_workers_for_specs(
                ctxt):
            worker_rpc = self._get_worker_rpc_for_host(worker_service['host'])
            diagnostics.append(worker_rpc.get_diagnostics(ctxt))

        return diagnostics

    def _get_rpc_client_for_service(self, service, *client_args, **client_kwargs):
        rpc_client_class = RPC_TOPIC_TO_CLIENT_CLASS_MAP.get(service['topic'])
        if not rpc_client_class:
            raise exception.NotFound(
                "No RPC client class for service with topic '%s'." % (
                    service.topic))

        topic = service['topic']
        if service['topic'] == constants.WORKER_MAIN_MESSAGING_TOPIC:
            # NOTE: coriolis.service.MessagingService-type services (such
            # as the worker), always have a dedicated per-host queue
            # which can be used to target the service:
            topic = constants.SERVICE_MESSAGING_TOPIC_FORMAT % ({
                "main_topic": constants.WORKER_MAIN_MESSAGING_TOPIC,
                "host": service['host']})

        return rpc_client_class(*client_args, topic=topic, **client_kwargs)

    def _get_worker_rpc_for_host(self, host, *client_args, **client_kwargs):
        rpc_client_class = RPC_TOPIC_TO_CLIENT_CLASS_MAP[
            constants.WORKER_MAIN_MESSAGING_TOPIC]
        topic = constants.SERVICE_MESSAGING_TOPIC_FORMAT % ({
            "main_topic": constants.WORKER_MAIN_MESSAGING_TOPIC,
            "host": host})
        return rpc_client_class(*client_args, topic=topic, **client_kwargs)

    def _get_worker_service_rpc_for_specs(
            self, ctxt, provider_requirements=None, region_sets=None,
            enabled=True, random_choice=False, raise_on_no_matches=True):
        selected_service = self._scheduler_client.get_worker_service_for_specs(
            ctxt, provider_requirements=provider_requirements,
            region_sets=region_sets, enabled=enabled,
            random_choice=random_choice,
            raise_on_no_matches=raise_on_no_matches)
        service = db_api.get_service(ctxt, selected_service["id"])
        return self._get_rpc_client_for_service(service)

    def _check_delete_reservation_for_transfer(self, transfer_action):
        action_id = transfer_action.base_id
        if not self._licensing_client:
            LOG.warn(
                "Licensing client not instantiated. Skipping deletion of "
                "reservation for transfer action '%s'", action_id)
            return

        reservation_id = transfer_action.reservation_id
        if reservation_id:
            try:
                self._licensing_client.delete_reservation(reservation_id)
            except (Exception, KeyboardInterrupt):
                LOG.warn(
                    "Failed to delete reservation with ID '%s' for transfer "
                    "action with ID '%s'. Skipping. Exception\n%s",
                    reservation_id, action_id, utils.get_exception_details())

    def _check_create_reservation_for_transfer(
            self, transfer_action, transfer_type):
        action_id = transfer_action.base_id
        if not self._licensing_client:
            LOG.warn(
                "Licensing client not instantiated. Skipping creation of "
                "reservation for transfer action '%s'", action_id)
            return

        ninstances = len(transfer_action.instances)
        LOG.debug(
            "Attempting to create '%s' reservation for %d instances for "
            "transfer action with ID '%s'.",
            transfer_type, ninstances, action_id)
        reservation = self._licensing_client.add_reservation(
            transfer_type, ninstances)
        transfer_action.reservation_id = reservation['id']

    def _check_reservation_for_transfer(
            self, transfer_action, reservation_type):
        action_id = transfer_action.base_id
        if not self._licensing_client:
            LOG.warn(
                "Licensing client not instantiated. Skipping checking of "
                "reservation for transfer action '%s'", action_id)
            return

        reservation_id = transfer_action.reservation_id
        if reservation_id:
            LOG.debug(
                "Attempting to check reservation with ID '%s' for transfer "
                "action '%s'", reservation_id, action_id)
            try:
                transfer_action.reservation_id = (
                    self._licensing_client.check_refresh_reservation(
                        reservation_id)['id'])
            except Exception as ex:
                exc_code = getattr(ex, 'code', None)
                if exc_code in [404, 409]:
                    if exc_code == 409:
                        LOG.debug(
                            "Server-side exception occurred while trying to "
                            "check the existing reservation '%s' for action "
                            "'%s'. Attempting to create a new reservation. "
                            "Trace was: %s",
                            reservation_id, action_id,
                            utils.get_exception_details())
                    elif exc_code == 404:
                        LOG.debug(
                            "Could not find previous reservation with ID '%s' "
                            "for action '%s'. Attempting to create a new "
                            "reservation. Trace was: %s",
                            reservation_id, action_id,
                            utils.get_exception_details())
                    self._check_create_reservation_for_transfer(
                        transfer_action, reservation_type)
                else:
                    raise ex
        else:
            LOG.debug(
                "Transfer action '%s' has no reservation ID set.", action_id)
            self._check_create_reservation_for_transfer(
                transfer_action, reservation_type)

    def create_endpoint(self, ctxt, name, endpoint_type, description,
                        connection_info, mapped_regions=None):
        endpoint = models.Endpoint()
        endpoint.id = str(uuid.uuid4())
        endpoint.name = name
        endpoint.type = endpoint_type
        endpoint.description = description
        endpoint.connection_info = connection_info

        db_api.add_endpoint(ctxt, endpoint)
        LOG.info("Endpoint created: %s", endpoint.id)

        # add region associations:
        if mapped_regions:
            try:
                db_api.update_endpoint(
                    ctxt, endpoint.id, {
                        "mapped_regions": mapped_regions})
            except Exception as ex:
                LOG.warn(
                    "Error adding region mappings during new endpoint creation "
                    "(name: %s), cleaning up endpoint and all created "
                    "mappings for regions: %s", endpoint.name, mapped_regions)
                db_api.delete_endpoint(ctxt, endpoint.id)
                raise

        return self.get_endpoint(ctxt, endpoint.id)

    @endpoint_synchronized
    def update_endpoint(self, ctxt, endpoint_id, updated_values):
        LOG.info(
            "Attempting to update endpoint '%s' with payload: %s",
            endpoint_id, updated_values)
        db_api.update_endpoint(ctxt, endpoint_id, updated_values)
        LOG.info("Endpoint updated: %s", endpoint_id)
        return db_api.get_endpoint(ctxt, endpoint_id)

    def get_endpoints(self, ctxt):
        return db_api.get_endpoints(ctxt)

    @endpoint_synchronized
    def get_endpoint(self, ctxt, endpoint_id):
        endpoint = db_api.get_endpoint(ctxt, endpoint_id)
        if not endpoint:
            raise exception.NotFound(
                "Endpoint with ID '%s' not found." % endpoint_id)
        return endpoint

    @endpoint_synchronized
    def delete_endpoint(self, ctxt, endpoint_id):
        q_replicas_count = db_api.get_endpoint_replicas_count(
            ctxt, endpoint_id)
        if q_replicas_count is not 0:
            raise exception.NotAuthorized("%s replicas would be orphaned!" %
                                          q_replicas_count)
        db_api.delete_endpoint(ctxt, endpoint_id)

    def get_endpoint_instances(self, ctxt, endpoint_id, source_environment,
                               marker, limit, instance_name_pattern):
        endpoint = self.get_endpoint(ctxt, endpoint_id)

        worker_rpc = self._get_worker_service_rpc_for_specs(
            ctxt, enabled=True,
            region_sets=[[reg.id for reg in endpoint.mapped_regions]],
            provider_requirements={
                endpoint.type: [constants.PROVIDER_TYPE_ENDPOINT_INSTANCES]})
        return worker_rpc.get_endpoint_instances(
            ctxt, endpoint.type, endpoint.connection_info,
            source_environment, marker, limit, instance_name_pattern)

    def get_endpoint_instance(
            self, ctxt, endpoint_id, source_environment, instance_name):
        endpoint = self.get_endpoint(ctxt, endpoint_id)

        worker_rpc = self._get_worker_service_rpc_for_specs(
            ctxt, enabled=True,
            region_sets=[[reg.id for reg in endpoint.mapped_regions]],
            provider_requirements={
                endpoint.type: [constants.PROVIDER_TYPE_ENDPOINT_INSTANCES]})

        return worker_rpc.get_endpoint_instance(
            ctxt, endpoint.type, endpoint.connection_info,
            source_environment, instance_name)

    def get_endpoint_source_options(
            self, ctxt, endpoint_id, env, option_names):
        endpoint = self.get_endpoint(ctxt, endpoint_id)

        worker_rpc = self._get_worker_service_rpc_for_specs(
            ctxt, enabled=True,
            region_sets=[[reg.id for reg in endpoint.mapped_regions]],
            provider_requirements={
                endpoint.type: [
                    constants.PROVIDER_TYPE_SOURCE_ENDPOINT_OPTIONS]})

        return worker_rpc.get_endpoint_source_options(
            ctxt, endpoint.type, endpoint.connection_info, env, option_names)

    def get_endpoint_destination_options(
            self, ctxt, endpoint_id, env, option_names):
        endpoint = self.get_endpoint(ctxt, endpoint_id)

        worker_rpc = self._get_worker_service_rpc_for_specs(
            ctxt, enabled=True,
            region_sets=[[reg.id for reg in endpoint.mapped_regions]],
            provider_requirements={
                endpoint.type: [
                    constants.PROVIDER_TYPE_DESTINATION_ENDPOINT_OPTIONS]})
        return worker_rpc.get_endpoint_destination_options(
            ctxt, endpoint.type, endpoint.connection_info, env, option_names)

    def get_endpoint_networks(self, ctxt, endpoint_id, env):
        endpoint = self.get_endpoint(ctxt, endpoint_id)

        worker_rpc = self._get_worker_service_rpc_for_specs(
            ctxt, enabled=True,
            region_sets=[[reg.id for reg in endpoint.mapped_regions]],
            provider_requirements={
                endpoint.type: [constants.PROVIDER_TYPE_ENDPOINT_NETWORKS]})

        return worker_rpc.get_endpoint_networks(
            ctxt, endpoint.type, endpoint.connection_info, env)

    def get_endpoint_storage(self, ctxt, endpoint_id, env):
        endpoint = self.get_endpoint(ctxt, endpoint_id)

        worker_rpc = self._get_worker_service_rpc_for_specs(
            ctxt, enabled=True,
            region_sets=[[reg.id for reg in endpoint.mapped_regions]],
            provider_requirements={
                endpoint.type: [constants.PROVIDER_TYPE_ENDPOINT_STORAGE]})

        return worker_rpc.get_endpoint_storage(
            ctxt, endpoint.type, endpoint.connection_info, env)

    def validate_endpoint_connection(self, ctxt, endpoint_id):
        endpoint = self.get_endpoint(ctxt, endpoint_id)

        worker_rpc = self._get_worker_service_rpc_for_specs(
            ctxt, enabled=True,
            region_sets=[[reg.id for reg in endpoint.mapped_regions]],
            provider_requirements={
                endpoint.type: [constants.PROVIDER_TYPE_ENDPOINT]})

        return worker_rpc.validate_endpoint_connection(
            ctxt, endpoint.type, endpoint.connection_info)

    def validate_endpoint_target_environment(
            self, ctxt, endpoint_id, target_env):
        endpoint = self.get_endpoint(ctxt, endpoint_id)
        worker_rpc = self._get_worker_service_rpc_for_specs(
            ctxt, enabled=True,
            region_sets=[[reg.id for reg in endpoint.mapped_regions]],
            provider_requirements={
                endpoint.type: [constants.PROVIDER_TYPE_ENDPOINT]})

        return worker_rpc.validate_endpoint_target_environment(
            ctxt, endpoint.type, target_env)

    def validate_endpoint_source_environment(
            self, ctxt, endpoint_id, source_env):
        endpoint = self.get_endpoint(ctxt, endpoint_id)

        worker_rpc = self._get_worker_service_rpc_for_specs(
            ctxt, enabled=True,
            region_sets=[[reg.id for reg in endpoint.mapped_regions]],
            provider_requirements={
                endpoint.type: [constants.PROVIDER_TYPE_ENDPOINT]})

        return worker_rpc.validate_endpoint_source_environment(
            ctxt, endpoint.type, source_env)

    def get_available_providers(self, ctxt):
        worker_rpc = self._get_rpc_client_for_service(
            self._scheduler_client.get_any_worker_service(ctxt))
        return worker_rpc.get_available_providers(ctxt)

    def get_provider_schemas(self, ctxt, platform_name, provider_type):
        # TODO(aznashwan): merge or version/namespace schemas for each worker?
        worker_rpc = self._get_rpc_client_for_service(
            self._scheduler_client.get_any_worker_service(ctxt))
        return worker_rpc.get_provider_schemas(
            ctxt, platform_name, provider_type)

    @staticmethod
    def _create_task(instance, task_type, execution, depends_on=None,
                     on_error=False, on_error_only=False):
        """ Creates a task with the given properties.

        NOTE: for on_error and on_error_only tasks, the parent dependencies who
        are the ones which require cleanup should also be included!
        Ex: for the DELETE_OS_MORPHING_RESOURCES task, include both the
        OSMorphing task, as well as the DEPLOY_OS_MORPHIN_RESOURCES one!
        """
        task = models.Task()
        task.id = str(uuid.uuid4())
        task.instance = instance
        task.execution = execution
        task.task_type = task_type
        task.depends_on = depends_on
        task.on_error = on_error
        task.index = len(task.execution.tasks) + 1

        # non-error tasks are automatically set to scheduled:
        if not on_error and not on_error_only:
            task.status = constants.TASK_STATUS_SCHEDULED
        else:
            task.status = constants.TASK_STATUS_ON_ERROR_ONLY
            # on-error-only tasks remain marked as such
            # regardless of dependencies:
            if on_error_only:
                task.on_error = True
            # plain on-error but depend on already-defined
            # scheduled tasks count as scheduled:
            elif depends_on:
                for task_id in depends_on:
                    if [t for t in task.execution.tasks if t.id == task_id and
                            t.status != constants.TASK_STATUS_ON_ERROR_ONLY]:
                        task.status = constants.TASK_STATUS_SCHEDULED
                        break
            # on_error tasks with no deps are automatically scheduled:
            else:
                task.status = constants.TASK_STATUS_SCHEDULED

        return task

    def _get_task_origin(self, ctxt, action):
        endpoint = self.get_endpoint(ctxt, action.origin_endpoint_id)
        return {
            "connection_info": endpoint.connection_info,
            "type": endpoint.type,
            "source_environment": action.source_environment
        }

    def _get_task_destination(self, ctxt, action):
        endpoint = self.get_endpoint(ctxt, action.destination_endpoint_id)
        return {
            "connection_info": endpoint.connection_info,
            "type": endpoint.type,
            "target_environment": action.destination_environment
        }

    def _get_worker_service_rpc_for_task(
            self, ctxt, task, origin_endpoint, destination_endpoint,
            retry_count=5, retry_period=2, random_choice=True):
        worker_service = None
        try:
            worker_service = self._scheduler_client.get_worker_service_for_task(
                ctxt, {"id": task.id, "task_type": task.task_type},
                origin_endpoint, destination_endpoint,
                retry_count=retry_count, retry_period=retry_period,
                random_choice=random_choice)
        except Exception as ex:
            LOG.debug(
                "Failed to get worker service for task '%s'. Updating status "
                "to unscheduleable. Error trace was: %s",
                task.id, utils.get_exception_details())
            db_api.set_task_status(
                ctxt, task.id, constants.TASK_STATUS_FAILED_TO_SCHEDULE,
                exception_details=str(ex))
            raise

        return self._get_rpc_client_for_service(worker_service)

    def _begin_tasks(
            self, ctxt, action, execution, task_info_override=None,
            scheduling_retry_count=5, scheduling_retry_period=2):
        """ Starts all non-error-only tasks which have no depencies. """
        if not ctxt.trust_id:
            keystone.create_trust(ctxt)
            ctxt.delete_trust_id = True

        task_info = action.info
        if task_info_override is not None:
            task_info = task_info_override

        origin = self._get_task_origin(ctxt, action)
        destination = self._get_task_destination(ctxt, action)
        origin_endpoint = db_api.get_endpoint(
            ctxt, action.origin_endpoint_id)
        destination_endpoint = db_api.get_endpoint(
            ctxt, action.destination_endpoint_id)

        newly_started_tasks = []
        for task in execution.tasks:
            if (not task.depends_on and
                    task.status == constants.TASK_STATUS_SCHEDULED):
                LOG.info(
                    "Starting dependency-less task '%s' for execution '%s'",
                    task.id, execution.id)
                db_api.set_task_status(
                    ctxt, task.id, constants.TASK_STATUS_PENDING)
                try:
                    worker_rpc = self._get_worker_service_rpc_for_task(
                        ctxt, task, origin_endpoint, destination_endpoint,
                        retry_count=scheduling_retry_count,
                        retry_period=scheduling_retry_period)
                    worker_rpc.begin_task(
                        ctxt,
                        task_id=task.id,
                        task_type=task.task_type,
                        origin=origin,
                        destination=destination,
                        instance=task.instance,
                        task_info=task_info.get(task.instance, {}))
                except Exception as ex:
                    LOG.warn(
                        "Error occured while starting new task '%s'. "
                        "Cancelling execution '%s'. Error was: %s",
                        task.id, execution.id, utils.get_exception_details())
                    self._cancel_tasks_execution(
                        ctxt, execution, requery=True)
                    raise
                newly_started_tasks.append(task.id)

        if newly_started_tasks:
            LOG.info(
                "Started the following tasks for Execution '%s': %s",
                execution.id, newly_started_tasks)
            self._set_tasks_execution_status(
                ctxt, execution.id, constants.TASK_STATUS_RUNNING)
        else:
            # NOTE: this should never happen if _check_execution_tasks_sanity
            # was called before this method:
            raise exception.InvalidActionTasksExecutionState(
                "No tasks were started at the beginning of execution '%s'" % (
                    execution.id))

    def _check_execution_tasks_sanity(
            self, execution, initial_task_info):
        """ Checks whether the given execution's tasks are:
        - properly odered and not set to deadlock off the bat
        - properly manipulate the task_info in the right order
        """
        all_instances_in_tasks = {
            t.instance for t in execution.tasks}
        instances_tasks_mapping = {
            instance: [
                t for t in execution.tasks if t.instance == instance]
            for instance in all_instances_in_tasks}

        def _check_task_cls_param_requirements(task, instance_task_info_keys):
            task_cls = tasks_factory.get_task_runner_class(task.task_type)
            missing_params = [
                p for p in task_cls.get_required_task_info_properties()
                if p not in instance_task_info_keys]
            if missing_params:
                raise exception.CoriolisException(
                    "The following task parameters for instance '%s' "
                    "are missing from the task_info for task '%s' of "
                    "type '%s': %s" % (
                        task.instance, task.id, task.task_type,
                        missing_params))
            return task_cls.get_returned_task_info_properties()

        for instance, instance_tasks in instances_tasks_mapping.items():
            task_info_keys = set(initial_task_info.get(
                instance, {}).keys())
            # mapping between the ID and associated object of processed tasks:
            processed_tasks = {}
            tasks_to_process = {
                t.id: t for t in instance_tasks}
            while tasks_to_process:
                queued_tasks = []
                # gather all tasks which will be queued to run in parallel:
                for task in tasks_to_process.values():
                    if task.status in (
                            constants.TASK_STATUS_SCHEDULED,
                            constants.TASK_STATUS_ON_ERROR_ONLY):
                        if not task.depends_on:
                            queued_tasks.append(task)
                        else:
                            missing_deps = [
                                dep_id
                                for dep_id in task.depends_on
                                if dep_id not in tasks_to_process and (
                                    dep_id not in processed_tasks)]
                            if missing_deps:
                                raise exception.CoriolisException(
                                    "Task '%s' (type '%s') for instance '%s' "
                                    "has non-existent tasks referenced as "
                                    "dependencies: %s" % (
                                        task.id, task.task_type,
                                        instance, missing_deps))
                            if all(
                                    [dep_id in processed_tasks
                                     for dep_id in task.depends_on]):
                                queued_tasks.append(task)
                    else:
                        raise exception.CoriolisException(
                            "Invalid initial state '%s' for task '%s' "
                            "of type '%s'."% (
                                task.status, task.id, task.task_type))

                # check if nothing was left queued:
                if not queued_tasks:
                    remaining_tasks_deps_map = {
                        (tid, t.task_type): t.depends_on
                        for tid, t in tasks_to_process.items()}
                    processed_tasks_type_map = {
                        tid: t.task_type
                        for tid, t in processed_tasks.items()}
                    raise exception.CoriolisException(
                        "Execution '%s' (type '%s') is bound to be deadlocked:"
                        " there are leftover tasks for instance '%s' which "
                        "will never get queued. Already processed tasks are: "
                        "%s. Tasks left: %s" % (
                            execution.id, execution.type, instance,
                            processed_tasks_type_map, remaining_tasks_deps_map))

                # mapping for task_info fields modified by each task:
                modified_fields_by_queued_tasks = {}
                # check that each task has what it needs and
                # register what they return/modify:
                for task in queued_tasks:
                    for new_field in _check_task_cls_param_requirements(
                            task, task_info_keys):
                        if new_field not in modified_fields_by_queued_tasks:
                            modified_fields_by_queued_tasks[new_field] = [task]
                        else:
                            modified_fields_by_queued_tasks[new_field].append(
                                task)

                # check if any queued tasks would manipulate the same fields:
                conflicting_fields = {
                    new_field: [t.task_type for t in tasks]
                    for new_field, tasks in (
                        modified_fields_by_queued_tasks.items())
                    if len(tasks) > 1}
                if conflicting_fields:
                    raise exception.CoriolisException(
                        "There are fields which will encounter a state "
                        "conflict following the parallelized execution of "
                        "tasks for execution '%s' (type '%s') for instance "
                        "'%s'. Conflicting fields and tasks will be: : %s" % (
                            execution.id, execution.type, instance,
                            conflicting_fields))

                # register queued tasks as processed before continuing:
                for task in queued_tasks:
                    processed_tasks[task.id] = task
                    tasks_to_process.pop(task.id)
                # update current state fields at this point:
                task_info_keys = task_info_keys.union(set(
                    modified_fields_by_queued_tasks.keys()))
                LOG.debug(
                    "Successfully processed following tasks for instance '%s' "
                    "for execution %s (type '%s') for any state conflict "
                    "checks: %s",
                    instance, execution.id, execution.type, [
                        (t.id, t.task_type) for t in queued_tasks])
            LOG.debug(
                "Successfully checked all tasks for instance '%s' as part of "
                "execution '%s' (type '%s') for any state conflicts: %s",
                instance, execution.id, execution.type,
                [(t.id, t.task_type) for t in instance_tasks])
        LOG.debug(
            "Successfully checked all tasks for execution '%s' (type '%s') "
            "for ordering or state conflicts.",
            execution.id, execution.type)

    @replica_synchronized
    def execute_replica_tasks(self, ctxt, replica_id, shutdown_instances):
        replica = self._get_replica(ctxt, replica_id)
        self._check_reservation_for_transfer(
            replica, licensing_client.RESERVATION_TYPE_REPLICA)
        self._check_replica_running_executions(ctxt, replica)
        self._check_minion_pools_for_action(ctxt, replica)

        execution = models.TasksExecution()
        execution.id = str(uuid.uuid4())
        execution.action = replica
        execution.status = constants.EXECUTION_STATUS_UNEXECUTED
        execution.type = constants.EXECUTION_TYPE_REPLICA_EXECUTION

        # TODO(aznashwan): have these passed separately to the relevant
        # provider methods. They're currently passed directly inside
        # dest-env by the API service when accepting the call, but we
        # re-overwrite them here in case of Replica updates.
        dest_env = copy.deepcopy(replica.destination_environment)
        dest_env['network_map'] = replica.network_map
        dest_env['storage_mappings'] = replica.storage_mappings

        for instance in execution.action.instances:
            # NOTE: we default/convert the volumes info to an empty list
            # to preserve backwards-compatibility with older versions
            # of Coriolis dating before the scheduling overhaul (PR##114)
            if instance not in replica.info:
                replica.info[instance] = {'volumes_info': []}
            elif replica.info[instance].get('volumes_info') is None:
                 replica.info[instance]['volumes_info'] = []
            # NOTE: we update all of the param values before triggering an
            # execution to ensure that the latest parameters are used:
            replica.info[instance].update({
                "source_environment": replica.source_environment,
                "target_environment": dest_env})
                # TODO(aznashwan): have these passed separately to the relevant
                # provider methods (they're currently passed directly inside
                # dest-env by the API service when accepting the call)
                # "network_map": network_map,
                # "storage_mappings": storage_mappings,

            validate_replica_source_inputs_task = self._create_task(
                instance,
                constants.TASK_TYPE_VALIDATE_REPLICA_SOURCE_INPUTS,
                execution)

            get_instance_info_task = self._create_task(
                instance,
                constants.TASK_TYPE_GET_INSTANCE_INFO,
                execution)

            validate_replica_destination_inputs_task = self._create_task(
                instance,
                constants.TASK_TYPE_VALIDATE_REPLICA_DESTINATION_INPUTS,
                execution,
                depends_on=[get_instance_info_task.id])

            disk_deployment_depends_on = []
            validate_source_minion_task = None
            if replica.origin_minion_pool_id:
                # NOTE: these values are required for the
                # _check_execution_tasks_sanity call but
                # will be populated later when the pool
                # allocations actually happen:
                replica.info[instance].update({
                    "source_minion_machine_id": None,
                    "source_minion_provider_properties": None,
                    "source_minion_connection_info": None})
                validate_source_minion_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_VALIDATE_SOURCE_MINION_POOL_COMPATIBILITY,
                    execution,
                    depends_on=[
                        get_instance_info_task.id,
                        validate_replica_source_inputs_task.id])
                disk_deployment_depends_on.append(
                    validate_source_minion_task.id)
            else:
                disk_deployment_depends_on.append(
                    validate_replica_source_inputs_task.id)

            validate_target_minion_task = None
            if replica.destination_minion_pool_id:
                # NOTE: these values are required for the
                # _check_execution_tasks_sanity call but
                # will be populated later when the pool
                # allocations actually happen:
                replica.info[instance].update({
                    "target_minion_machine_id": None,
                    "target_minion_provider_properties": None,
                    "target_minion_connection_info": None,
                    "target_minion_backup_writer_connection_info": None})
                validate_target_minion_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_VALIDATE_DESTINATION_MINION_POOL_COMPATIBILITY,
                    execution,
                    depends_on=[
                        validate_replica_destination_inputs_task.id])
                disk_deployment_depends_on.append(
                    validate_target_minion_task.id)
            else:
                disk_deployment_depends_on.append(
                    validate_replica_destination_inputs_task.id)

            deploy_replica_disks_task = self._create_task(
                instance, constants.TASK_TYPE_DEPLOY_REPLICA_DISKS,
                execution, depends_on=disk_deployment_depends_on)

            shutdown_deps = []
            deploy_replica_source_resources_task = None
            if not replica.origin_minion_pool_id:
                deploy_replica_source_resources_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_DEPLOY_REPLICA_SOURCE_RESOURCES,
                    execution, depends_on=[
                        deploy_replica_disks_task.id])
                shutdown_deps.append(deploy_replica_source_resources_task)

            attach_target_minion_disks_task = None
            deploy_replica_target_resources_task = None
            if replica.destination_minion_pool_id:
                ttyp = constants.TASK_TYPE_ATTACH_VOLUMES_TO_DESTINATION_MINION
                attach_target_minion_disks_task = self._create_task(
                    instance, ttyp, execution, depends_on=[
                        deploy_replica_disks_task.id])
                shutdown_deps.append(attach_target_minion_disks_task)
            else:
                deploy_replica_target_resources_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_DEPLOY_REPLICA_TARGET_RESOURCES,
                    execution, depends_on=[
                        deploy_replica_disks_task.id])
                shutdown_deps.append(deploy_replica_target_resources_task)

            depends_on = [t.id for t in shutdown_deps]
            if shutdown_instances:
                shutdown_instance_task = self._create_task(
                    instance, constants.TASK_TYPE_SHUTDOWN_INSTANCE,
                    execution, depends_on=depends_on)
                depends_on = [shutdown_instance_task.id]

            replicate_disks_task = self._create_task(
                instance, constants.TASK_TYPE_REPLICATE_DISKS,
                execution, depends_on=depends_on)

            if replica.origin_minion_pool_id:
                self._create_task(
                    instance,
                    constants.TASK_TYPE_RELEASE_SOURCE_MINION,
                    execution,
                    depends_on=[
                        validate_source_minion_task.id,
                        replicate_disks_task.id],
                    on_error=True)
            else:
                self._create_task(
                    instance,
                    constants.TASK_TYPE_DELETE_REPLICA_SOURCE_RESOURCES,
                    execution,
                    depends_on=[
                        deploy_replica_source_resources_task.id,
                        replicate_disks_task.id],
                    on_error=True)

            if replica.destination_minion_pool_id:
                detach_volumes_from_minion_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_DETACH_VOLUMES_FROM_DESTINATION_MINION,
                    execution,
                    depends_on=[
                        attach_target_minion_disks_task.id,
                        replicate_disks_task.id],
                    on_error=True)

                self._create_task(
                    instance,
                    constants.TASK_TYPE_RELEASE_DESTINATION_MINION,
                    execution,
                    depends_on=[
                        validate_target_minion_task.id,
                        detach_volumes_from_minion_task.id],
                    on_error=True)
            else:
                self._create_task(
                    instance,
                    constants.TASK_TYPE_DELETE_REPLICA_TARGET_RESOURCES,
                    execution, depends_on=[
                        deploy_replica_target_resources_task.id,
                        replicate_disks_task.id],
                    on_error=True)

        self._check_execution_tasks_sanity(execution, replica.info)

        # update the action info for all of the Replicas:
        for instance in execution.action.instances:
            db_api.update_transfer_action_info_for_instance(
                ctxt, replica.id, instance, replica.info[instance])

        # add new execution to DB:
        db_api.add_replica_tasks_execution(ctxt, execution)
        LOG.info("Replica tasks execution added to DB: %s", execution.id)

        uses_minion_pools = any([
            replica.origin_minion_pool_id,
            replica.destination_minion_pool_id])
        if uses_minion_pools:
            self._minion_manager_client.allocate_minion_machines_for_replica(
                ctxt, replica)
            self._set_tasks_execution_status(
                ctxt, execution.id,
                constants.EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS)
        else:
            self._begin_tasks(ctxt, replica, execution)

        return self.get_replica_tasks_execution(ctxt, replica_id, execution.id)

    @replica_synchronized
    def get_replica_tasks_executions(self, ctxt, replica_id,
                                     include_tasks=False):
        return db_api.get_replica_tasks_executions(
            ctxt, replica_id, include_tasks)

    @tasks_execution_synchronized
    def get_replica_tasks_execution(self, ctxt, replica_id, execution_id):
        return self._get_replica_tasks_execution(
            ctxt, replica_id, execution_id)

    @tasks_execution_synchronized
    def delete_replica_tasks_execution(self, ctxt, replica_id, execution_id):
        execution = self._get_replica_tasks_execution(
            ctxt, replica_id, execution_id)
        if execution.status in constants.ACTIVE_EXECUTION_STATUSES:
            raise exception.InvalidMigrationState(
                "Cannot delete execution '%s' for Replica '%s' as it is "
                "currently in '%s' state." % (
                    execution_id, replica_id, execution.status))
        db_api.delete_replica_tasks_execution(ctxt, execution_id)

    @tasks_execution_synchronized
    def cancel_replica_tasks_execution(self, ctxt, replica_id, execution_id,
                                       force):
        execution = self._get_replica_tasks_execution(
            ctxt, replica_id, execution_id)
        if execution.status not in constants.ACTIVE_EXECUTION_STATUSES:
            raise exception.InvalidReplicaState(
                "Replica '%s' has no running execution to cancel." % (
                    replica_id))
        if execution.status == constants.EXECUTION_STATUS_CANCELLING and (
                not force):
            raise exception.InvalidReplicaState(
                "Replica '%s' is already being cancelled. Please use the "
                "force option if you'd like to force-cancel it." % (
                    replica_id))
        self._cancel_tasks_execution(ctxt, execution, force=force)

    def _get_replica_tasks_execution(self, ctxt, replica_id, execution_id):
        execution = db_api.get_replica_tasks_execution(
            ctxt, replica_id, execution_id)
        if not execution:
            raise exception.NotFound(
                "Execution with ID '%s' for Replica '%s' not found." % (
                    execution_id, replica_id))
        return execution

    def get_replicas(self, ctxt, include_tasks_executions=False):
        return db_api.get_replicas(ctxt, include_tasks_executions)

    @replica_synchronized
    def get_replica(self, ctxt, replica_id):
        return self._get_replica(ctxt, replica_id)

    @replica_synchronized
    def delete_replica(self, ctxt, replica_id):
        replica = self._get_replica(ctxt, replica_id)
        self._check_replica_running_executions(ctxt, replica)
        self._check_delete_reservation_for_transfer(replica)
        db_api.delete_replica(ctxt, replica_id)

    @replica_synchronized
    def delete_replica_disks(self, ctxt, replica_id):
        replica = self._get_replica(ctxt, replica_id)
        self._check_replica_running_executions(ctxt, replica)

        execution = models.TasksExecution()
        execution.id = str(uuid.uuid4())
        execution.status = constants.EXECUTION_STATUS_UNEXECUTED
        execution.action = replica
        execution.type = constants.EXECUTION_TYPE_REPLICA_DISKS_DELETE

        has_tasks = False
        for instance in replica.instances:
            if (instance in replica.info and
                    replica.info[instance].get('volumes_info')):
                source_del_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_DELETE_REPLICA_SOURCE_DISK_SNAPSHOTS,
                    execution)
                self._create_task(
                    instance, constants.TASK_TYPE_DELETE_REPLICA_DISKS,
                    execution, depends_on=[source_del_task.id])
                has_tasks = True

        if not has_tasks:
            raise exception.InvalidReplicaState(
                "Replica '%s' does not have volumes information for any "
                "instances. Ensure that the replica has been executed "
                "successfully priorly" % replica_id)

        # ensure we're passing the updated target-env options on the
        # parent Replica itself in case of a Replica update:
        dest_env = copy.deepcopy(replica.destination_environment)
        dest_env['network_map'] = replica.network_map
        dest_env['storage_mappings'] = replica.storage_mappings
        for instance in replica.instances:
            replica.info[instance].update({
                "target_environment": dest_env})

        self._check_execution_tasks_sanity(execution, replica.info)

        # update the action info for all of the Replicas' instances:
        for instance in replica.instances:
            db_api.update_transfer_action_info_for_instance(
                ctxt, replica.id, instance, replica.info[instance])
        db_api.add_replica_tasks_execution(ctxt, execution)
        LOG.info("Replica tasks execution created: %s", execution.id)

        self._begin_tasks(ctxt, replica, execution)
        return self.get_replica_tasks_execution(ctxt, replica_id, execution.id)

    @staticmethod
    def _check_endpoints(ctxt, origin_endpoint, destination_endpoint):
        if origin_endpoint.id == destination_endpoint.id:
            raise exception.SameDestination(
                "The origin and destination endpoints cannot be the same. "
                "If you need to perform operations across two areas of "
                "the same platform (ex: migrating across public cloud regions)"
                ", please create two separate endpoints.")
        # TODO(alexpilotti): check Barbican secrets content as well
        if (origin_endpoint.connection_info ==
                destination_endpoint.connection_info):
            raise exception.SameDestination()

    def create_instances_replica(self, ctxt, origin_endpoint_id,
                                 destination_endpoint_id,
                                 origin_minion_pool_id,
                                 destination_minion_pool_id,
                                 instance_osmorphing_minion_pool_mappings,
                                 source_environment,
                                 destination_environment, instances,
                                 network_map, storage_mappings, notes=None,
                                 user_scripts=None):
        origin_endpoint = self.get_endpoint(ctxt, origin_endpoint_id)
        destination_endpoint = self.get_endpoint(ctxt, destination_endpoint_id)
        self._check_endpoints(ctxt, origin_endpoint, destination_endpoint)

        replica = models.Replica()
        replica.id = str(uuid.uuid4())
        replica.base_id = replica.id
        replica.origin_endpoint_id = origin_endpoint_id
        replica.origin_minion_pool_id = origin_minion_pool_id
        replica.destination_endpoint_id = destination_endpoint_id
        replica.destination_minion_pool_id = destination_minion_pool_id
        replica.destination_environment = destination_environment
        replica.source_environment = source_environment
        replica.last_execution_status = constants.EXECUTION_STATUS_UNEXECUTED
        replica.instances = instances
        replica.executions = []
        replica.info = {instance: {
            'volumes_info': []} for instance in instances}
        replica.notes = notes
        replica.network_map = network_map
        replica.storage_mappings = storage_mappings
        replica.instance_osmorphing_minion_pool_mappings = (
            instance_osmorphing_minion_pool_mappings)
        replica.user_scripts = user_scripts

        self._check_minion_pools_for_action(ctxt, replica)

        self._check_create_reservation_for_transfer(
            replica, licensing_client.RESERVATION_TYPE_REPLICA)

        db_api.add_replica(ctxt, replica)
        LOG.info("Replica created: %s", replica.id)
        return self.get_replica(ctxt, replica.id)

    def _get_replica(self, ctxt, replica_id):
        replica = db_api.get_replica(ctxt, replica_id)
        if not replica:
            raise exception.NotFound(
                "Replica with ID '%s' not found." % replica_id)
        return replica

    def get_migrations(self, ctxt, include_tasks,
                       include_info=False):
        return db_api.get_migrations(
            ctxt, include_tasks,
            include_info=include_info)

    @migration_synchronized
    def get_migration(self, ctxt, migration_id):
        # the default serialization mechanism enforces a max_depth of 3
        return utils.to_dict(self._get_migration(ctxt, migration_id))

    @staticmethod
    def _check_running_replica_migrations(ctxt, replica_id):
        migrations = db_api.get_replica_migrations(ctxt, replica_id)
        if [m.id for m in migrations if m.executions[0].status in (
                constants.ACTIVE_EXECUTION_STATUSES)]:
            raise exception.InvalidReplicaState(
                "Replica '%s' is currently being migrated" % replica_id)

    @staticmethod
    def _check_running_executions(action):
        running_executions = [
            e.id for e in action.executions
            if e.status in constants.ACTIVE_EXECUTION_STATUSES]
        if running_executions:
            raise exception.InvalidActionTasksExecutionState(
                "Another tasks execution is in progress: %s" % (
                    running_executions))

    def _check_replica_running_executions(self, ctxt, replica):
        self._check_running_executions(replica)
        self._check_running_replica_migrations(ctxt, replica.id)

    @staticmethod
    def _check_valid_replica_tasks_execution(replica, force=False):
        sorted_executions = sorted(
            replica.executions, key=lambda e: e.number, reverse=True)
        if not sorted_executions:
            raise exception.InvalidReplicaState(
                "The Replica has never been executed.")

        if not [e for e in sorted_executions
                if e.type == constants.EXECUTION_TYPE_REPLICA_EXECUTION and (
                    e.status == constants.EXECUTION_STATUS_COMPLETED)]:
            if not force:
                raise exception.InvalidReplicaState(
                    "A replica must have been executed successfully at least "
                    "once in order to be migrated")

    def _get_provider_types(self, ctxt, endpoint):
        provider_types = self.get_available_providers(ctxt).get(endpoint.type)
        if provider_types is None:
            raise exception.NotFound(
                "No provider found for: %s" % endpoint.type)
        return provider_types["types"]

    @replica_synchronized
    def deploy_replica_instances(self, ctxt, replica_id,
                                 clone_disks, force,
                                 instance_osmorphing_minion_pool_mappings=None,
                                 skip_os_morphing=False,
                                 user_scripts=None):
        replica = self._get_replica(ctxt, replica_id)
        self._check_reservation_for_transfer(
            replica, licensing_client.RESERVATION_TYPE_REPLICA)
        self._check_replica_running_executions(ctxt, replica)
        self._check_valid_replica_tasks_execution(replica, force)
        user_scripts = user_scripts or replica.user_scripts

        destination_endpoint = self.get_endpoint(
            ctxt, replica.destination_endpoint_id)
        destination_provider_types = self._get_provider_types(
            ctxt, destination_endpoint)

        for instance, info in replica.info.items():
            if not info.get("volumes_info"):
                raise exception.InvalidReplicaState(
                    "The replica doesn't contain volumes information for "
                    "instance: %s. If replicated disks are deleted, the "
                    "replica needs to be executed anew before a migration can "
                    "occur" % instance)

        instances = replica.instances

        migration = models.Migration()
        migration.id = str(uuid.uuid4())
        migration.base_id = migration.id
        migration.origin_endpoint_id = replica.origin_endpoint_id
        migration.destination_endpoint_id = replica.destination_endpoint_id
        # TODO(aznashwan): have these passed separately to the relevant
        # provider methods instead of through the dest-env:
        dest_env = copy.deepcopy(replica.destination_environment)
        dest_env['network_map'] = replica.network_map
        dest_env['storage_mappings'] = replica.storage_mappings
        migration.destination_environment = dest_env
        migration.source_environment = replica.source_environment
        migration.network_map = replica.network_map
        migration.storage_mappings = replica.storage_mappings
        migration.instances = instances
        migration.replica = replica
        migration.info = replica.info
        migration.user_scripts = user_scripts
        migration.origin_minion_pool_id = replica.origin_minion_pool_id
        migration.destination_minion_pool_id = (
            replica.destination_minion_pool_id)
        migration.instance_osmorphing_minion_pool_mappings = (
            replica.instance_osmorphing_minion_pool_mappings)
        if instance_osmorphing_minion_pool_mappings:
            migration.instance_osmorphing_minion_pool_mappings.update(
                instance_osmorphing_minion_pool_mappings)

        execution = models.TasksExecution()
        migration.executions = [execution]
        execution.status = constants.EXECUTION_STATUS_UNEXECUTED
        execution.number = 1
        execution.type = constants.EXECUTION_TYPE_REPLICA_DEPLOY

        for instance in instances:
            migration.info[instance]["clone_disks"] = clone_disks
            scripts = self._get_instance_scripts(user_scripts, instance)
            migration.info[instance]["user_scripts"] = scripts

            # NOTE: we default/convert the volumes info to an empty list
            # to preserve backwards-compatibility with older versions
            # of Coriolis dating before the scheduling overhaul (PR##114)
            if instance not in migration.info:
                migration.info[instance] = {'volumes_info': []}
            # NOTE: we update all of the param values before triggering an
            # execution to ensure that the params on the Replica are used
            # in case there was a failed Replica update (where the new values
            # could be in the `.info` field instead of the old ones)
            migration.info[instance].update({
                "source_environment": migration.source_environment,
                "target_environment": dest_env})
                # TODO(aznashwan): have these passed separately to the relevant
                # provider methods (they're currently passed directly inside
                # dest-env by the API service when accepting the call)
                # "network_map": network_map,
                # "storage_mappings": storage_mappings,

            validate_replica_deployment_inputs_task = self._create_task(
                instance,
                constants.TASK_TYPE_VALIDATE_REPLICA_DEPLOYMENT_INPUTS,
                execution)

            validate_osmorphing_minion_task = None
            last_validation_task = validate_replica_deployment_inputs_task
            if not skip_os_morphing and instance in (
                    migration.instance_osmorphing_minion_pool_mappings):
                # NOTE: these values are required for the
                # _check_execution_tasks_sanity call but
                # will be populated later when the pool
                # allocations actually happen:
                migration.info[instance].update({
                    "osmorphing_minion_machine_id": None,
                    "osmorphing_minion_provider_properties": None,
                    "osmorphing_minion_connection_info": None})
                validate_osmorphing_minion_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_VALIDATE_OSMORPHING_MINION_POOL_COMPATIBILITY,
                    execution, depends_on=[
                        validate_replica_deployment_inputs_task.id])
                last_validation_task = validate_osmorphing_minion_task

            create_snapshot_task = self._create_task(
                instance, constants.TASK_TYPE_CREATE_REPLICA_DISK_SNAPSHOTS,
                execution, depends_on=[
                    last_validation_task.id])

            deploy_replica_task = self._create_task(
                instance,
                constants.TASK_TYPE_DEPLOY_REPLICA_INSTANCE_RESOURCES,
                execution,
                depends_on=[create_snapshot_task.id])

            depends_on = [deploy_replica_task.id]
            if not skip_os_morphing:
                task_deploy_os_morphing_resources = None
                attach_osmorphing_minion_volumes_task = None
                last_osmorphing_resources_deployment_task = None
                if instance in (
                        migration.instance_osmorphing_minion_pool_mappings):
                    osmorphing_vol_attachment_deps = [
                        validate_osmorphing_minion_task.id]
                    osmorphing_vol_attachment_deps.extend(depends_on)
                    attach_osmorphing_minion_volumes_task = self._create_task(
                        instance,
                        constants.TASK_TYPE_ATTACH_VOLUMES_TO_OSMORPHING_MINION,
                        execution, depends_on=osmorphing_vol_attachment_deps)
                    last_osmorphing_resources_deployment_task = (
                        attach_osmorphing_minion_volumes_task)

                    collect_osmorphing_info_task = self._create_task(
                        instance,
                        constants.TASK_TYPE_COLLECT_OSMORPHING_INFO,
                        execution,
                        depends_on=[attach_osmorphing_minion_volumes_task.id])
                    last_osmorphing_resources_deployment_task = (
                        collect_osmorphing_info_task)
                else:
                    task_deploy_os_morphing_resources = self._create_task(
                        instance,
                        constants.TASK_TYPE_DEPLOY_OS_MORPHING_RESOURCES,
                        execution, depends_on=depends_on)
                    last_osmorphing_resources_deployment_task = (
                        task_deploy_os_morphing_resources)

                task_osmorphing = self._create_task(
                    instance, constants.TASK_TYPE_OS_MORPHING,
                    execution, depends_on=[
                        last_osmorphing_resources_deployment_task.id])

                depends_on = [task_osmorphing.id]

                if instance in (
                        migration.instance_osmorphing_minion_pool_mappings):
                    detach_osmorphing_minion_volumes_task = self._create_task(
                        instance,
                        constants.TASK_TYPE_DETACH_VOLUMES_FROM_OSMORPHING_MINION,
                        execution, depends_on=[
                            attach_osmorphing_minion_volumes_task.id,
                            task_osmorphing.id],
                        on_error=True)

                    release_osmorphing_minion_task = self._create_task(
                        instance,
                        constants.TASK_TYPE_RELEASE_OSMORPHING_MINION,
                        execution, depends_on=[
                            validate_osmorphing_minion_task.id,
                            detach_osmorphing_minion_volumes_task.id],
                        on_error=True)
                    depends_on.append(release_osmorphing_minion_task.id)
                else:
                    task_delete_os_morphing_resources = self._create_task(
                        instance, constants.TASK_TYPE_DELETE_OS_MORPHING_RESOURCES,
                        execution, depends_on=[
                            task_deploy_os_morphing_resources.id,
                            task_osmorphing.id],
                        on_error=True)
                    depends_on.append(task_delete_os_morphing_resources.id)

            if (constants.PROVIDER_TYPE_INSTANCE_FLAVOR in
                    destination_provider_types):
                get_optimal_flavor_task = self._create_task(
                    instance, constants.TASK_TYPE_GET_OPTIMAL_FLAVOR,
                    execution, depends_on=depends_on)
                depends_on = [get_optimal_flavor_task.id]

            finalize_deployment_task = self._create_task(
                instance,
                constants.TASK_TYPE_FINALIZE_REPLICA_INSTANCE_DEPLOYMENT,
                execution,
                depends_on=depends_on)

            self._create_task(
                instance,
                constants.TASK_TYPE_DELETE_REPLICA_TARGET_DISK_SNAPSHOTS,
                execution, depends_on=[
                    create_snapshot_task.id,
                    finalize_deployment_task.id],
                on_error=clone_disks)

            cleanup_deployment_task = self._create_task(
                instance,
                constants.TASK_TYPE_CLEANUP_FAILED_REPLICA_INSTANCE_DEPLOYMENT,
                execution,
                depends_on=[
                    deploy_replica_task.id,
                    finalize_deployment_task.id],
                on_error_only=True)

            if not clone_disks:
                self._create_task(
                    instance,
                    constants.TASK_TYPE_RESTORE_REPLICA_DISK_SNAPSHOTS,
                    execution,
                    depends_on=[cleanup_deployment_task.id],
                    on_error=True)

        self._check_execution_tasks_sanity(execution, migration.info)
        db_api.add_migration(ctxt, migration)
        LOG.info("Migration created: %s", migration.id)

        if not skip_os_morphing and (
                migration.instance_osmorphing_minion_pool_mappings):
            # NOTE: we lock on the migration ID to ensure the minion
            # allocation confirmations don't come in too early:
            with lockutils.lock(
                    constants.MIGRATION_LOCK_NAME_FORMAT % migration.id,
                    external=True):
                self._minion_manager_client.allocate_minion_machines_for_migration(
                    ctxt, migration, include_transfer_minions=False,
                    include_osmorphing_minions=True)
                self._set_tasks_execution_status(
                    ctxt, execution.id,
                    constants.EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS)
        else:
            self._begin_tasks(ctxt, migration, execution)

        return self.get_migration(ctxt, migration.id)

    def _get_instance_scripts(self, user_scripts, instance):
        user_scripts = user_scripts or {}
        ret = {
            "global": user_scripts.get("global", {}),
            "instances": {},
        }
        if user_scripts:
            instance_script = user_scripts.get(
                "instances", {}).get(instance)
            if instance_script:
                ret["instances"][instance] = instance_script
        return ret

    def _deallocate_minion_machines_for_action(self, ctxt, action):
        return self._minion_manager_client.deallocate_minion_machines_for_action(
            ctxt, action.base_id)

    def _check_minion_pools_for_action(self, ctxt, action):
        return self._minion_manager_client.validate_minion_pool_selections_for_action(
            ctxt, action)

    def _update_task_info_for_minion_allocations(
            self, ctxt, action, minion_machine_allocations):

        for instance in action.instances:
            instance_minion_machines = minion_machine_allocations.get(
                instance, {})
            instance_source_minion = instance_minion_machines.get(
                'source_minion')
            if instance_source_minion:
                action.info[instance].update({
                    "source_minion_machine_id": instance_source_minion['id'],
                    "source_minion_provider_properties": (
                        instance_source_minion['provider_properties']),
                    "source_minion_connection_info": (
                        instance_source_minion['connection_info'])})

            instance_target_minion = instance_minion_machines.get(
                'target_minion')
            if instance_target_minion:
                action.info[instance].update({
                    "target_minion_machine_id": instance_target_minion['id'],
                    "target_minion_provider_properties": (
                        instance_target_minion['provider_properties']),
                    "target_minion_connection_info": (
                        instance_target_minion['connection_info']),
                    "target_minion_backup_writer_connection_info": (
                        instance_target_minion['backup_writer_connection_info'])})

            instance_osmorphing_minion = instance_minion_machines.get(
                'osmorphing_minion')
            if instance_osmorphing_minion:
                action.info[instance].update({
                    "osmorphing_minion_machine_id": instance_osmorphing_minion['id'],
                    "osmorphing_minion_provider_properties": (
                        instance_osmorphing_minion['provider_properties']),
                    "osmorphing_minion_connection_info": (
                        instance_osmorphing_minion['connection_info'])})

        # update the action info for all of the instances:
        for instance in minion_machine_allocations:
            if instance not in action.instances:
                LOG.warn(
                    "Got minion pool allocations for machine(s) which are not "
                    "part of action '%s'. Ignoring: %s", action.id,
                    minion_machine_allocations[instance])
                continue

            db_api.update_transfer_action_info_for_instance(
                ctxt, action.id, instance, action.info[instance])

    def _get_last_execution_for_replica(self, ctxt, replica, requery=False):
        if requery:
            replica = self._get_replica(ctxt, replica.id)
        last_replica_execution = None
        if not replica.executions:
            raise exception.InvalidReplicaState(
                "Replica with ID '%s' has no existing Replica "
                "executions." % (replica.id))
        last_replica_execution = sorted(
            replica.executions, key=lambda e: e.number)[-1]
        return last_replica_execution

    def _get_execution_for_migration(self, ctxt, migration, requery=False):
        if requery:
            migration = self._get_migration(ctxt, migration.id)

        if not migration.executions:
            raise exception.InvalidMigrationState(
                "Migration with ID '%s' has no existing executions." % (
                    migration.id))
        if len(migration.executions) > 1:
            raise exception.InvalidMigrationState(
                "Migration with ID '%s' has more than one execution:"
                " %s" % (migration.id, [e.id for e in migration.executions]))
        return migration.executions[0]

    @replica_synchronized
    def confirm_replica_minions_allocation(
            self, ctxt, replica_id, minion_machine_allocations):
        replica = self._get_replica(ctxt, replica_id)

        awaiting_minions_status = (
            constants.EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS)
        if replica.last_execution_status != awaiting_minions_status:
            raise exception.InvalidReplicaState(
                "Replica is in '%s' status instead of the expected '%s' to "
                "have minion machines allocated for it." % (
                    replica.last_execution_status, awaiting_minions_status))

        last_replica_execution = self._get_last_execution_for_replica(
            ctxt, replica, requery=False)
        self._update_task_info_for_minion_allocations(
            ctxt, replica, minion_machine_allocations)

        last_replica_execution = db_api.get_replica_tasks_execution(
            ctxt, replica.id, last_replica_execution.id)
        self._begin_tasks(
            ctxt, replica, last_replica_execution)

    @replica_synchronized
    def report_replica_minions_allocation_error(
            self, ctxt, replica_id, minion_allocation_error_details):
        replica = self._get_replica(ctxt, replica_id)
        awaiting_minions_status = (
            constants.EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS)
        if replica.last_execution_status != awaiting_minions_status:
            raise exception.InvalidReplicaState(
                "Replica is in '%s' status instead of the expected '%s' to "
                "have minion machines allocations fail for it." % (
                    replica.last_execution_status, awaiting_minions_status))

        last_replica_execution = self._get_last_execution_for_replica(
            ctxt, replica, requery=False)
        LOG.warn(
            "Error occured while allocating minion machines for Replica '%s'. "
            "Cancelling the current Replica Execution ('%s'). Error was: %s",
            replica_id, last_replica_execution.id,
            minion_allocation_error_details)
        self._cancel_tasks_execution(
            ctxt, last_replica_execution, requery=True)

    @migration_synchronized
    def confirm_migration_minions_allocation(
            self, ctxt, migration_id, minion_machine_allocations):
        migration = self._get_migration(ctxt, migration_id)

        awaiting_minions_status = (
            constants.EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS)
        if migration.last_execution_status != awaiting_minions_status:
            raise exception.InvalidMigrationState(
                "Migration is in '%s' status instead of the expected '%s' to "
                "have minion machines allocated for it." % (
                    migration.last_execution_status, awaiting_minions_status))

        execution = self._get_execution_for_migration(
            ctxt, migration, requery=False)
        self._update_task_info_for_minion_allocations(
            ctxt, migration, minion_machine_allocations)
        self._begin_tasks(ctxt, migration, execution)

    @migration_synchronized
    def report_migration_minions_allocation_error(
            self, ctxt, migration_id, minion_allocation_error_details):
        migration = self._get_migration(ctxt, migration_id)
        awaiting_minions_status = (
            constants.EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS)
        if migration.last_execution_status != awaiting_minions_status:
            raise exception.InvalidReplicaState(
                "Machine is in '%s' status instead of the expected '%s' to "
                "have minion machines allocations fail for it." % (
                    migration.last_execution_status, awaiting_minions_status))

        execution = self._get_execution_for_migration(
            ctxt, migration, requery=False)
        LOG.warn(
            "Error occured while allocating minion machines for Migration '%s'. "
            "Cancelling the current Execution ('%s'). Error was: %s",
            migration_id, execution.id, minion_allocation_error_details)
        self._cancel_tasks_execution(
            ctxt, execution, requery=True)

    def migrate_instances(self, ctxt, origin_endpoint_id,
                          destination_endpoint_id, origin_minion_pool_id,
                          destination_minion_pool_id,
                          instance_osmorphing_minion_pool_mappings,
                          source_environment, destination_environment,
                          instances, network_map, storage_mappings,
                          replication_count, shutdown_instances=False,
                          notes=None, skip_os_morphing=False, user_scripts=None):
        origin_endpoint = self.get_endpoint(ctxt, origin_endpoint_id)
        destination_endpoint = self.get_endpoint(ctxt, destination_endpoint_id)
        self._check_endpoints(ctxt, origin_endpoint, destination_endpoint)

        destination_provider_types = self._get_provider_types(
            ctxt, destination_endpoint)

        migration = models.Migration()
        migration.id = str(uuid.uuid4())
        migration.base_id = migration.id
        migration.origin_endpoint_id = origin_endpoint_id
        migration.destination_endpoint_id = destination_endpoint_id
        migration.destination_environment = destination_environment
        migration.source_environment = source_environment
        migration.network_map = network_map
        migration.storage_mappings = storage_mappings
        migration.last_execution_status = constants.EXECUTION_STATUS_UNEXECUTED
        execution = models.TasksExecution()
        execution.status = constants.EXECUTION_STATUS_UNEXECUTED
        execution.number = 1
        execution.type = constants.EXECUTION_TYPE_MIGRATION
        migration.executions = [execution]
        migration.instances = instances
        migration.info = {}
        migration.user_scripts = user_scripts
        migration.notes = notes
        migration.shutdown_instances = shutdown_instances
        migration.replication_count = replication_count
        migration.origin_minion_pool_id = origin_minion_pool_id
        migration.destination_minion_pool_id = destination_minion_pool_id
        if instance_osmorphing_minion_pool_mappings is None:
            instance_osmorphing_minion_pool_mappings = {}
        migration.instance_osmorphing_minion_pool_mappings = (
            instance_osmorphing_minion_pool_mappings)

        self._check_create_reservation_for_transfer(
            migration, licensing_client.RESERVATION_TYPE_MIGRATION)

        self._check_minion_pools_for_action(ctxt, migration)

        for instance in instances:
            migration.info[instance] = {
                "volumes_info": [],
                "source_environment": source_environment,
                "target_environment": destination_environment,
                "user_scripts": self._get_instance_scripts(
                    user_scripts, instance),
                # NOTE: we must explicitly set this in each VM's info
                # to prevent the Replica disks from being cloned:
                "clone_disks": False}
                # TODO(aznashwan): have these passed separately to the relevant
                # provider methods (they're currently passed directly inside
                # dest-env by the API service when accepting the call)
                # "network_map": network_map,
                # "storage_mappings": storage_mappings,

            get_instance_info_task = self._create_task(
                instance,
                constants.TASK_TYPE_GET_INSTANCE_INFO,
                execution)

            validate_migration_source_inputs_task = self._create_task(
                instance,
                constants.TASK_TYPE_VALIDATE_MIGRATION_SOURCE_INPUTS,
                execution)

            validate_migration_destination_inputs_task = self._create_task(
                instance,
                constants.TASK_TYPE_VALIDATE_MIGRATION_DESTINATION_INPUTS,
                execution,
                depends_on=[get_instance_info_task.id])

            migration_resources_task_ids = []
            validate_source_minion_task = None
            deploy_migration_source_resources_task = None
            migration_resources_task_deps = [
                get_instance_info_task.id,
                validate_migration_source_inputs_task.id]
            if migration.origin_minion_pool_id:
                # NOTE: these values are required for the
                # _check_execution_tasks_sanity call but
                # will be populated later when the pool
                # allocations actually happen:
                migration.info[instance].update({
                    "source_minion_machine_id": None,
                    "source_minion_provider_properties": None,
                    "source_minion_connection_info": None})
                validate_source_minion_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_VALIDATE_SOURCE_MINION_POOL_COMPATIBILITY,
                    execution,
                    depends_on=migration_resources_task_deps)
                migration_resources_task_ids.append(
                    validate_source_minion_task.id)
            else:
                deploy_migration_source_resources_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_DEPLOY_MIGRATION_SOURCE_RESOURCES,
                    execution, depends_on=migration_resources_task_deps)
                migration_resources_task_ids.append(
                    deploy_migration_source_resources_task.id)

            create_instance_disks_task = self._create_task(
                instance, constants.TASK_TYPE_CREATE_INSTANCE_DISKS,
                execution, depends_on=[
                    validate_migration_source_inputs_task.id,
                    validate_migration_destination_inputs_task.id])

            validate_target_minion_task = None
            attach_target_minion_disks_task = None
            deploy_migration_target_resources_task = None
            if migration.destination_minion_pool_id:
                # NOTE: these values are required for the
                # _check_execution_tasks_sanity call but
                # will be populated later when the pool
                # allocations actually happen:
                migration.info[instance].update({
                    "target_minion_machine_id": None,
                    "target_minion_provider_properties": None,
                    "target_minion_connection_info": None,
                    "target_minion_backup_writer_connection_info": None})
                ttyp = (
                    constants.TASK_TYPE_VALIDATE_DESTINATION_MINION_POOL_COMPATIBILITY)
                validate_target_minion_task = self._create_task(
                    instance, ttyp, execution, depends_on=[
                        validate_migration_destination_inputs_task.id])

                attach_target_minion_disks_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_ATTACH_VOLUMES_TO_DESTINATION_MINION,
                    execution, depends_on=[
                        validate_target_minion_task.id,
                        create_instance_disks_task.id])
                migration_resources_task_ids.append(
                    attach_target_minion_disks_task.id)
            else:
                deploy_migration_target_resources_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_DEPLOY_MIGRATION_TARGET_RESOURCES,
                    execution, depends_on=[create_instance_disks_task.id])
                migration_resources_task_ids.append(
                    deploy_migration_target_resources_task.id)

            validate_osmorphing_minion_task = None
            if not skip_os_morphing and (
                    instance in instance_osmorphing_minion_pool_mappings):
                # NOTE: these values are required for the
                # _check_execution_tasks_sanity call but
                # will be populated later when the pool
                # allocations actually happen:
                migration.info[instance].update({
                    "osmorphing_minion_machine_id": None,
                    "osmorphing_minion_provider_properties": None,
                    "osmorphing_minion_connection_info": None})
                validate_osmorphing_minion_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_VALIDATE_OSMORPHING_MINION_POOL_COMPATIBILITY,
                    execution, depends_on=[
                        validate_migration_destination_inputs_task.id])
                migration_resources_task_ids.append(
                    validate_osmorphing_minion_task.id)

            last_sync_task = None
            first_sync_task = None
            for i in range(migration.replication_count):
                # insert SHUTDOWN_INSTANCES task before the last sync:
                if i == (migration.replication_count - 1) and (
                        migration.shutdown_instances):
                    shutdown_deps = migration_resources_task_ids
                    if last_sync_task:
                        shutdown_deps = [last_sync_task.id]
                    last_sync_task = self._create_task(
                        instance, constants.TASK_TYPE_SHUTDOWN_INSTANCE,
                        execution, depends_on=shutdown_deps)

                replication_deps = migration_resources_task_ids
                if last_sync_task:
                    replication_deps = [last_sync_task.id]

                last_sync_task = self._create_task(
                    instance, constants.TASK_TYPE_REPLICATE_DISKS,
                    execution, depends_on=replication_deps)
                if not first_sync_task:
                    first_sync_task = last_sync_task

            release_source_minion_task = None
            delete_source_resources_task = None
            source_resource_cleanup_task = None
            if migration.origin_minion_pool_id:
                release_source_minion_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_RELEASE_SOURCE_MINION,
                    execution,
                    depends_on=[
                        validate_source_minion_task.id,
                        last_sync_task.id],
                    on_error=True)
                source_resource_cleanup_task = release_source_minion_task
            else:
                delete_source_resources_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_DELETE_MIGRATION_SOURCE_RESOURCES,
                    execution, depends_on=[
                        deploy_migration_source_resources_task.id,
                        last_sync_task.id],
                    on_error=True)
                source_resource_cleanup_task = delete_source_resources_task

            cleanup_source_storage_task = self._create_task(
                instance, constants.TASK_TYPE_CLEANUP_INSTANCE_SOURCE_STORAGE,
                execution, depends_on=[
                    first_sync_task.id,
                    source_resource_cleanup_task.id],
                on_error=True)

            target_resources_cleanup_task = None
            if migration.destination_minion_pool_id:
                detach_volumes_from_target_minion_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_DETACH_VOLUMES_FROM_DESTINATION_MINION,
                    execution,
                    depends_on=[
                        attach_target_minion_disks_task.id,
                        last_sync_task.id],
                    on_error=True)

                release_target_minion_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_RELEASE_DESTINATION_MINION,
                    execution, depends_on=[
                        validate_target_minion_task.id,
                        detach_volumes_from_target_minion_task.id],
                    on_error=True)
                target_resources_cleanup_task = release_target_minion_task
            else:
                delete_destination_resources_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_DELETE_MIGRATION_TARGET_RESOURCES,
                    execution, depends_on=[
                        deploy_migration_target_resources_task.id,
                        last_sync_task.id],
                    on_error=True)
                target_resources_cleanup_task = (
                    delete_destination_resources_task)

            deploy_instance_task = self._create_task(
                instance, constants.TASK_TYPE_DEPLOY_INSTANCE_RESOURCES,
                execution, depends_on=[
                    last_sync_task.id,
                    target_resources_cleanup_task.id])

            depends_on = [deploy_instance_task.id]
            osmorphing_resources_cleanup_task = None
            if not skip_os_morphing:
                task_deploy_os_morphing_resources = None
                task_delete_os_morphing_resources = None
                attach_osmorphing_minion_volumes_task = None
                last_osmorphing_resources_deployment_task = None
                if instance in (
                        migration.instance_osmorphing_minion_pool_mappings):
                    osmorphing_vol_attachment_deps = [
                        validate_osmorphing_minion_task.id]
                    osmorphing_vol_attachment_deps.extend(depends_on)
                    attach_osmorphing_minion_volumes_task = self._create_task(
                        instance,
                        constants.TASK_TYPE_ATTACH_VOLUMES_TO_OSMORPHING_MINION,
                        execution, depends_on=osmorphing_vol_attachment_deps)
                    last_osmorphing_resources_deployment_task = (
                        attach_osmorphing_minion_volumes_task)

                    collect_osmorphing_info_task = self._create_task(
                        instance,
                        constants.TASK_TYPE_COLLECT_OSMORPHING_INFO,
                        execution,
                        depends_on=[attach_osmorphing_minion_volumes_task.id])
                    last_osmorphing_resources_deployment_task = (
                        collect_osmorphing_info_task)
                else:
                    task_deploy_os_morphing_resources = self._create_task(
                        instance,
                        constants.TASK_TYPE_DEPLOY_OS_MORPHING_RESOURCES,
                        execution, depends_on=depends_on)
                    last_osmorphing_resources_deployment_task = (
                        task_deploy_os_morphing_resources)

                task_osmorphing = self._create_task(
                    instance, constants.TASK_TYPE_OS_MORPHING,
                    execution, depends_on=[
                        last_osmorphing_resources_deployment_task.id])

                depends_on = [task_osmorphing.id]

                if instance in (
                        migration.instance_osmorphing_minion_pool_mappings):
                    detach_osmorphing_minion_volumes_task = self._create_task(
                        instance,
                        constants.TASK_TYPE_DETACH_VOLUMES_FROM_OSMORPHING_MINION,
                        execution, depends_on=[
                            attach_osmorphing_minion_volumes_task.id,
                            task_osmorphing.id],
                        on_error=True)

                    release_osmorphing_minion_task = self._create_task(
                        instance,
                        constants.TASK_TYPE_RELEASE_OSMORPHING_MINION,
                        execution, depends_on=[
                            validate_osmorphing_minion_task.id,
                            detach_osmorphing_minion_volumes_task.id],
                        on_error=True)
                    depends_on.append(release_osmorphing_minion_task.id)
                    osmorphing_resources_cleanup_task = (
                        release_osmorphing_minion_task)
                else:
                    task_delete_os_morphing_resources = self._create_task(
                        instance, constants.TASK_TYPE_DELETE_OS_MORPHING_RESOURCES,
                        execution, depends_on=[
                            task_deploy_os_morphing_resources.id,
                            task_osmorphing.id],
                        on_error=True)
                    depends_on.append(task_delete_os_morphing_resources.id)
                    osmorphing_resources_cleanup_task = (
                        task_delete_os_morphing_resources)

            if (constants.PROVIDER_TYPE_INSTANCE_FLAVOR in
                    destination_provider_types):
                get_optimal_flavor_task = self._create_task(
                    instance, constants.TASK_TYPE_GET_OPTIMAL_FLAVOR,
                    execution, depends_on=depends_on)
                depends_on = [get_optimal_flavor_task.id]

            finalize_deployment_task = self._create_task(
                instance,
                constants.TASK_TYPE_FINALIZE_INSTANCE_DEPLOYMENT,
                execution, depends_on=depends_on)

            cleanup_failed_deployment_task = self._create_task(
                instance,
                constants.TASK_TYPE_CLEANUP_FAILED_INSTANCE_DEPLOYMENT,
                execution, depends_on=[
                    deploy_instance_task.id,
                    finalize_deployment_task.id],
                on_error_only=True)

            cleanup_deps = [
                create_instance_disks_task.id,
                cleanup_source_storage_task.id,
                target_resources_cleanup_task.id,
                cleanup_failed_deployment_task.id]
            if osmorphing_resources_cleanup_task:
                cleanup_deps.append(osmorphing_resources_cleanup_task.id)
            self._create_task(
                instance, constants.TASK_TYPE_CLEANUP_INSTANCE_TARGET_STORAGE,
                execution, depends_on=cleanup_deps,
                on_error_only=True)

        self._check_execution_tasks_sanity(execution, migration.info)
        db_api.add_migration(ctxt, migration)
        LOG.info("Migration added to DB: %s", migration.id)

        uses_minion_pools = any([
            migration.origin_minion_pool_id,
            migration.destination_minion_pool_id,
            migration.instance_osmorphing_minion_pool_mappings])
        if uses_minion_pools:
            # NOTE: we lock on the migration ID to ensure the minion
            # allocation confirmations don't come in too early:
            with lockutils.lock(
                    constants.MIGRATION_LOCK_NAME_FORMAT % migration.id,
                    external=True):
                self._minion_manager_client.allocate_minion_machines_for_migration(
                    ctxt, migration, include_transfer_minions=True,
                    include_osmorphing_minions=not skip_os_morphing)
                self._set_tasks_execution_status(
                    ctxt, execution.id,
                    constants.EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS)
        else:
            self._begin_tasks(ctxt, migration, execution)

        return self.get_migration(ctxt, migration.id)

    def _get_migration(self, ctxt, migration_id):
        migration = db_api.get_migration(ctxt, migration_id)
        if not migration:
            raise exception.NotFound(
                "Migration with ID '%s' not found." % migration_id)
        return migration

    @migration_synchronized
    def delete_migration(self, ctxt, migration_id):
        migration = self._get_migration(ctxt, migration_id)
        execution = migration.executions[0]
        if execution.status in constants.ACTIVE_EXECUTION_STATUSES:
            raise exception.InvalidMigrationState(
                "Cannot delete Migration '%s' as it is currently in "
                "'%s' state." % (migration_id, execution.status))
        db_api.delete_migration(ctxt, migration_id)

    @migration_synchronized
    def cancel_migration(self, ctxt, migration_id, force):
        migration = self._get_migration(ctxt, migration_id)
        if len(migration.executions) != 1:
            raise exception.InvalidMigrationState(
                "Migration '%s' has in improper number of tasks "
                "executions: %d" % (migration_id, len(migration.executions)))
        execution = migration.executions[0]
        if execution.status not in constants.ACTIVE_EXECUTION_STATUSES:
            raise exception.InvalidMigrationState(
                "Migration '%s' is not currently running" % migration_id)
        if execution.status == constants.EXECUTION_STATUS_CANCELLING and (
                not force):
            raise exception.InvalidMigrationState(
                "Migration '%s' is already being cancelled. Please use the "
                "force option if you'd like to force-cancel it.")

        with lockutils.lock(
                constants.EXECUTION_LOCK_NAME_FORMAT % execution.id,
                external=True):
            self._cancel_tasks_execution(ctxt, execution, force=force)
        self._check_delete_reservation_for_transfer(migration)

    def _cancel_tasks_execution(
            self, ctxt, execution, requery=True, force=False):
        """ Cancels a running Execution by:
        - telling workers to kill any already running non-on-error tasks
        - cancelling any non-on-error tasks which are pending
        - making all on-error-only tasks as scheduled

        NOTE: affects whole execution, only call this
        with a lock on the Execution as a whole!
        """
        if requery:
            execution = db_api.get_tasks_execution(ctxt, execution.id)
        if execution.status == constants.EXECUTION_STATUS_RUNNING:
            LOG.info(
                "Cancelling tasks execution %s. Current status before "
                "cancellation is '%s'", execution.id, execution.status)
            # mark execution as cancelling:
            self._set_tasks_execution_status(
                ctxt, execution.id, constants.EXECUTION_STATUS_CANCELLING)
        elif execution.status == constants.EXECUTION_STATUS_CANCELLING and (
                not force):
            LOG.info(
                "Execution '%s' is already in CANCELLING status and no "
                "force flag was provided, skipping re-cancellation.",
                execution.id)
            self._advance_execution_state(
                ctxt, execution, requery=not requery)
            return
        elif execution.status in constants.FINALIZED_TASK_STATUSES:
            LOG.info(
                "Execution '%s' is in a finalized status '%s'. "
                "Skipping re-cancellation.", execution.id, execution.status)
            return

        # iterate through and kill/cancel any non-error
        # tasks which are running/pending:
        for task in sorted(execution.tasks, key=lambda t: t.index):

            # if force is provided, force-cancel tasks directly:
            if force and task.status in itertools.chain(
                    constants.ACTIVE_TASK_STATUSES,
                    [constants.TASK_STATUS_FAILED_TO_CANCEL]):
                LOG.warn(
                    "Task '%s' is in %s state, but forcibly setting to "
                    "'%s' as part of cancellation of execution '%s' "
                    "because the 'force' flag was provided.",
                    task.id, task.status,
                    constants.TASK_STATUS_FORCE_CANCELED,
                    execution.id)
                db_api.set_task_status(
                    ctxt, task.id, constants.TASK_STATUS_FORCE_CANCELED,
                    exception_details=(
                        "This task was force-canceled at user request. "
                        "Its state prior to its cancellation was '%s'. "
                        "Its error details prior to its cancellation "
                        "were: %s" % (
                            task.status, task.exception_details)))
                continue

            if task.status == constants.TASK_STATUS_SCHEDULED and (
                    not task.depends_on):
                # any SCHEDULED tasks with no dependencies are automatically
                # unscheduled. The chain-reaction of unscheduling all
                # subsequent child tasks will be handled in
                # _advance_execution_state:
                LOG.debug(
                    "Setting currently '%s' task '%s' to '%s' as part of "
                    "cancellation of execution '%s'.",
                    task.status, task.id, constants.TASK_STATUS_UNSCHEDULED,
                    execution.id)
                db_api.set_task_status(
                    ctxt, task.id, constants.TASK_STATUS_UNSCHEDULED,
                    exception_details=(
                        "This task was unscheduled during the cancellation "
                        "of the parent tasks execution."))
            elif task.status in (
                    constants.TASK_STATUS_PENDING,
                    constants.TASK_STATUS_STARTING):
                # any PENDING/STARTING tasks means that they did not have a
                # host assigned to them yet, and presuming the host does not
                # start executing the task until it marks itself as the runner,
                # we can just mark the task as unscheduled:
                LOG.debug(
                    "Setting currently '%s' task '%s' to '%s' as part of the "
                    "cancellation of execution '%s'",
                    task.status, task.id,
                    constants.TASK_STATUS_UNSCHEDULED, execution.id)
                db_api.set_task_status(
                    ctxt, task.id, constants.TASK_STATUS_UNSCHEDULED,
                    exception_details=(
                        "This task was already pending execution but was "
                        "unscheduled during the cancellation of the parent "
                        "tasks execution."))
            elif task.status in (
                    constants.TASK_STATUS_RUNNING,
                    constants.TASK_STATUS_FAILED_TO_CANCEL):
                # cancel any currently running non-error tasks:
                if not task.on_error:
                    LOG.debug(
                        "Sending cancellation request for  %s non-error task  "
                        "'%s' as part of cancellation of execution '%s'",
                        task.status, task.id, execution.id)
                    db_api.set_task_status(
                        ctxt, task.id, constants.TASK_STATUS_CANCELLING)
                    try:
                        worker_rpc = self._get_worker_rpc_for_host(
                            # NOTE: we intetionally lowball the timeout for the
                            # cancellation call to prevent the conductor from
                            # hanging an excessive amount of time:
                            task.host, timeout=10)
                        worker_rpc.cancel_task(
                            ctxt, task.id, task.process_id, force)
                    except (Exception, KeyboardInterrupt):
                        msg = (
                            "Failed to send cancellation request for task '%s'"
                            "to worker host '%s' as part of cancellation of "
                            "execution '%s'. Marking task as '%s' for now and "
                            "awaiting any eventual worker replies later." % (
                                task.id, task.host, execution.id,
                                constants.TASK_STATUS_FAILED_TO_CANCEL))
                        LOG.error(
                            "%s. Exception was: %s", msg,
                            utils.get_exception_details())
                        db_api.set_task_status(
                            ctxt, task.id,
                            constants.TASK_STATUS_FAILED_TO_CANCEL,
                            exception_details=msg)

                # let any on-error tasks run to completion but mark
                # them as CANCELLING_AFTER_COMPLETION so they will
                # be marked as cancelled once they are completed:
                else:
                    LOG.debug(
                        "Marking %s on-error task %s as %s as part of "
                        "cancellation of execution  %s",
                        task.status, task.id,
                        constants.TASK_STATUS_CANCELLING_AFTER_COMPLETION,
                        execution.id)
                    db_api.set_task_status(
                        ctxt, task.id,
                        constants.TASK_STATUS_CANCELLING_AFTER_COMPLETION,
                        exception_details=(
                            "Task will be marked as cancelled after completion"
                            " as it is a cleanup task."))
            elif task.status == constants.TASK_STATUS_ON_ERROR_ONLY:
                # mark all on-error-only tasks as scheduled:
                LOG.debug(
                    "Marking on-error-only task '%s' as scheduled following "
                    "cancellation of execution '%s'",
                    task.id, execution.id)
                db_api.set_task_status(
                    ctxt, task.id, constants.TASK_STATUS_SCHEDULED)
            else:
                LOG.debug(
                    "No action currently taken with respect to task '%s' "
                    "(status '%s', on_error=%s) during cancellation of "
                    "execution '%s'",
                    task.id, task.status, task.on_error, execution.id)

        started_tasks = self._advance_execution_state(
            ctxt, execution, requery=True)
        if started_tasks:
            LOG.info(
                "The following tasks were started after state advancement "
                "of execution '%s' after cancellation request: %s",
                execution.id, started_tasks)
        else:
            LOG.debug(
                "No new tasks were started for execution '%s' following "
                "state advancement after cancellation.", execution.id)

    def _set_tasks_execution_status(self, ctxt, execution_id, execution_status):
        execution = db_api.set_execution_status(
            ctxt, execution_id, execution_status)
        LOG.info(
            "Tasks execution %(id)s (action %(action)s) status updated "
            "to: %(status)s",
            {"id": execution_id, "status": execution_status,
             "action": execution.action_id})

        if execution_status in constants.FINALIZED_EXECUTION_STATUSES:
            LOG.debug(
                "Attempting to deallocate minion machines for finalized "
                "Execution '%s' of type '%s' (action '%s') following "
                "its transition into finalized status '%s'",
                execution.id, execution.type, execution.action_id,
                execution_status)
            action = db_api.get_action(ctxt, execution.action_id)
            self._deallocate_minion_machines_for_action(
                ctxt, action)

            if ctxt.delete_trust_id:
                LOG.debug(
                    "Deleting Keystone trust following status change "
                    "for Execution '%s' (action '%s') to '%s'",
                    execution_id, execution.action_id, execution_status)
                keystone.delete_trust(ctxt)
        else:
            LOG.debug(
                "Not deallocating minion machines for Execution '%s' "
                "of type '%s' (action '%s') yet as it is still in an "
                "active Execution status (%s)",
                execution.id, execution.type, execution.action_id,
                execution_status)

    @parent_tasks_execution_synchronized
    def set_task_host(self, ctxt, task_id, host):
        """ Saves the ID of the worker host which has accepted
        the task to the DB and marks the task as STARTING. """
        task = db_api.get_task(ctxt, task_id)
        new_status = constants.TASK_STATUS_STARTING
        exception_details = None
        if task.status == constants.TASK_STATUS_CANCELLING:
            raise exception.TaskIsCancelling(task_id=task_id)
        elif task.status == constants.TASK_STATUS_CANCELLING_AFTER_COMPLETION:
            if not task.on_error:
                LOG.warn(
                    "Non-error task '%s' was in '%s' status although it should"
                    " not have been. Setting a task host for it anyway.",
                    task.id, task.status)
            LOG.debug(
                "Task '%s' is in %s status, so it will be allowed to "
                "have a host set for it and run to completion.",
                task.id, task.status)
            new_status = constants.TASK_STATUS_CANCELLING_AFTER_COMPLETION
            exception_details = (
                "This is a cleanup task so it will be allowed to run to "
                "completion despite user-cancellation.")
        elif task.status != constants.TASK_STATUS_PENDING:
            raise exception.InvalidTaskState(
                "Task with ID '%s' is in '%s' status instead of the "
                "expected '%s' required for it to have a task host set." % (
                    task_id, task.status, constants.TASK_STATUS_PENDING))
        LOG.info(
            "Setting host for task with ID '%s' to '%s'", task_id, host)
        db_api.set_task_host_properties(ctxt, task_id, host=host)
        db_api.set_task_status(
            ctxt, task_id, new_status,
            exception_details=exception_details)
        LOG.info(
            "Successfully set host for task with ID '%s' to '%s'",
            task_id, host)

    @parent_tasks_execution_synchronized
    def set_task_process(self, ctxt, task_id, process_id):
        """ Sets the ID of the Worker-side process for the given task,
        and marks the task as actually 'RUNNING'. """
        task = db_api.get_task(ctxt, task_id)
        if not task.host:
            raise exception.InvalidTaskState(
                "Task with ID '%s' (current status '%s') has no host set "
                "for it. Cannot set host process." % (
                    task_id, task.status))
        acceptable_statuses = [
            constants.TASK_STATUS_STARTING,
            constants.TASK_STATUS_CANCELLING_AFTER_COMPLETION]
        if task.status not in acceptable_statuses:
            raise exception.InvalidTaskState(
                "Task with ID '%s' is in '%s' status instead of the "
                "expected statuses (%s) required for it to have a task "
                "process set." % (
                    task_id, task.status, acceptable_statuses))

        LOG.info(
            "Setting process '%s' (host %s) for task '%s' and transitioning "
            "it from status '%s' to '%s'", process_id, task.host, task_id,
            task.status, constants.TASK_STATUS_RUNNING)
        db_api.set_task_host_properties(ctxt, task_id, process_id=process_id)
        db_api.set_task_status(ctxt, task_id, constants.TASK_STATUS_RUNNING)
        LOG.info(
            "Successfully set task process for task with ID '%s' to '%s'",
            task_id, process_id)

    def _check_clean_execution_deadlock(
            self, ctxt, execution, task_statuses=None, requery=True):
        """ Checks whether an execution is deadlocked.
        Deadlocked executions have no currently running/pending tasks
        but some remaining scheduled tasks.
        If this occurs, all pending/error-only tasks are marked
        as DEADLOCKED, and the execution is marked as such too.
        Returns the state of the execution when the check occured
        (either RUNNING or DEADLOCKED)
        """
        if requery:
            execution = db_api.get_tasks_execution(ctxt, execution.id)
        if not task_statuses:
            task_statuses = {}
            for task in execution.tasks:
                task_statuses[task.id] = task.status

        determined_state = constants.EXECUTION_STATUS_RUNNING
        status_vals = task_statuses.values()
        if constants.TASK_STATUS_SCHEDULED in status_vals and not (
                any([stat in status_vals
                     for stat in constants.ACTIVE_TASK_STATUSES])):
            LOG.warn(
                "Execution '%s' is deadlocked. Cleaning up now. "
                "Task statuses are: %s",
                execution.id, task_statuses)
            for task_id, stat in task_statuses.items():
                if stat in (
                        constants.TASK_STATUS_SCHEDULED,
                        constants.TASK_STATUS_ON_ERROR_ONLY):
                    LOG.warn(
                        "Marking deadlocked task '%s' as that (current "
                        "state: %s)", task_id, stat)
                    db_api.set_task_status(
                        ctxt, task_id,
                        constants.TASK_STATUS_CANCELED_FROM_DEADLOCK,
                        exception_details=TASK_DEADLOCK_ERROR_MESSAGE)
            LOG.warn(
                "Marking deadlocked execution '%s' as DEADLOCKED", execution.id)
            self._set_tasks_execution_status(
                ctxt, execution.id, constants.EXECUTION_STATUS_DEADLOCKED)
            LOG.error(
                "Execution '%s' is deadlocked. Cleanup has been performed. "
                "Task statuses at time of deadlock were: %s",
                execution.id, task_statuses)
            determined_state = constants.EXECUTION_STATUS_DEADLOCKED
        return determined_state

    def _get_execution_status(self, ctxt, execution, requery=False):
        """ Returns the global status of an execution.
        RUNNING - at least one task is RUNNING, STARTING, PENDING or CANCELLING
        COMPLETED - all non-error-only tasks are COMPLETED
        CANCELED - no more RUNNING/PENDING/SCHEDULED tasks but some CANCELED
        CANCELIING - at least one task in CANCELLING status
        ERROR - not RUNNING and at least one is ERROR'd
        DEADLOCKED - has SCHEDULED tasks but none RUNNING/PENDING/CANCELLING
        """
        is_running = False
        is_canceled = False
        is_cancelling = False
        is_errord = False
        has_scheduled_tasks = False
        task_stat_map = {}
        if requery:
            execution = db_api.get_tasks_execution(ctxt, execution.id)
        for task in execution.tasks:
            task_stat_map[task.id] = task.status
            if task.status in constants.ACTIVE_TASK_STATUSES:
                is_running = True
            if task.status in constants.CANCELED_TASK_STATUSES:
                is_canceled = True
            if task.status in (
                    constants.TASK_STATUS_ERROR,
                    constants.TASK_STATUS_FAILED_TO_SCHEDULE):
                is_errord = True
            if task.status in (
                    constants.TASK_STATUS_CANCELLING,
                    constants.TASK_STATUS_CANCELLING_AFTER_COMPLETION):
                is_cancelling = True
            if task.status == constants.TASK_STATUS_SCHEDULED:
                has_scheduled_tasks = True

        status = constants.EXECUTION_STATUS_COMPLETED
        if has_scheduled_tasks and not is_running:
            status = constants.EXECUTION_STATUS_DEADLOCKED
        elif is_cancelling:
            status = constants.EXECUTION_STATUS_CANCELLING
        elif is_running:
            if is_canceled:
                # means that a cancel was issued but some cleanup tasks are
                # currently being run, so the final status is CANCELLING:
                status = constants.EXECUTION_STATUS_CANCELLING
            else:
                status = constants.EXECUTION_STATUS_RUNNING
        elif is_errord:
            status = constants.EXECUTION_STATUS_ERROR
        # NOTE: user-canceled executions should never have ERROR'd tasks
        # (they should also be marked as CANCELED) so this comes last:
        elif is_canceled:
            status = constants.EXECUTION_STATUS_CANCELED

        LOG.debug(
            "Overall status for Execution '%s' determined to be '%s'."
            "Task statuses at time of decision: %s",
            execution.id, status, task_stat_map)
        return status

    def _advance_execution_state(
            self, ctxt, execution, requery=True, instance=None):
        """ Advances the state of the execution by starting/refreshing
        the state of all child tasks.
        If the execution has finalized (either completed or error'd),
        updates its state to the finalized one.
        Returns a list of all the tasks which were started.
        NOTE: should only be called with a lock on the Execution!

        Requirements for a task to be started:
        - any SCHEDULED task with no deps will be instantly started
        - any task where all parent tasks got UNSCHEDULED will
          also be UNSCHEDULED
        - normal tasks (task.on_error==False & task.status==SCHEDULED):
            * started if all parent dependency tasks have been COMPLETED
            * instantly unscheduled if all parents finalized but some
              didn't complete successfuly.
        - on-error tasks (task.on_error==True & task.status==SCHEDULED):
            * all parent tasks (including on-error parents) must have
              reached a terminal state
            * at least one non-error parent task must have been COMPLETED
        """
        if requery:
            execution = db_api.get_tasks_execution(ctxt, execution.id)
        if execution.status not in constants.ACTIVE_EXECUTION_STATUSES:
            LOG.warn(
                "Execution state advancement called on Execution '%s' which "
                "is not in an active status in the DB (it's currently '%s'). "
                "Double-checking for deadlock and returning early.",
                execution.id, execution.status)
            if self._check_clean_execution_deadlock(
                    ctxt, execution, task_statuses=None,
                    requery=not requery) == (
                        constants.EXECUTION_STATUS_DEADLOCKED):
                LOG.error(
                    "Execution '%s' deadlocked even before Replica state "
                    "advancement . Cleanup has been perfomed. Returning.",
                    execution.id)
            return []

        tasks_to_process = execution.tasks
        if instance:
            tasks_to_process = [
                task for task in execution.tasks
                if task.instance == instance]
        if not tasks_to_process:
            raise exception.InvalidActionTasksExecutionState(
                "State advancement requested for execution '%s' for "
                "instance '%s', which has no tasks defined for it." % (
                    execution.id, instance))

        LOG.debug(
            "State of execution '%s' before state advancement is: %s",
            execution.id, execution.status)

        origin = self._get_task_origin(ctxt, execution.action)
        destination = self._get_task_destination(ctxt, execution.action)
        action = db_api.get_action(ctxt, execution.action_id)
        origin_endpoint = db_api.get_endpoint(
            ctxt, execution.action.origin_endpoint_id)
        destination_endpoint = db_api.get_endpoint(
            ctxt, execution.action.destination_endpoint_id)

        started_tasks = []

        def _start_task(task):
            task_info = None
            if task.instance not in action.info:
                LOG.error(
                    "No info present for instance '%s' in action '%s' for task"
                    " '%s' (type '%s') of execution '%s' (type '%s'). "
                    "Defaulting to empty dict." % (
                        task.instance, action.id, task.id, task.task_type,
                        execution.id, execution.type))
                task_info = {}
            else:
                task_info = action.info[task.instance]
            db_api.set_task_status(
                ctxt, task.id, constants.TASK_STATUS_PENDING)
            try:
                worker_rpc = self._get_worker_service_rpc_for_task(
                    ctxt, task, origin_endpoint, destination_endpoint)
                worker_rpc.begin_task(
                    ctxt,
                    task_id=task.id,
                    task_type=task.task_type,
                    origin=origin,
                    destination=destination,
                    instance=task.instance,
                    task_info=task_info)
                LOG.debug(
                    "Successfully started task with ID '%s' (type '%s') "
                    "for execution '%s'", task.id, task.task_type,
                    execution.id)
                started_tasks.append(task.id)
                return constants.TASK_STATUS_PENDING
            except Exception as ex:
                LOG.warn(
                    "Error occured while starting new task '%s'. "
                    "Cancelling execution '%s'. Error was: %s",
                    task.id, execution.id, utils.get_exception_details())
                self._cancel_tasks_execution(
                    ctxt, execution, requery=True)
                raise

        # aggregate all tasks and statuses:
        task_statuses = {}
        task_deps = {}
        on_error_tasks = []
        for task in execution.tasks:
            task_statuses[task.id] = task.status

            if task.depends_on:
                task_deps[task.id] = task.depends_on
            else:
                task_deps[task.id] = []

            if task.on_error:
                on_error_tasks.append(task.id)

        LOG.debug(
            "All task statuses before execution '%s' lifecycle iteration "
            "(for tasks of instance '%s'): %s",
            execution.id, instance, task_statuses)

        # NOTE: the tasks are saved in a random order in the DB, which
        # complicates the processing logic so we just pre-sort:
        for task in sorted(tasks_to_process, key=lambda t: t.index):

            if task_statuses[task.id] == constants.TASK_STATUS_SCHEDULED:

                # immediately start depency-less tasks (on-error or otherwise)
                if not task_deps[task.id]:
                    LOG.info(
                        "Starting depency-less task '%s'", task.id)
                    task_statuses[task.id] = _start_task(task)
                    continue

                parent_task_statuses = {
                    dep_id: task_statuses[dep_id]
                    for dep_id in task_deps[task.id]}

                # immediately unschedule tasks (on-error or otherwise)
                # if all of their parent tasks got un-scheduled:
                if task_deps[task.id] and all([
                        dep_stat == constants.TASK_STATUS_UNSCHEDULED
                        for dep_stat in parent_task_statuses.values()]):
                    LOG.info(
                        "Unscheduling task '%s' as all parent "
                        "tasks got unscheduled: %s",
                        task.id, parent_task_statuses)
                    db_api.set_task_status(
                        ctxt, task.id, constants.TASK_STATUS_UNSCHEDULED,
                        exception_details=(
                            "Unscheduled due to the unscheduling of all "
                            "parent tasks."))
                    task_statuses[task.id] = constants.TASK_STATUS_UNSCHEDULED
                    continue

                # check all parents have finalized:
                if all([
                        dep_stat in constants.FINALIZED_TASK_STATUSES
                        for dep_stat in parent_task_statuses.values()]):

                    # handle non-error tasks:
                    if task.id not in on_error_tasks:
                        # start non-error tasks whose parents have
                        # all completed successfully:
                        if all([
                                dep_stat == constants.TASK_STATUS_COMPLETED
                                for dep_stat in (
                                        parent_task_statuses.values())]):
                            LOG.info(
                                "Starting task '%s' as all dependencies have "
                                "completed successfully: %s",
                                task.id, parent_task_statuses)
                            task_statuses[task.id] = _start_task(task)
                        else:
                            # it means one/more parents error'd/unscheduled
                            # so we mark this task as unscheduled:
                            LOG.info(
                                "Unscheduling plain task '%s' as not all "
                                "parent tasks completed successfully: %s",
                                task.id, parent_task_statuses)
                            db_api.set_task_status(
                                ctxt, task.id,
                                constants.TASK_STATUS_UNSCHEDULED,
                                exception_details=(
                                    "Unscheduled due to some parent tasks not "
                                    "having completed successfully."))
                            task_statuses[task.id] = (
                                constants.TASK_STATUS_UNSCHEDULED)

                    # handle on-error tasks:
                    else:
                        non_error_parents = {
                            dep_id: task_statuses[dep_id]
                            for dep_id in parent_task_statuses.keys()
                            if dep_id not in on_error_tasks}

                        # start on-error tasks only if at least one non-error
                        # parent task has completed successfully:
                        if constants.TASK_STATUS_COMPLETED in (
                                non_error_parents.values()):
                            LOG.info(
                                "Starting on-error task '%s' as all parent "
                                "tasks have been finalized and at least one "
                                "non-error parent (%s) was completed: %s",
                                task.id, list(non_error_parents.keys()),
                                parent_task_statuses)
                            task_statuses[task.id] = _start_task(task)
                        else:
                            LOG.info(
                                "Unscheduling on-error task '%s' as none of "
                                "its parent non-error tasks (%s) have "
                                "completed successfully: %s",
                                task.id, list(non_error_parents.keys()),
                                parent_task_statuses)
                            db_api.set_task_status(
                                ctxt, task.id,
                                constants.TASK_STATUS_UNSCHEDULED,
                                exception_details=(
                                    "Unscheduled due to no non-error parent "
                                    "tasks having completed successfully."))
                            task_statuses[task.id] = (
                                constants.TASK_STATUS_UNSCHEDULED)

                else:
                    LOG.debug(
                        "No lifecycle decision was taken with respect to task "
                        "%s of execution %s as not all parent tasks have "
                        "reached a terminal state: %s",
                        task.id, execution.id, parent_task_statuses)
            else:
                LOG.debug(
                    "No lifecycle decision to make for task '%s' of execution "
                    "'%s' as it is not in a position to be scheduled: %s",
                    task.id, execution.id, task_statuses[task.id])

        if started_tasks:
            LOG.debug(
                "Started the following tasks for execution '%s': %s",
                execution.id, started_tasks)
        else:
            # check for deadlock:
            if self._check_clean_execution_deadlock(
                    ctxt, execution, task_statuses=task_statuses) == (
                        constants.EXECUTION_STATUS_DEADLOCKED):
                LOG.error(
                    "Execution '%s' deadlocked after Replica state advancement"
                    ". Cleanup has been perfomed. Returning early.",
                    execution.id)
                return []
            LOG.debug(
                "No new tasks were started for execution '%s'", execution.id)

        # check if execution status has changed:
        latest_execution_status = self._get_execution_status(
            ctxt, execution, requery=True)
        if latest_execution_status != execution.status:
            LOG.info(
                "Execution '%s' transitioned from status %s to %s "
                "following the updated task statuses: %s",
                execution.id, execution.status,
                latest_execution_status, task_statuses)
            self._set_tasks_execution_status(
                ctxt, execution.id, latest_execution_status)
        else:
            LOG.debug(
                "Execution '%s' has remained in status '%s' following "
                "state advancement. task statuses are: %s",
                execution.id, latest_execution_status, task_statuses)

        return started_tasks

    def _update_replica_volumes_info(self, ctxt, replica_id, instance,
                                     updated_task_info):
        """ WARN: the lock for the Replica must be pre-acquired. """
        db_api.update_transfer_action_info_for_instance(
            ctxt, replica_id, instance,
            updated_task_info)

    def _update_volumes_info_for_migration_parent_replica(
            self, ctxt, migration_id, instance, updated_task_info):
        migration = db_api.get_migration(ctxt, migration_id)
        replica_id = migration.replica_id

        with lockutils.lock(
                constants.REPLICA_LOCK_NAME_FORMAT % replica_id,
                external=True):
            LOG.debug(
                "Updating volume_info in replica due to snapshot "
                "restore during migration. replica id: %s", replica_id)
            self._update_replica_volumes_info(
                ctxt, replica_id, instance, updated_task_info)

    def _handle_post_task_actions(self, ctxt, task, execution, task_info):
        task_type = task.task_type

        def _check_other_tasks_running(execution, current_task):
            still_running = False
            for other_task in execution.tasks:
                if other_task.id == current_task.id:
                    continue
                if other_task.status in constants.ACTIVE_TASK_STATUSES:
                    still_running = True
                    break
            return still_running

        if task_type == constants.TASK_TYPE_RESTORE_REPLICA_DISK_SNAPSHOTS:

            # When restoring a snapshot in some import providers (OpenStack),
            # a new volume_id is generated. This needs to be updated in the
            # Replica instance as well.
            volumes_info = task_info.get('volumes_info')
            if not volumes_info:
                LOG.warn(
                    "No volumes_info was provided by task '%s' (type '%s') "
                    "after completion. NOT updating parent action '%s'",
                    task.id, task_type, execution.action_id)
            else:
                LOG.debug(
                    "Updating volumes_info for instance '%s' in parent action "
                    "'%s' following completion of task '%s' (type '%s'): %s",
                    task.instance, execution.action_id, task.id, task_type,
                    utils.sanitize_task_info(
                        {'volumes_info': volumes_info}))
                self._update_volumes_info_for_migration_parent_replica(
                    ctxt, execution.action_id, task.instance,
                    {"volumes_info": volumes_info})

        elif task_type == (
                constants.TASK_TYPE_DELETE_REPLICA_TARGET_DISK_SNAPSHOTS):

            if not task_info.get("clone_disks"):
                # The migration completed. If the replica is executed again,
                # new volumes need to be deployed in place of the migrated
                # ones.
                LOG.info(
                    "Unsetting 'volumes_info' for instance '%s' in Replica "
                    "'%s' after completion of Replica task '%s' (type '%s') "
                    "with clone_disks=False.",
                    task.instance, execution.action_id, task.id,
                    task_type)
                self._update_volumes_info_for_migration_parent_replica(
                    ctxt, execution.action_id, task.instance,
                    {"volumes_info": []})

        elif task_type in (
                constants.TASK_TYPE_FINALIZE_REPLICA_INSTANCE_DEPLOYMENT,
                constants.TASK_TYPE_FINALIZE_INSTANCE_DEPLOYMENT):
            # set 'transfer_result' in the 'base_transfer_action'
            # table if the task returned a result.
            if "transfer_result" in task_info:
                transfer_result = task_info.get("transfer_result")
                try:
                    schemas.validate_value(
                        transfer_result,
                        schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA)
                    LOG.debug(
                        "Setting result for transfer action '%s': %s",
                        execution.action_id, transfer_result)
                    db_api.set_transfer_action_result(
                        ctxt, execution.action_id, task.instance,
                        transfer_result)
                except exception.SchemaValidationException:
                    LOG.warn(
                        "Could not validate transfer result '%s' against the "
                        "VM export info schema. NOT saving value in Database. "
                        "Exception details: %s",
                        transfer_result, utils.get_exception_details())
            else:
                LOG.debug(
                    "No 'transfer_result' was returned for task type '%s' "
                    "for transfer action '%s'", task_type, execution.action_id)
        elif task_type in (
                constants.TASK_TYPE_UPDATE_SOURCE_REPLICA,
                constants.TASK_TYPE_UPDATE_DESTINATION_REPLICA):
            # NOTE: remember to update the `volumes_info`:
            # NOTE: considering this method is only called with a lock on the
            # `execution.action_id` (in a Replica update tasks' case that's the
            # ID of the Replica itself) we can safely call
            # `_update_replica_volumes_info` below:
            self._update_replica_volumes_info(
                ctxt, execution.action_id, task.instance,
                {"volumes_info": task_info.get("volumes_info", [])})

            if task_type == constants.TASK_TYPE_UPDATE_DESTINATION_REPLICA:
                # check if this was the last task in the update execution:
                still_running = _check_other_tasks_running(execution, task)
                if not still_running:
                    # it means this was the last update task in the Execution
                    # and we may safely update the params of the Replica
                    # as they are in the DB:
                    LOG.info(
                        "All tasks of the '%s' Replica update procedure have "
                        "completed successfully.  Setting the updated parameter "
                        "values on the parent Replica itself.",
                        execution.action_id)
                    # NOTE: considering all the instances of the Replica get
                    # the same params, it doesn't matter which instance's
                    # update task finishes last:
                    db_api.update_replica(
                        ctxt, execution.action_id, task_info)

        elif task_type in (
                constants.TASK_TYPE_ATTACH_VOLUMES_TO_SOURCE_MINION,
                constants.TASK_TYPE_DETACH_VOLUMES_FROM_SOURCE_MINION):

            updated_values = {
                "provider_properties": task_info[
                    "source_minion_provider_properties"]}

            LOG.debug(
                "Updating minion provider properties of minion machine '%s' "
                "following the completion of task '%s' (type '%s') to %s",
                task_info['source_minion_machine_id'],
                task.id, task_type, updated_values)
            db_api.update_minion_machine(
                ctxt, task_info['source_minion_machine_id'], updated_values)

        elif task_type in (
                constants.TASK_TYPE_ATTACH_VOLUMES_TO_DESTINATION_MINION,
                constants.TASK_TYPE_DETACH_VOLUMES_FROM_DESTINATION_MINION):

            updated_values = {
                "provider_properties": task_info[
                    "target_minion_provider_properties"]}

            LOG.debug(
                "Updating minion provider properties of minion machine '%s' "
                "following the completion of task '%s' (type '%s') to %s",
                task_info['target_minion_machine_id'],
                task.id, task_type, updated_values)
            db_api.update_minion_machine(
                ctxt, task_info['target_minion_machine_id'], updated_values)

        elif task_type in (
                constants.TASK_TYPE_ATTACH_VOLUMES_TO_OSMORPHING_MINION,
                constants.TASK_TYPE_DETACH_VOLUMES_FROM_OSMORPHING_MINION):

            updated_values = {
                "provider_properties": task_info[
                    "osmorphing_minion_provider_properties"]}

            LOG.debug(
                "Updating minion provider properties of minion machine '%s' "
                "following the completion of task '%s' (type '%s') to %s",
                task_info['osmorphing_minion_machine_id'],
                task.id, task_type, updated_values)
            db_api.update_minion_machine(
                ctxt, task_info['osmorphing_minion_machine_id'],
                updated_values)

        elif task_type == constants.TASK_TYPE_RELEASE_SOURCE_MINION:

            LOG.debug(
                "Releasing source minion '%s' following the completion of "
                "task with ID '%s' (type '%s')",
                task_info['source_minion_machine_id'],
                task.id, task_type)
            self._minion_manager_client.deallocate_minion_machine(
                ctxt, task_info['source_minion_machine_id'])

        elif task_type == constants.TASK_TYPE_RELEASE_DESTINATION_MINION:

            if task_info['target_minion_machine_id'] != task_info.get(
                    "osmorphing_minion_machine_id"):
                LOG.debug(
                    "Releasing destination minion '%s' following the "
                    "completion of task with ID '%s' (type '%s')",
                    task_info['target_minion_machine_id'],
                    task.id, task_type)
                self._minion_manager_client.deallocate_minion_machine(
                    ctxt, task_info['target_minion_machine_id'])
            else:
                LOG.debug(
                    "NOT releasing destination minion with ID '%s' following "
                    "the completion of task with ID '%s' (type '%s') as it "
                    "also to be used as the OSMorphing minion.",
                    task_info['target_minion_machine_id'],
                    task.id, task_type)

        elif task_type == constants.TASK_TYPE_RELEASE_OSMORPHING_MINION:

            LOG.debug(
                "Releasing OSMorphing minion '%s' following the completion of "
                "task with ID '%s' (type '%s')",
                task_info['osmorphing_minion_machine_id'],
                task.id, task_type)
            self._minion_manager_client.deallocate_minion_machine(
                ctxt, task_info['osmorphing_minion_machine_id'])

        else:
            LOG.debug(
                "No post-task actions required for task '%s' of type '%s'",
                task.id, task_type)

    @parent_tasks_execution_synchronized
    def task_completed(self, ctxt, task_id, task_result):
        LOG.info("Task completed: %s", task_id)

        task = db_api.get_task(ctxt, task_id)
        if task.status == constants.TASK_STATUS_CANCELLING_AFTER_COMPLETION:
            if not task.on_error:
                LOG.warn(
                    "Non-error task '%s' was marked as %s although it should "
                    "not have. It was run to completion anyway.",
                    task.id, task.status)
            LOG.info(
                "On-error task '%s' which was '%s' has just completed "
                "successfully.  Marking it as '%s' as a final status, "
                "but processing its result as if it completed successfully.",
                task_id, task.status,
                constants.TASK_STATUS_CANCELED_AFTER_COMPLETION)
            db_api.set_task_status(
                ctxt, task_id, constants.TASK_STATUS_CANCELED_AFTER_COMPLETION,
                exception_details=(
                    "This is a cleanup task so it was allowed to run to "
                    "completion after user-cancellation."))
        elif task.status == constants.TASK_STATUS_CANCELLING:
            LOG.error(
                "Received confirmation that presumably cancelling task '%s' "
                "(status '%s') has just completed successfully. "
                "This should have never happened and indicates that its worker "
                "host ('%s') has either failed to cancel it properly, or it "
                "was completed before the cancellation request was received. "
                "Please check the worker logs for more details. "
                "Marking as %s and processing its result as if it completed "
                "successfully.",
                task.id, task.status, task.host,
                constants.TASK_STATUS_CANCELED_AFTER_COMPLETION)
            db_api.set_task_status(
                ctxt, task_id, constants.TASK_STATUS_CANCELED_AFTER_COMPLETION,
                exception_details=(
                    "The worker host for this task ('%s') has either failed "
                    "at cancelling it or the cancellation request arrived "
                    "after it was already completed so this task was run to "
                    "completion. Please review the worker logs for "
                    "more relevant details." % (
                        task.host)))
        elif task.status == constants.TASK_STATUS_FAILED_TO_CANCEL:
            LOG.error(
                "Received confirmation that presumably '%s' task '%s' has just "
                "completed successfully. Marking as '%s' and processing its "
                "result as if it had completed normally.",
                task.status, task.id,
                constants.TASK_STATUS_CANCELED_AFTER_COMPLETION)
            db_api.set_task_status(
                ctxt, task_id, constants.TASK_STATUS_CANCELED_AFTER_COMPLETION,
                exception_details=(
                    "The worker host for this task ('%s') had not either not "
                    "accepted task cancellation request when it was asked to "
                    "or had failed to receive the request, so this task was "
                    "run to completion. Please review the worker logs for "
                    "more relevant details." % (
                        task.host)))
        elif task.status in constants.FINALIZED_TASK_STATUSES:
            LOG.error(
                "Received confirmation that presumably finalized task '%s' "
                "(status '%s') has just completed successfully from worker "
                "host '%s'. This should have never happened and indicates "
                "that there is an inconsistency with the task scheduling. "
                "Check the rest of the logs for further details. "
                "The results of this task will NOT be processed.",
                task.id, task.status, task.host)
            return
        else:
            if task.status != constants.TASK_STATUS_RUNNING:
                LOG.warn(
                    "Just-completed task '%s' was in '%s' state instead of "
                    "the expected '%s' state. Marking as '%s' anyway.",
                    task_id, task.status, constants.TASK_STATUS_RUNNING,
                    constants.TASK_STATUS_COMPLETED)
            db_api.set_task_status(
                ctxt, task_id, constants.TASK_STATUS_COMPLETED)

        execution = db_api.get_tasks_execution(ctxt, task.execution_id)
        with lockutils.lock(
                constants.EXECUTION_TYPE_TO_ACTION_LOCK_NAME_FORMAT_MAP[
                    execution.type] % execution.action_id,
                external=True):
            action_id = execution.action_id
            action = db_api.get_action(ctxt, action_id)

            updated_task_info = None
            if task_result:
                LOG.info(
                    "Setting task %(task_id)s (type %(task_type)s)result for "
                    "instance %(instance)s into action %(action_id)s info: "
                    "%(task_result)s", {
                        "task_id": task_id,
                        "instance": task.instance,
                        "task_type": task.task_type,
                        "action_id": action_id,
                        "task_result": utils.sanitize_task_info(
                            task_result)})
                updated_task_info = (
                    db_api.update_transfer_action_info_for_instance(
                        ctxt, action_id, task.instance, task_result))
            else:
                action = db_api.get_action(ctxt, action_id)
                updated_task_info = action.info[task.instance]
                LOG.info(
                    "Task '%s' for instance '%s' of transfer action '%s' "
                    "has completed successfuly but has not returned "
                    "any result.", task.id, task.instance, action_id)

            # NOTE: refresh the execution just in case:
            execution = db_api.get_tasks_execution(ctxt, task.execution_id)
            self._handle_post_task_actions(
                ctxt, task, execution, updated_task_info)

            newly_started_tasks = self._advance_execution_state(
                ctxt, execution, instance=task.instance, requery=False)
            if newly_started_tasks:
                LOG.info(
                    "The following tasks were started for execution '%s' "
                    "following the completion of task '%s' for instance %s: "
                    "%s" % (
                        execution.id, task.id, task.instance,
                        newly_started_tasks))
            else:
                LOG.debug(
                    "No new tasks were started for execution '%s' for instance "
                    "'%s' following the successful completion of task '%s'.",
                    execution.id, task.instance, task.id)

    def _cancel_execution_for_osmorphing_debugging(self, ctxt, execution):
        # go through all scheduled tasks and cancel them:
        for subtask in execution.tasks:
            if subtask.task_type == constants.TASK_TYPE_OS_MORPHING:
                continue

            if subtask.status in constants.ACTIVE_TASK_STATUSES:
                raise exception.CoriolisException(
                    "Task %s is still in an active state (%s) although "
                    "it should not!" % (subtask.id, subtask.status))

            if subtask.status in [
                    constants.TASK_STATUS_SCHEDULED,
                    constants.TASK_STATUS_ON_ERROR_ONLY]:
                msg = (
                    "This task was unscheduled for debugging an error in the "
                    "OSMorphing process.")
                if subtask.on_error:
                    msg = (
                        "%s Please note that any cleanup operations this task "
                        "should have included will need to performed manually "
                        "once the debugging process has been completed." % (
                            msg))
                db_api.set_task_status(
                    ctxt, subtask.id,
                    constants.TASK_STATUS_CANCELED_FOR_DEBUGGING,
                    exception_details=msg)

    @parent_tasks_execution_synchronized
    def confirm_task_cancellation(self, ctxt, task_id, cancellation_details):
        LOG.info(
            "Received confirmation of cancellation for task '%s': %s",
            task_id, cancellation_details)
        task = db_api.get_task(ctxt, task_id)

        final_status = constants.TASK_STATUS_CANCELED
        exception_details = (
            "This task was user-cancelled. Additional cancellation "
            "info from worker service: '%s'" % cancellation_details)
        if task.status == constants.TASK_STATUS_CANCELLING_AFTER_COMPLETION:
            LOG.error(
                "Received cancellation confirmation for task '%s' which was "
                "in '%s' state. This likely means that a double-cancellation "
                "occurred. Marking task as '%s' either way.",
                task.id, task.status, final_status)
        elif task.status == constants.TASK_STATUS_FORCE_CANCELED:
            # it means a force cancel has been issued before the
            # confirmation that the task was canceled came in:
            LOG.warn(
                "Only just received cancellation confirmation for "
                "force-canceled task '%s'. Leaving marked as "
                "force-cancelled.", task.id)
            final_status = constants.TASK_STATUS_FORCE_CANCELED
            exception_details = (
                "This task was force-cancelled. Confirmation of its "
                "cancellation did eventually come in. Additional details on "
                "the cancellation: %s" % cancellation_details)
        elif task.status == constants.TASK_STATUS_FAILED_TO_CANCEL:
            final_status = constants.TASK_STATUS_CANCELED
            LOG.warn(
                "Only just received cancellation confirmation for task '%s' "
                "despite it being marked as '%s'. Marking as '%s' anyway. "
                "Error reported by worker was: %s",
                task.id, task.status, final_status, exception_details)
            exception_details = (
                "The worker service for this task (%s) had either failed to "
                "cancel this task when it was initially asked to or did not "
                "receive the cancellation request, but it did eventually "
                "confirm the task's cancellation with the following "
                "details: %s" % (task.host, cancellation_details))
        elif task.status in constants.FINALIZED_TASK_STATUSES:
            LOG.warn(
                "Received confirmation of cancellation for already finalized "
                "task '%s' (status '%s') from host '%s'. NOT modifying "
                "its status.", task.id, task.status, task.host)
            final_status = task.status
        elif task.status != constants.TASK_STATUS_CANCELLING:
            LOG.warn(
                "Received confirmation of cancellation for non-CANCELLING "
                "task '%s' (status '%s'). Marking as '%s' anyway.",
                task.id, task.status, final_status)

        if final_status == task.status:
            LOG.debug(
                "NOT altering state of finalized task '%s' ('%s') following "
                "confirmation of cancellation. Updating its exception "
                "details though: %s", task.id, task.status, exception_details)
            db_api.set_task_status(
                ctxt, task.id, final_status,
                exception_details=exception_details)
        else:
            LOG.info(
                "Transitioning canceled task '%s' from '%s' to '%s' following "
                "confirmation of its cancellation.",
                task.id, task.status, final_status)
            db_api.set_task_status(
                ctxt, task.id, final_status,
                exception_details=exception_details)
            execution = db_api.get_tasks_execution(ctxt, task.execution_id)
            self._advance_execution_state(ctxt, execution, requery=False)

    @parent_tasks_execution_synchronized
    def set_task_error(self, ctxt, task_id, exception_details):
        LOG.error(
            "Received error confirmation for task: %(task_id)s - %(ex)s",
            {"task_id": task_id, "ex": exception_details})

        task = db_api.get_task(ctxt, task_id)

        final_status = constants.TASK_STATUS_ERROR
        if task.status == constants.TASK_STATUS_CANCELLING:
            final_status = constants.TASK_STATUS_CANCELED
        elif task.status == constants.TASK_STATUS_CANCELLING_AFTER_COMPLETION:
            final_status = constants.TASK_STATUS_CANCELED
            if not task.on_error:
                LOG.warn(
                    "Non-error '%s' was in '%s' status although it should "
                    "never have been marked as such. Marking as '%s' anyway.",
                    task.id, task.status, final_status)
            else:
                LOG.warn(
                    "On-error task '%s' which was in '%s' status ended up "
                    "error-ing. Marking as '%s'",
                    task.id, task.status, final_status)
                exception_details = (
                    "This is a cleanup task so was allowed to complete "
                    "following user-cancellation, but encountered an "
                    "error: %s" % exception_details)
        elif task.status == constants.TASK_STATUS_FORCE_CANCELED:
            # it means a force cancel has been issued priorly but
            # the task has error'd anyway:
            LOG.warn(
                "Only just received error confirmation for force-cancelled "
                "task '%s'. Leaving marked as force-cancelled.", task.id)
            final_status = constants.TASK_STATUS_FORCE_CANCELED
            exception_details = (
                "This task was force-cancelled but ended up errorring anyway. "
                "The error details were: '%s'" % exception_details)
        elif task.status == constants.TASK_STATUS_FAILED_TO_CANCEL:
            final_status = constants.TASK_STATUS_CANCELED
            LOG.warn(
                "Only just received error confirmation for task '%s' despite "
                "it being marked as '%s'. Marking as '%s' anyway. Error "
                "reported by worker was: %s",
                task.id, task.status, final_status, exception_details)
            exception_details = (
                "The worker service for this task (%s) has failed to cancel "
                "this task when it was asked to, and the task eventually "
                "exited with an error. The error details were: %s" % (
                    task.host, exception_details))
        elif task.status in constants.FINALIZED_TASK_STATUSES:
            LOG.error(
                "Error confirmation on task '%s' arrived despite it being "
                "in a terminal state ('%s'). This should never happen and "
                "indicates an issue with its scheduling/handling. Error "
                "was: %s", task.id, task.status, exception_details)
            exception_details = (
                "Error confirmation came in for this task despite it having "
                "already been marked as %s. Please notify support of this "
                "occurence and share the Conductor and Worker logs. "
                "Error message in confirmation was: %s" % (
                    task.status, exception_details))

        LOG.debug(
            "Transitioning errored task '%s' from '%s' to '%s'",
            task.id, task.status, final_status)
        db_api.set_task_status(
            ctxt, task_id, final_status, exception_details)

        task = db_api.get_task(ctxt, task_id)
        execution = db_api.get_tasks_execution(ctxt, task.execution_id)

        action_id = execution.action_id
        action = db_api.get_action(ctxt, action_id)
        with lockutils.lock(
                constants.EXECUTION_TYPE_TO_ACTION_LOCK_NAME_FORMAT_MAP[
                    execution.type] % action_id,
                external=True):
            if task.task_type == constants.TASK_TYPE_OS_MORPHING and (
                    CONF.conductor.debug_os_morphing_errors):
                LOG.debug(
                    "Attempting to cancel execution '%s' of action '%s' "
                    "for OSMorphing debugging.", execution.id, action_id)
                # NOTE: the OSMorphing task always runs by itself so no
                # further tasks should be running, but we double-check here:
                running = [
                    t for t in execution.tasks
                    if t.status in constants.ACTIVE_TASK_STATUSES
                    and t.task_type != constants.TASK_TYPE_OS_MORPHING]
                if not running:
                    self._cancel_execution_for_osmorphing_debugging(
                        ctxt, execution)
                    db_api.set_task_status(
                        ctxt, task_id, final_status,
                        exception_details=(
                            "An error has occured during OSMorphing. Cleanup "
                            "will not be performed for debugging reasons. "
                            "Please review the Conductor logs for the debug "
                            "connection info. Original error was: %s" % (
                                exception_details)))
                    LOG.warn(
                        "All subtasks for Migration '%s' have been cancelled "
                        "to allow for OSMorphing debugging. The connection "
                        "info for the worker VM is: %s",
                        action_id, action.info.get(task.instance, {}).get(
                            'osmorphing_connection_info', {}))
                    self._set_tasks_execution_status(
                        ctxt, execution.id,
                        constants.EXECUTION_STATUS_CANCELED_FOR_DEBUGGING)
                else:
                    LOG.warn(
                        "Some tasks are running in parallel with the "
                        "OSMorphing task, a debug setup cannot be safely "
                        "achieved. Proceeding with cleanup tasks as usual.")
                    self._cancel_tasks_execution(ctxt, execution)
            else:
                self._cancel_tasks_execution(ctxt, execution)


            # NOTE: if this was a migration, make sure to delete
            # its associated reservation.
            if execution.type == constants.EXECUTION_TYPE_MIGRATION:
                self._check_delete_reservation_for_transfer(action)

    @task_synchronized
    def task_event(self, ctxt, task_id, level, message):
        LOG.info("Task event: %s", task_id)
        task = db_api.get_task(ctxt, task_id)
        if task.status not in constants.ACTIVE_TASK_STATUSES:
            raise exception.InvalidTaskState(
                "Task with ID '%s' is in a non-running state ('%s') but it "
                "has received a task event from its task host ('%s'). "
                "Refusing task event. The event string was: %s" % (
                    task.id, task.status, task.host, message))
        db_api.add_task_event(ctxt, task_id, level, message)

    @task_synchronized
    def add_task_progress_update(self, ctxt, task_id, total_steps, message):
        LOG.info("Adding task progress update: %s", task_id)
        task = db_api.get_task(ctxt, task_id)
        if task.status not in constants.ACTIVE_TASK_STATUSES:
            raise exception.InvalidTaskState(
                "Task with ID '%s' is in a non-running state ('%s') but it "
                "has received a progress update from its task host ('%s'). "
                "Refusing progress update. The progress update string "
                "was: %s" % (
                    task.id, task.status, task.host, message))
        db_api.add_task_progress_update(ctxt, task_id, total_steps, message)

    @task_synchronized
    def update_task_progress_update(self, ctxt, task_id, step,
                                    total_steps, message):
        LOG.info("Updating task progress update: %s", task_id)
        db_api.update_task_progress_update(
            ctxt, task_id, step, total_steps, message)

    @task_synchronized
    def get_task_progress_step(self, ctxt, task_id):
        return db_api.get_task_progress_step(ctxt, task_id)

    def _get_replica_schedule(self, ctxt, replica_id,
                              schedule_id, expired=True):
        schedule = db_api.get_replica_schedule(
            ctxt, replica_id, schedule_id, expired=expired)
        if not schedule:
            raise exception.NotFound(
                "Schedule with ID '%s' for Replica '%s' not found." %  (
                    schedule_id, replica_id))
        return schedule

    def create_replica_schedule(self, ctxt, replica_id,
                                schedule, enabled, exp_date,
                                shutdown_instance):
        keystone.create_trust(ctxt)
        replica = self._get_replica(ctxt, replica_id)
        replica_schedule = models.ReplicaSchedule()
        replica_schedule.id = str(uuid.uuid4())
        replica_schedule.replica = replica
        replica_schedule.replica_id = replica_id
        replica_schedule.schedule = schedule
        replica_schedule.expiration_date = exp_date
        replica_schedule.enabled = enabled
        replica_schedule.shutdown_instance = shutdown_instance
        replica_schedule.trust_id = ctxt.trust_id

        db_api.add_replica_schedule(
            ctxt, replica_schedule,
            lambda ctxt, sched: self._replica_cron_client.register(
                ctxt, sched))
        return self.get_replica_schedule(
            ctxt, replica_id, replica_schedule.id)

    @schedule_synchronized
    def update_replica_schedule(self, ctxt, replica_id, schedule_id,
                                updated_values):
        db_api.update_replica_schedule(
            ctxt, replica_id, schedule_id, updated_values, None,
            lambda ctxt, sched: self._replica_cron_client.register(
                ctxt, sched))
        return self._get_replica_schedule(ctxt, replica_id, schedule_id)

    def _cleanup_schedule_resources(self, ctxt, schedule):
        self._replica_cron_client.unregister(ctxt, schedule)
        if schedule.trust_id:
            tmp_trust = context.get_admin_context(
                trust_id=schedule.trust_id)
            keystone.delete_trust(tmp_trust)

    @schedule_synchronized
    def delete_replica_schedule(self, ctxt, replica_id, schedule_id):
        db_api.delete_replica_schedule(
            ctxt, replica_id, schedule_id, None,
            lambda ctxt, sched: self._cleanup_schedule_resources(
                ctxt, sched))

    @replica_synchronized
    def get_replica_schedules(self, ctxt, replica_id=None, expired=True):
        return db_api.get_replica_schedules(
            ctxt, replica_id=replica_id, expired=expired)

    @schedule_synchronized
    def get_replica_schedule(self, ctxt, replica_id,
                             schedule_id, expired=True):
        return self._get_replica_schedule(
            ctxt, replica_id, schedule_id, expired=expired)

    @replica_synchronized
    def update_replica(
            self, ctxt, replica_id, updated_properties):
        replica = self._get_replica(ctxt, replica_id)

        minion_pool_fields = [
            "origin_minion_pool_id", "destination_minion_pool_id",
            "instance_osmorphing_minion_pool_mappings"]
        if any([mpf in updated_properties for mpf in minion_pool_fields]):
            # NOTE: this is just a dummy Replica model to use for validation:
            dummy = models.Replica()
            dummy.id = replica.id
            dummy.instances = replica.instances
            dummy.origin_endpoint_id = replica.origin_endpoint_id
            dummy.destination_endpoint_id = replica.destination_endpoint_id
            dummy.origin_minion_pool_id = updated_properties.get(
                'origin_minion_pool_id')
            dummy.destination_minion_pool_id = updated_properties.get(
                'destination_minion_pool_id')
            dummy.instance_osmorphing_minion_pool_mappings = (
                updated_properties.get(
                    'instance_osmorphing_minion_pool_mappings'))
            self._check_minion_pools_for_action(ctxt, dummy)

        self._check_replica_running_executions(ctxt, replica)
        self._check_valid_replica_tasks_execution(replica, force=True)
        if updated_properties.get('user_scripts'):
            replica.user_scripts = updated_properties['user_scripts']
        execution = models.TasksExecution()
        execution.id = str(uuid.uuid4())
        execution.status = constants.EXECUTION_STATUS_UNEXECUTED
        execution.action = replica
        execution.type = constants.EXECUTION_TYPE_REPLICA_UPDATE

        for instance in replica.instances:
            LOG.debug(
                "Pre-replica-update task_info for instance '%s' of Replica "
                "'%s': %s", instance, replica_id,
                utils.sanitize_task_info(
                    replica.info[instance]))

            # NOTE: "circular assignment" would lead to a `None` value
            # so we must operate on a copy:
            inst_info_copy = copy.deepcopy(replica.info[instance])

            # NOTE: we update the various values in the task info itself
            # As a result, the values within the task_info will be the updated
            # values which will be checked. The old values will be sent to the
            # tasks through the origin/destination parameters for them to be
            # compared to the new ones.
            # The actual values on the Replica object itself will be set
            # during _handle_post_task_actions once the final destination-side
            # update task will be completed.
            inst_info_copy.update({
                key: updated_properties[key]
                for key in updated_properties
                if key != "destination_environment"})
            # NOTE: the API service labels the target-env as the
            # "destination_environment":
            if "destination_environment" in updated_properties:
                inst_info_copy["target_environment"] = updated_properties[
                    "destination_environment"]
            replica.info[instance] = inst_info_copy

            LOG.debug(
                "Updated task_info for instance '%s' of Replica "
                "'%s' which will be verified during update procedure: %s",
                instance, replica_id, utils.sanitize_task_info(
                    replica.info[instance]))

            get_instance_info_task = self._create_task(
                instance, constants.TASK_TYPE_GET_INSTANCE_INFO,
                execution)
            update_source_replica_task = self._create_task(
                instance, constants.TASK_TYPE_UPDATE_SOURCE_REPLICA,
                execution)
            self._create_task(
                instance, constants.TASK_TYPE_UPDATE_DESTINATION_REPLICA,
                execution,
                depends_on=[
                    get_instance_info_task.id,
                    # NOTE: the dest-side update task must be done after
                    # the source-side one as both can potentially modify
                    # the 'volumes_info' together:
                    update_source_replica_task.id])

        self._check_execution_tasks_sanity(execution, replica.info)

        # update the action info for all of the instances in the Replica:
        for instance in execution.action.instances:
            db_api.update_transfer_action_info_for_instance(
                ctxt, replica.id, instance, replica.info[instance])

        db_api.add_replica_tasks_execution(ctxt, execution)
        LOG.debug("Execution for Replica update tasks created: %s",
                  execution.id)

        self._begin_tasks(ctxt, replica, execution)

        return self.get_replica_tasks_execution(ctxt, replica_id, execution.id)

    def get_diagnostics(self, ctxt):
        return utils.get_diagnostics_info()

    def create_region(self, ctxt, region_name, description="", enabled=True):
        region = models.Region()
        region.id = str(uuid.uuid4())
        region.name = region_name
        region.description = description
        region.enabled = enabled
        db_api.add_region(ctxt, region)
        return self.get_region(ctxt, region.id)

    def get_regions(self, ctxt):
        return db_api.get_regions(ctxt)

    @region_synchronized
    def get_region(self, ctxt, region_id):
        region = db_api.get_region(ctxt, region_id)
        if not region:
            raise exception.NotFound(
                "Region with ID '%s' not found." % region_id)
        return region

    @region_synchronized
    def update_region(self, ctxt, region_id, updated_values):
        LOG.info(
            "Attempting to update region '%s' with payload: %s",
            region_id, updated_values)
        db_api.update_region(ctxt, region_id, updated_values)
        LOG.info("Region '%s' successfully updated", region_id)
        return db_api.get_region(ctxt, region_id)

    @region_synchronized
    def delete_region(self, ctxt, region_id):
        # TODO(aznashwan): add checks for endpoints/services
        # associated to the region before deletion:
        db_api.delete_region(ctxt, region_id)

    def register_service(
            self, ctxt, host, binary, topic, enabled, mapped_regions=None,
            providers=None, specs=None):
        service = db_api.find_service(ctxt, host, binary, topic=topic)
        if service:
            raise exception.Conflict(
                "A Service with the specified parameters (host %s, binary %s, "
                "topic %s) has already been registered under ID: %s" % (
                    host, binary, topic, service.id))

        service = models.Service()
        service.id = str(uuid.uuid4())
        service.host = host
        service.binary = binary
        service.enabled = enabled
        service.topic = topic
        service.status = constants.SERVICE_STATUS_UP

        if None in (providers, specs):
            worker_rpc = self._get_worker_rpc_for_host(service['host'])
            status = worker_rpc.get_service_status(ctxt)

            service.providers = status["providers"]
            service.specs = status["specs"]
        else:
            service.providers = providers
            service.specs = specs

        # create the service:
        db_api.add_service(ctxt, service)
        LOG.debug(
            "Added new service to DB: %s", service.id)

        # add region associations:
        if mapped_regions:
            try:
                db_api.update_service(
                    ctxt, service.id, {
                        "mapped_regions": mapped_regions})
            except Exception as ex:
                LOG.warn(
                    "Error adding region mappings during new service "
                    "registration (host: %s), cleaning up endpoint and "
                    "all created mappings for regions: %s",
                    service.host, mapped_regions)
                db_api.delete_service(ctxt, service.id)
                raise

        return self.get_service(ctxt, service.id)

    def check_service_registered(self, ctxt, host, binary, topic):
        props = "host='%s', binary='%s', topic='%s'" % (host, binary, topic)
        LOG.debug(
            "Checking for existence of service with properties: %s", props)
        service = db_api.find_service(ctxt, host, binary, topic=topic)
        if service:
            LOG.debug(
                "Found service '%s' for properties %s", service.id, props)
        else:
            LOG.debug(
                "Could not find any service with the specified "
                "properties: %s", props)
        return service

    @service_synchronized
    def refresh_service_status(self, ctxt, service_id):
        LOG.debug("Updating registration for worker service '%s'", service_id)
        service = db_api.get_service(ctxt, service_id)
        worker_rpc = self._get_worker_rpc_for_host(service['host'])
        status = worker_rpc.get_service_status(ctxt)
        updated_values = {
            "providers": status["providers"],
            "specs": status["specs"],
            "status": constants.SERVICE_STATUS_UP}
        db_api.update_service(ctxt, service_id, updated_values)
        LOG.debug("Successfully refreshed status of service '%s'", service_id)
        return db_api.get_service(ctxt, service_id)

    def get_services(self, ctxt):
        return db_api.get_services(ctxt)

    @service_synchronized
    def get_service(self, ctxt, service_id):
        service = db_api.get_service(ctxt, service_id)
        if not service:
            raise exception.NotFound(
                "Service with ID '%s' not found." % service_id)
        return service

    @service_synchronized
    def update_service(self, ctxt, service_id, updated_values):
        LOG.info(
            "Attempting to update service '%s' with payload: %s",
            service_id, updated_values)
        db_api.update_service(ctxt, service_id, updated_values)
        LOG.info("Successfully updated service '%s'", service_id)
        return db_api.get_service(ctxt, service_id)

    @service_synchronized
    def delete_service(self, ctxt, service_id):
        db_api.delete_service(ctxt, service_id)
