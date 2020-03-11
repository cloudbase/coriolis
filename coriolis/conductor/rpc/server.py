# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import copy
import functools
import itertools
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
from coriolis.replica_cron.rpc import client as rpc_cron_client
from coriolis import schemas
from coriolis import utils
from coriolis.worker.rpc import client as rpc_worker_client


VERSION = "1.0"

LOG = logging.getLogger(__name__)


conductor_opts = [
    cfg.BoolOpt("debug_os_morphing_errors",
                default=False,
                help="If set, any OSMorphing task which errors out will have "
                     "all of its following tasks unscheduled so as to allow "
                     "for live debugging of the OSMorphing setup.")
]

CONF = cfg.CONF
CONF.register_opts(conductor_opts, 'conductor')

TASK_LOCK_NAME_FORMAT = "task-%s"
EXECUTION_LOCK_NAME_FORMAT = "execution-%s"
ENDPOINT_LOCK_NAME_FORMAT = "endpoint-%s"
MIGRATION_LOCK_NAME_FORMAT = "migration-%s"
REPLICA_LOCK_NAME_FORMAT = "replica-%s"
SCHEDULE_LOCK_NAME_FORMAT = "schedule-%s"

TASK_DEADLOCK_ERROR_MESSAGE = (
    "A fatal deadlock has occurred. Further debugging is required. "
    "Please review the Conductor logs and contact support for assistance.")


def endpoint_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, endpoint_id, *args, **kwargs):
        @lockutils.synchronized(
                ENDPOINT_LOCK_NAME_FORMAT % endpoint_id)
        def inner():
            return func(self, ctxt, endpoint_id, *args, **kwargs)
        return inner()
    return wrapper


def replica_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, replica_id, *args, **kwargs):
        @lockutils.synchronized(
                REPLICA_LOCK_NAME_FORMAT % replica_id)
        def inner():
            return func(self, ctxt, replica_id, *args, **kwargs)
        return inner()
    return wrapper


def schedule_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, replica_id, schedule_id, *args, **kwargs):
        @lockutils.synchronized(
                SCHEDULE_LOCK_NAME_FORMAT % schedule_id)
        def inner():
            return func(self, ctxt, replica_id, schedule_id, *args, **kwargs)
        return inner()
    return wrapper


def task_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, task_id, *args, **kwargs):
        @lockutils.synchronized(
                TASK_LOCK_NAME_FORMAT % task_id)
        def inner():
            return func(self, ctxt, task_id, *args, **kwargs)
        return inner()
    return wrapper


def task_and_execution_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, task_id, *args, **kwargs):
        task = db_api.get_task(ctxt, task_id)
        @lockutils.synchronized(
                EXECUTION_LOCK_NAME_FORMAT % task.execution_id)
        @lockutils.synchronized(
                TASK_LOCK_NAME_FORMAT % task_id)
        def inner():
            return func(self, ctxt, task_id, *args, **kwargs)
        return inner()
    return wrapper


def migration_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, migration_id, *args, **kwargs):
        @lockutils.synchronized(
                MIGRATION_LOCK_NAME_FORMAT % migration_id)
        def inner():
            return func(self, ctxt, migration_id, *args, **kwargs)
        return inner()
    return wrapper


def tasks_execution_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, replica_id, execution_id, *args, **kwargs):
        @lockutils.synchronized(
                EXECUTION_LOCK_NAME_FORMAT % execution_id)
        def inner():
            return func(self, ctxt, replica_id, execution_id, *args, **kwargs)
        return inner()
    return wrapper


class ConductorServerEndpoint(object):
    def __init__(self):
        self._licensing_client = licensing_client.LicensingClient.from_env()
        self._rpc_worker_client = rpc_worker_client.WorkerClient()
        self._replica_cron_client = rpc_cron_client.ReplicaCronClient()

    def get_all_diagnostics(self, ctxt):
        conductor = self.get_diagnostics(ctxt)
        cron = self._replica_cron_client.get_diagnostics(ctxt)
        worker = self._rpc_worker_client.get_diagnostics(ctxt)
        return [
            conductor,
            cron,
            worker,
        ]

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

    def _check_reservation_for_transfer(self, transfer_action):
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
            self._licensing_client.check_reservation(reservation_id)
        else:
            LOG.debug(
                "Transfer action '%s' has no reservation ID set. Skipping "
                "all reservation licensing checks.", action_id)

    def create_endpoint(self, ctxt, name, endpoint_type, description,
                        connection_info):
        endpoint = models.Endpoint()
        endpoint.name = name
        endpoint.type = endpoint_type
        endpoint.description = description
        endpoint.connection_info = connection_info

        db_api.add_endpoint(ctxt, endpoint)
        LOG.info("Endpoint created: %s", endpoint.id)
        return self.get_endpoint(ctxt, endpoint.id)

    def update_endpoint(self, ctxt, endpoint_id, updated_values):
        db_api.update_endpoint(ctxt, endpoint_id, updated_values)
        LOG.info("Endpoint updated: %s", endpoint_id)
        return self.get_endpoint(ctxt, endpoint_id)

    def get_endpoints(self, ctxt):
        return db_api.get_endpoints(ctxt)

    @endpoint_synchronized
    def get_endpoint(self, ctxt, endpoint_id):
        endpoint = db_api.get_endpoint(ctxt, endpoint_id)
        if not endpoint:
            raise exception.NotFound("Endpoint not found")
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

        return self._rpc_worker_client.get_endpoint_instances(
            ctxt, endpoint.type, endpoint.connection_info,
            source_environment, marker, limit, instance_name_pattern)

    def get_endpoint_instance(
            self, ctxt, endpoint_id, source_environment, instance_name):
        endpoint = self.get_endpoint(ctxt, endpoint_id)

        return self._rpc_worker_client.get_endpoint_instance(
            ctxt, endpoint.type, endpoint.connection_info,
            source_environment, instance_name)

    def get_endpoint_source_options(
            self, ctxt, endpoint_id, env, option_names):
        endpoint = self.get_endpoint(ctxt, endpoint_id)

        return self._rpc_worker_client.get_endpoint_source_options(
            ctxt, endpoint.type, endpoint.connection_info, env, option_names)

    def get_endpoint_destination_options(
            self, ctxt, endpoint_id, env, option_names):
        endpoint = self.get_endpoint(ctxt, endpoint_id)

        return self._rpc_worker_client.get_endpoint_destination_options(
            ctxt, endpoint.type, endpoint.connection_info, env, option_names)

    def get_endpoint_networks(self, ctxt, endpoint_id, env):
        endpoint = self.get_endpoint(ctxt, endpoint_id)

        return self._rpc_worker_client.get_endpoint_networks(
            ctxt, endpoint.type, endpoint.connection_info, env)

    def get_endpoint_storage(self, ctxt, endpoint_id, env):
        endpoint = self.get_endpoint(ctxt, endpoint_id)

        return self._rpc_worker_client.get_endpoint_storage(
            ctxt, endpoint.type, endpoint.connection_info, env)

    def validate_endpoint_connection(self, ctxt, endpoint_id):
        endpoint = self.get_endpoint(ctxt, endpoint_id)
        return self._rpc_worker_client.validate_endpoint_connection(
            ctxt, endpoint.type, endpoint.connection_info)

    def validate_endpoint_target_environment(
            self, ctxt, endpoint_id, target_env):
        endpoint = self.get_endpoint(ctxt, endpoint_id)
        return self._rpc_worker_client.validate_endpoint_target_environment(
            ctxt, endpoint.type, target_env)

    def validate_endpoint_source_environment(
            self, ctxt, endpoint_id, source_env):
        endpoint = self.get_endpoint(ctxt, endpoint_id)
        return self._rpc_worker_client.validate_endpoint_source_environment(
            ctxt, endpoint.type, source_env)

    def get_available_providers(self, ctxt):
        return self._rpc_worker_client.get_available_providers(ctxt)

    def get_provider_schemas(self, ctxt, platform_name, provider_type):
        return self._rpc_worker_client.get_provider_schemas(
            ctxt, platform_name, provider_type)

    @staticmethod
    def _create_task(instance, task_type, execution, depends_on=None,
                     on_error=False, on_error_only=False):
        task = models.Task()
        task.id = str(uuid.uuid4())
        task.instance = instance
        task.execution = execution
        task.task_type = task_type
        task.depends_on = depends_on
        task.on_error = on_error
        task.index = len(task.execution.tasks) + 1

        # non-error tasks are automatically set to pending:
        if not on_error and not on_error_only:
            task.status = constants.TASK_STATUS_PENDING
        else:
            task.status = constants.TASK_STATUS_ON_ERROR_ONLY
            # on-error-only tasks remain marked as such regardless
            # of dependencies:
            if on_error_only:
                task.on_error = True
            # tasks which are on-error but depend on already-defined
            # pending tasks count as pending:
            elif depends_on:
                for task_id in depends_on:
                    if [t for t in task.execution.tasks if t.id == task_id and
                            t.status != constants.TASK_STATUS_ON_ERROR_ONLY]:
                        task.status = constants.TASK_STATUS_PENDING
                        break
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

    def _begin_tasks(self, ctxt, execution, task_info={}):
        if not ctxt.trust_id:
            keystone.create_trust(ctxt)
            ctxt.delete_trust_id = True

        origin = self._get_task_origin(ctxt, execution.action)
        destination = self._get_task_destination(ctxt, execution.action)

        for task in execution.tasks:
            if (not task.depends_on and
                    task.status == constants.TASK_STATUS_PENDING):
                db_api.set_task_status(
                    ctxt, task.id, constants.TASK_STATUS_SCHEDULED)
                self._rpc_worker_client.begin_task(
                    ctxt, server=None,
                    task_id=task.id,
                    task_type=task.task_type,
                    origin=origin,
                    destination=destination,
                    instance=task.instance,
                    task_info=task_info.get(task.instance, {}))

    @replica_synchronized
    def execute_replica_tasks(self, ctxt, replica_id, shutdown_instances):
        replica = self._get_replica(ctxt, replica_id)
        self._check_reservation_for_transfer(replica)
        self._check_replica_running_executions(ctxt, replica)
        execution = models.TasksExecution()
        execution.id = str(uuid.uuid4())
        execution.status = constants.EXECUTION_STATUS_RUNNING
        execution.action = replica
        execution.type = constants.EXECUTION_TYPE_REPLICA_EXECUTION

        for instance in execution.action.instances:
            validate_replica_source_inputs_task = self._create_task(
                instance,
                constants.TASK_TYPE_VALIDATE_REPLICA_SOURCE_INPUTS,
                execution)

            get_instance_info_task = self._create_task(
                instance,
                constants.TASK_TYPE_GET_INSTANCE_INFO,
                execution,
                depends_on=[validate_replica_source_inputs_task.id])

            validate_replica_destination_inputs_task = self._create_task(
                instance,
                constants.TASK_TYPE_VALIDATE_REPLICA_DESTINATION_INPUTS,
                execution,
                depends_on=[get_instance_info_task.id])

            depends_on = [
                validate_replica_source_inputs_task.id,
                validate_replica_destination_inputs_task.id]
            if shutdown_instances:
                shutdown_instance_task = self._create_task(
                    instance, constants.TASK_TYPE_SHUTDOWN_INSTANCE,
                    execution, depends_on=depends_on)
                depends_on = [shutdown_instance_task.id]

            deploy_replica_disks_task = self._create_task(
                instance, constants.TASK_TYPE_DEPLOY_REPLICA_DISKS,
                execution, depends_on=depends_on)

            deploy_replica_source_resources_task = self._create_task(
                instance,
                constants.TASK_TYPE_DEPLOY_REPLICA_SOURCE_RESOURCES,
                execution, depends_on=[deploy_replica_disks_task.id])

            deploy_replica_target_resources_task = self._create_task(
                instance,
                constants.TASK_TYPE_DEPLOY_REPLICA_TARGET_RESOURCES,
                execution, depends_on=[
                    deploy_replica_disks_task.id,
                    deploy_replica_source_resources_task.id])

            replicate_disks_task = self._create_task(
                instance, constants.TASK_TYPE_REPLICATE_DISKS,
                execution, depends_on=[
                    deploy_replica_source_resources_task.id,
                    deploy_replica_target_resources_task.id])

            delete_source_resources_task = self._create_task(
                instance,
                constants.TASK_TYPE_DELETE_REPLICA_SOURCE_RESOURCES,
                execution,
                depends_on=[replicate_disks_task.id],
                on_error=True)

            self._create_task(
                instance,
                constants.TASK_TYPE_DELETE_REPLICA_TARGET_RESOURCES,
                execution, depends_on=[
                    replicate_disks_task.id, delete_source_resources_task.id],
                on_error=True)

        db_api.add_replica_tasks_execution(ctxt, execution)
        LOG.info("Replica tasks execution created: %s", execution.id)

        self._begin_tasks(ctxt, execution, replica.info)
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
                "Replica '%s' has no running execution." % replica_id)
        if execution.status == constants.EXECUTION_STATUS_CANCELLING and (
                not force):
            raise exception.InvalidReplicaState(
                "Replica '%s' is already being cancelled. Please use the "
                "force option if you'd like to force-cancel it." % (
                    replica_id))
        self._cancel_tasks_execution(ctxt, execution, force)

    def _get_replica_tasks_execution(self, ctxt, replica_id, execution_id):
        execution = db_api.get_replica_tasks_execution(
            ctxt, replica_id, execution_id)
        if not execution:
            raise exception.NotFound("Tasks execution not found")
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
        execution.status = constants.EXECUTION_STATUS_RUNNING
        execution.action = replica
        execution.type = constants.EXECUTION_TYPE_REPLICA_DISKS_DELETE

        has_tasks = False
        for instance in replica.instances:
            if (instance in replica.info and
                    "volumes_info" in replica.info[instance]):
                self._create_task(
                    instance, constants.TASK_TYPE_DELETE_REPLICA_DISKS,
                    execution)
                has_tasks = True

        if not has_tasks:
            raise exception.InvalidReplicaState(
                "This replica does not have volumes information for any "
                "instance. Ensure that the replica has been executed "
                "successfully priorly")

        db_api.add_replica_tasks_execution(ctxt, execution)
        LOG.info("Replica tasks execution created: %s", execution.id)

        self._begin_tasks(ctxt, execution, replica.info)
        return self.get_replica_tasks_execution(ctxt, replica_id, execution.id)

    @staticmethod
    def _check_endpoints(ctxt, origin_endpoint, destination_endpoint):
        # TODO(alexpilotti): check Barbican secrets content as well
        if (origin_endpoint.connection_info ==
                destination_endpoint.connection_info):
            raise exception.SameDestination()

    def create_instances_replica(self, ctxt, origin_endpoint_id,
                                 destination_endpoint_id, source_environment,
                                 destination_environment, instances,
                                 network_map, storage_mappings, notes=None):
        origin_endpoint = self.get_endpoint(ctxt, origin_endpoint_id)
        destination_endpoint = self.get_endpoint(ctxt, destination_endpoint_id)
        self._check_endpoints(ctxt, origin_endpoint, destination_endpoint)

        replica = models.Replica()
        replica.id = str(uuid.uuid4())
        replica.origin_endpoint = origin_endpoint
        replica.destination_endpoint = destination_endpoint
        replica.destination_environment = destination_environment
        replica.source_environment = source_environment
        replica.instances = instances
        replica.executions = []
        replica.info = {}
        replica.notes = notes
        replica.network_map = network_map
        replica.storage_mappings = storage_mappings

        self._check_create_reservation_for_transfer(
            replica, licensing_client.RESERVATION_TYPE_REPLICA)

        db_api.add_replica(ctxt, replica)
        LOG.info("Replica created: %s", replica.id)
        return self.get_replica(ctxt, replica.id)

    def _get_replica(self, ctxt, replica_id):
        replica = db_api.get_replica(ctxt, replica_id)
        if not replica:
            raise exception.NotFound("Replica not found")
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
        if [m.id for m in migrations if m.executions[0].status ==
                constants.EXECUTION_STATUS_RUNNING]:
            raise exception.InvalidReplicaState(
                "This replica is currently being migrated")

    @staticmethod
    def _check_running_executions(action):
        if [e for e in action.executions
                if e.status == constants.EXECUTION_STATUS_RUNNING]:
            raise exception.InvalidActionTasksExecutionState(
                "Another tasks execution is in progress")

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
    def deploy_replica_instances(self, ctxt, replica_id, clone_disks, force,
                                 skip_os_morphing=False, user_scripts=None):
        replica = self._get_replica(ctxt, replica_id)
        self._check_reservation_for_transfer(replica)
        self._check_replica_running_executions(ctxt, replica)
        self._check_valid_replica_tasks_execution(replica, force)

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
        migration.origin_endpoint_id = replica.origin_endpoint_id
        migration.destination_endpoint_id = replica.destination_endpoint_id
        migration.destination_environment = replica.destination_environment
        migration.source_environment = replica.source_environment
        migration.network_map = replica.network_map
        migration.storage_mappings = replica.storage_mappings
        migration.instances = instances
        migration.replica = replica
        migration.info = replica.info

        for instance in instances:
            migration.info[instance]["clone_disks"] = clone_disks
            scripts = self._get_instance_scripts(user_scripts, instance)
            migration.info[instance]["user_scripts"] = scripts

        execution = models.TasksExecution()
        migration.executions = [execution]
        execution.status = constants.EXECUTION_STATUS_RUNNING
        execution.number = 1
        execution.type = constants.EXECUTION_TYPE_REPLICA_DEPLOY

        for instance in instances:
            validate_replica_desployment_inputs_task = self._create_task(
                instance,
                constants.TASK_TYPE_VALIDATE_REPLICA_DEPLOYMENT_INPUTS,
                execution)

            create_snapshot_task_depends_on = [
                validate_replica_desployment_inputs_task.id]

            if (constants.PROVIDER_TYPE_INSTANCE_FLAVOR in
                    destination_provider_types):
                get_optimal_flavor_task = self._create_task(
                    instance, constants.TASK_TYPE_GET_OPTIMAL_FLAVOR,
                    execution, depends_on=[
                        validate_replica_desployment_inputs_task.id])
                create_snapshot_task_depends_on.append(
                    get_optimal_flavor_task.id)

            create_snapshot_task = self._create_task(
                instance, constants.TASK_TYPE_CREATE_REPLICA_DISK_SNAPSHOTS,
                execution, depends_on=create_snapshot_task_depends_on)

            deploy_replica_task = self._create_task(
                instance, constants.TASK_TYPE_DEPLOY_REPLICA_INSTANCE,
                execution, [create_snapshot_task.id])

            if not skip_os_morphing:
                task_deploy_os_morphing_resources = self._create_task(
                    instance, constants.TASK_TYPE_DEPLOY_OS_MORPHING_RESOURCES,
                    execution, depends_on=[deploy_replica_task.id])

                task_osmorphing = self._create_task(
                    instance, constants.TASK_TYPE_OS_MORPHING,
                    execution, depends_on=[
                        task_deploy_os_morphing_resources.id])

                task_delete_os_morphing_resources = self._create_task(
                    instance, constants.TASK_TYPE_DELETE_OS_MORPHING_RESOURCES,
                    execution, depends_on=[task_osmorphing.id],
                    on_error=True)

                next_task = task_delete_os_morphing_resources
            else:
                next_task = deploy_replica_task

            finalize_deployment_task = self._create_task(
                instance,
                constants.TASK_TYPE_FINALIZE_REPLICA_INSTANCE_DEPLOYMENT,
                execution,
                depends_on=[next_task.id])

            self._create_task(
                instance, constants.TASK_TYPE_DELETE_REPLICA_DISK_SNAPSHOTS,
                execution, depends_on=[finalize_deployment_task.id],
                on_error=clone_disks)

            cleanup_deployment_task = self._create_task(
                instance,
                constants.TASK_TYPE_CLEANUP_FAILED_REPLICA_INSTANCE_DEPLOYMENT,
                execution, on_error_only=True)

            if not clone_disks:
                self._create_task(
                    instance,
                    constants.TASK_TYPE_RESTORE_REPLICA_DISK_SNAPSHOTS,
                    execution,
                    depends_on=[cleanup_deployment_task.id],
                    on_error=True)

        db_api.add_migration(ctxt, migration)
        LOG.info("Migration created: %s", migration.id)

        self._begin_tasks(ctxt, execution, migration.info)

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

    def migrate_instances(self, ctxt, origin_endpoint_id,
                          destination_endpoint_id, source_environment,
                          destination_environment, instances, network_map,
                          storage_mappings, replication_count,
                          shutdown_instances=False, notes=None,
                          skip_os_morphing=False, user_scripts=None):
        origin_endpoint = self.get_endpoint(ctxt, origin_endpoint_id)
        destination_endpoint = self.get_endpoint(ctxt, destination_endpoint_id)
        self._check_endpoints(ctxt, origin_endpoint, destination_endpoint)

        destination_provider_types = self._get_provider_types(
            ctxt, destination_endpoint)

        migration = models.Migration()
        migration.id = str(uuid.uuid4())
        migration.origin_endpoint = origin_endpoint
        migration.destination_endpoint = destination_endpoint
        migration.destination_environment = destination_environment
        migration.source_environment = source_environment
        migration.network_map = network_map
        migration.storage_mappings = storage_mappings
        execution = models.TasksExecution()
        execution.status = constants.EXECUTION_STATUS_RUNNING
        execution.number = 1
        execution.type = constants.EXECUTION_TYPE_MIGRATION
        migration.executions = [execution]
        migration.instances = instances
        migration.info = {}
        migration.notes = notes
        migration.shutdown_instances = shutdown_instances
        migration.replication_count = replication_count

        self._check_create_reservation_for_transfer(
            migration, licensing_client.RESERVATION_TYPE_MIGRATION)

        for instance in instances:
            # NOTE: we must explicitly set this in each VM's info
            # to prevent the Replica disks from being cloned:
            migration.info[instance] = {"clone_disks": False}
            scripts = self._get_instance_scripts(user_scripts, instance)
            migration.info[instance]["user_scripts"] = scripts

            validate_migration_source_inputs_task = self._create_task(
                instance,
                constants.TASK_TYPE_VALIDATE_MIGRATION_SOURCE_INPUTS,
                execution)

            get_instance_info_task = self._create_task(
                instance,
                constants.TASK_TYPE_GET_INSTANCE_INFO,
                execution,
                depends_on=[validate_migration_source_inputs_task.id])

            validate_migration_destination_inputs_task = self._create_task(
                instance,
                constants.TASK_TYPE_VALIDATE_MIGRATION_DESTINATION_INPUTS,
                execution,
                depends_on=[get_instance_info_task.id])

            depends_on = [
                validate_migration_source_inputs_task.id,
                validate_migration_destination_inputs_task.id]

            create_instance_disks_task = self._create_task(
                instance, constants.TASK_TYPE_CREATE_INSTANCE_DISKS,
                execution, depends_on=depends_on)

            deploy_migration_source_resources_task = self._create_task(
                instance,
                constants.TASK_TYPE_DEPLOY_MIGRATION_SOURCE_RESOURCES,
                execution, depends_on=[create_instance_disks_task.id])

            deploy_migration_target_resources_task = self._create_task(
                instance,
                constants.TASK_TYPE_DEPLOY_MIGRATION_TARGET_RESOURCES,
                execution, depends_on=[
                    create_instance_disks_task.id,
                    deploy_migration_source_resources_task.id])

            # NOTE(aznashwan): re-executing the REPLICATE_DISKS task only works
            # if all the source disk snapshotting and worker setup steps are
            # performed by the source plugin in REPLICATE_DISKS.
            # This should no longer be a problem when worker pooling lands.
            # Alternatively, if the DEPLOY_REPLICA_SOURCE/DEST_RESOURCES tasks
            # will no longer have a state conflict, iterating through and
            # re-executing DEPLOY_REPLICA_SOURCE_RESOURCES will be required:
            last_migration_task = None
            migration_resources_tasks = [
                deploy_migration_source_resources_task.id,
                deploy_migration_target_resources_task.id]
            for i in range(migration.replication_count):
                # insert SHUTDOWN_INSTANCES task before the last sync:
                if i == (migration.replication_count - 1) and (
                        migration.shutdown_instances):
                    shutdown_deps = migration_resources_tasks
                    if last_migration_task:
                        shutdown_deps = [last_migration_task.id]
                    last_migration_task = self._create_task(
                        instance, constants.TASK_TYPE_SHUTDOWN_INSTANCE,
                        execution, depends_on=shutdown_deps)

                replication_deps = migration_resources_tasks
                if last_migration_task:
                    replication_deps = [last_migration_task.id]

                last_migration_task = self._create_task(
                    instance, constants.TASK_TYPE_REPLICATE_DISKS,
                    execution, depends_on=replication_deps)

            delete_source_resources_task = self._create_task(
                instance,
                constants.TASK_TYPE_DELETE_MIGRATION_SOURCE_RESOURCES,
                execution, depends_on=[last_migration_task.id],
                on_error=True)

            delete_destination_resources_task = self._create_task(
                instance,
                constants.TASK_TYPE_DELETE_MIGRATION_TARGET_RESOURCES,
                execution, depends_on=[
                    last_migration_task.id,
                    delete_source_resources_task.id],
                on_error=True)

            deploy_instance_task = self._create_task(
                instance, constants.TASK_TYPE_DEPLOY_INSTANCE_RESOURCES,
                execution, depends_on=[
                    delete_source_resources_task.id,
                    delete_destination_resources_task.id])

            last_task = deploy_instance_task
            if not skip_os_morphing:
                task_deploy_os_morphing_resources = self._create_task(
                    instance, constants.TASK_TYPE_DEPLOY_OS_MORPHING_RESOURCES,
                    execution, depends_on=[last_task.id])

                task_osmorphing = self._create_task(
                    instance, constants.TASK_TYPE_OS_MORPHING,
                    execution, depends_on=[
                        task_deploy_os_morphing_resources.id])

                task_delete_os_morphing_resources = self._create_task(
                    instance, constants.TASK_TYPE_DELETE_OS_MORPHING_RESOURCES,
                    execution, depends_on=[task_osmorphing.id],
                    on_error=True)

                last_task = task_delete_os_morphing_resources

            if (constants.PROVIDER_TYPE_INSTANCE_FLAVOR in
                    destination_provider_types):
                get_optimal_flavor_task = self._create_task(
                    instance, constants.TASK_TYPE_GET_OPTIMAL_FLAVOR,
                    execution, depends_on=[last_task.id])
                last_task = get_optimal_flavor_task

            finalize_deployment_task = self._create_task(
                instance,
                constants.TASK_TYPE_FINALIZE_INSTANCE_DEPLOYMENT,
                execution, depends_on=[last_task.id])

            cleanup_failed_deployment_task = self._create_task(
                instance,
                constants.TASK_TYPE_CLEANUP_FAILED_INSTANCE_DEPLOYMENT,
                execution, depends_on=[finalize_deployment_task.id],
                on_error_only=True)

            self._create_task(
                instance, constants.TASK_TYPE_CLEANUP_INSTANCE_STORAGE,
                execution, depends_on=[
                    create_instance_disks_task.id,
                    cleanup_failed_deployment_task.id],
                on_error_only=True)

        db_api.add_migration(ctxt, migration)
        LOG.info("Migration created: %s", migration.id)

        self._begin_tasks(ctxt, execution)

        return self.get_migration(ctxt, migration.id)

    def _get_migration(self, ctxt, migration_id):
        migration = db_api.get_migration(ctxt, migration_id)
        if not migration:
            raise exception.NotFound("Migration not found")
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
        execution = migration.executions[0]
        if execution.status not in constants.ACTIVE_EXECUTION_STATUSES:
            raise exception.InvalidMigrationState(
                "Migration '%s' is not running" % migration_id)
        if execution.status == constants.EXECUTION_STATUS_CANCELLING and (
                not force):
            raise exception.InvalidMigrationState(
                "Migration '%s' is already being cancelled. Please use the "
                "force option if you'd like to force-cancel it.")
        self._cancel_tasks_execution(ctxt, execution, force)
        self._check_delete_reservation_for_transfer(migration)

    def _cancel_tasks_execution(self, ctxt, execution, force=False):
        """ Cancels a running Execution by:
        - telling workers to kill any already running non-on-error tasks
        - cancelling any non-on-error tasks which are pending
        - triggering any on-error-only tasks with no dependencies

        NOTE: affects whole execution, only call this
        with a lock on the Execution as a whole!
        """
        LOG.debug(
            "Cancelling tasks execution %s. Current status before "
            "cancellation is '%s'", execution.id, execution.status)

        # mark execution as cancelling:
        self._set_tasks_execution_status(
            ctxt, execution.id, constants.EXECUTION_STATUS_CANCELLING)
        # iterate through and kill/cancel any non-error
        # tasks which are running/pending:
        for task in execution.tasks:
            if force and task.status == constants.TASK_STATUS_CANCELLING:
                LOG.warn(
                    "Task '%s' is in %s state, but forcibly setting to "
                    "'%s' because 'force' flag was provided",
                    task.id, task.status,
                    constants.TASK_STATUS_FORCE_CANCELED)
                db_api.set_task_status(
                    ctxt, task.id, constants.TASK_STATUS_FORCE_CANCELED)
                continue

            if not task.on_error:
                if task.status in (
                        constants.TASK_STATUS_RUNNING,
                        constants.TASK_STATUS_SCHEDULED):
                    LOG.debug(
                        "Killing %s task '%s' as part of "
                        "cancellation of execution '%s'",
                        task.status, task.id, execution.id)
                    db_api.set_task_status(
                        ctxt, task.id, constants.TASK_STATUS_CANCELLING)
                    self._rpc_worker_client.cancel_task(
                        ctxt, task.host, task.id, task.process_id, force)
                # cancel any non-on-error tasks which are aleady pending:
                elif task.status == constants.TASK_STATUS_PENDING:
                    LOG.debug(
                        "Marking pending task '%s' as cancelled as "
                        "part of cancellation of execution '%s'",
                        task.id, execution.id)
                    db_api.set_task_status(
                        ctxt, task.id, constants.TASK_STATUS_CANCELED)
            # NOTE: ideally, all on-error-only tasks should have a
            # clear-cut dependency task so this inefficient code
            # should never be called:
            elif task.on_error and (
                    task.status == constants.TASK_STATUS_ON_ERROR_ONLY and not (
                        task.depends_on)):
                try:
                    origin = self._get_task_origin(ctxt, execution.action)
                    destination = self._get_task_destination(
                        ctxt, execution.action)
                except exception.NotFound as ex:
                    LOG.error("A required endpoint could not be found")
                    LOG.exception(ex)

                action = db_api.get_action(
                    ctxt, execution.action_id)
                db_api.set_task_status(
                    ctxt, task.id, constants.TASK_STATUS_SCHEDULED)
                self._rpc_worker_client.begin_task(
                    ctxt, server=None,
                    task_id=task.id,
                    task_type=task.task_type,
                    origin=origin,
                    destination=destination,
                    instance=task.instance,
                    task_info=action.info.get(task.instance, {}))
            else:
                LOG.debug(
                    "No action currently taken with respect to task '%s' "
                    "during cancellation of execution '%s'",
                    task.id, execution.id)

        self._advance_execution_state(ctxt, execution)

    @staticmethod
    def _set_tasks_execution_status(ctxt, execution_id, execution_status):
        LOG.info(
            "Tasks execution %(id)s status updated to: %(status)s",
            {"id": execution_id, "status": execution_status})
        db_api.set_execution_status(ctxt, execution_id, execution_status)
        if ctxt.delete_trust_id:
            keystone.delete_trust(ctxt)

    @task_synchronized
    def set_task_host(self, ctxt, task_id, host, process_id):
        """ Saves the ID of the worker host which has accepted and started
        the task to the DB and marks the task as 'RUNNING'. """
        task = db_api.get_task(ctxt, task_id)
        if task.status != constants.TASK_STATUS_SCHEDULED:
            raise exception.InvalidTaskState(
                "Task with ID '%s' is in '%s' status instead of the "
                "expected '%s' required for it to be executed." % (
                    task_id, task.status, constants.TASK_STATUS_SCHEDULED))
        db_api.set_task_host(ctxt, task_id, host, process_id)
        db_api.set_task_status(
            ctxt, task_id, constants.TASK_STATUS_RUNNING)

    def _check_clean_execution_deadlock(self, ctxt, execution):
        """ Checks whether an execution is deadlocked.
        Deadlocked execution have no currently running/scheduled tasks
        but some remaining pending tasks.
        If this occurs, all pending tasks are marked as deadlocked,
        and the execution is marked as ERROR'd.
        Returns the state of the when the check occured
        (either RUNNING or DEADLOCKED)
        """
        execution = db_api.get_tasks_execution(ctxt, execution.id)
        task_statuses = {}
        for task in execution.tasks:
            dbtask = db_api.get_task(ctxt, task.id)
            task_statuses[dbtask.id] = dbtask.status

        determined_state = constants.EXECUTION_STATUS_RUNNING
        status_vals = task_statuses.values()
        if constants.TASK_STATUS_PENDING in status_vals and (
                constants.TASK_STATUS_RUNNING not in status_vals or (
                    constants.TASK_STATUS_SCHEDULED not in status_vals)):
            LOG.warn(
                "Execution '%s' is deadlocked. Task statuses are: %s",
                execution.id, task_statuses)
            for task_id, stat in task_statuses.items():
                if stat == constants.TASK_STATUS_PENDING:
                    LOG.warn("Marking deadlocked task '%s' as that ", task_id)
                    db_api.set_task_status(
                        ctxt, task_id,
                        constants.TASK_STATUS_CANCELED_FROM_DEADLOCK,
                        exception_details=TASK_DEADLOCK_ERROR_MESSAGE)
            LOG.warn(
                "Marking deadlocked execution '%s' as ERROR'd", execution.id)
            self._set_tasks_execution_status(
                ctxt, execution.id, constants.EXECUTION_STATUS_ERROR)
            LOG.error(
                "Execution '%s' is deadlocked. Cleanup has been performed. "
                "Task statuses at time of deadlock were: %s",
                execution.id, task_statuses)
            determined_state = constants.EXECUTION_STATUS_DEADLOCKED
        return determined_state

    def _get_execution_status(self, ctxt, execution, requery=False):
        """ Returns the global status of an execution.
        RUNNING - at least one task is RUNNING, SCHEDULED or CANCELLING
        COMPLETED - all non-error-only tasks are COMPLETED
        CANCELED - no more RUNNING or PENDING tasks but some CANCELED
        ERROR - no tasks are RUNNING and at least one is ERROR'd
        DEADLOCKED - has PENDING tasks but none RUNNING/SCHEDULED/CANCELLING
        """
        is_running = False
        is_canceled = False
        is_cancelling = False
        is_errord = False
        has_pending_tasks = False
        task_stat_map = {}
        if requery:
            execution = db_api.get_tasks_execution(ctxt, execution.id)
        for task in execution.tasks:
            task_stat_map[task.id] = task.status
            if task.status in constants.ACTIVE_TASK_STATUSES:
                is_running = True
            if task.status in constants.CANCELED_TASK_STATUSES:
                is_canceled = True
            if task.status == constants.TASK_STATUS_ERROR:
                is_errord = True
            if task.status == constants.TASK_STATUS_CANCELLING:
                is_cancelling = True
            if task.status == constants.TASK_STATUS_PENDING:
                has_pending_tasks = True

        status = constants.EXECUTION_STATUS_COMPLETED
        if has_pending_tasks and not is_running:
            status = constants.EXECUTION_STATUS_DEADLOCKED
        elif is_cancelling:
            status = constants.EXECUTION_STATUS_CANCELLING
        elif is_running:
            status = constants.EXECUTION_STATUS_RUNNING
        elif is_errord:
            status = constants.EXECUTION_STATUS_ERROR
        # NOTE: canceled executions should never have ERROR'd tasks
        # (they should also be CANCELED) so this comes last:
        elif is_canceled:
            status = constants.EXECUTION_STATUS_CANCELED

        LOG.debug(
            "Overall status for Execution '%s' determined to be '%s'."
            "Task statuses at time of decision: %s",
            execution.id, status, task_stat_map)
        return status

    def _advance_execution_state(self, ctxt, execution):
        """ Advances the state of the execution by starting/refreshing
        the state of all child tasks.
        If the task has finalized (either completed or error'd), updates
        its state to the finalized one.
        Returns a list of all the tasks which were started.
        NOTE: should only be called with a lock on the Execution!

        Requirements for a tasks to be started:
        - any task where any parent task got CANCELED will also be CANCELED
        - normal tasks (task.on_error==False & task.status==PENDING):
            * instantly started if they have no parent dependencies
            * all parent dependency tasks must be COMPLETED
        - on-error tasks (task.on_error==True):
            * all parent tasks must be COMPLETED or ERROR'd
        - on-error-only tasks (task.status==ON_ERROR_ONLY):
            * at least one non-error parent tasks must have been COMPLETED
            * all non-error parent tasks must have reached a terminal state
            * all on-error parent tasks must have reached a terminal state
        """
        LOG.debug(
            "State of execution '%s' before state advancement is: %s",
            execution.id, execution.status)

        started_tasks = []

        origin = self._get_task_origin(ctxt, execution.action)
        destination = self._get_task_destination(ctxt, execution.action)
        action = db_api.get_action(ctxt, execution.action_id)

        def _start_task(task):
            db_api.set_task_status(
                ctxt, task.id, constants.TASK_STATUS_SCHEDULED)
            self._rpc_worker_client.begin_task(
                ctxt, server=None,
                task_id=task.id,
                task_type=task.task_type,
                origin=origin,
                destination=destination,
                instance=task.instance,
                task_info=action.info.get(task.instance, {}))
            started_tasks.append(task.id)

        for task in execution.tasks:
            if task.status == constants.TASK_STATUS_PENDING:
                # immediately start pending tasks with no deps:
                if not task.depends_on:
                    LOG.info(
                        "Starting depency-less task '%s'", task.id)
                    _start_task(task)
                    continue

                # gather statuses of dependent tasks:
                parent_task_statuses = {}
                if task.depends_on:
                    for dep_task_id in task.depends_on:
                        depend_task = db_api.get_task(ctxt, dep_task_id)
                        parent_task_statuses[dep_task_id] = depend_task.status

                # immediately cancel any task whose parent task(s)
                # got canceled (including on_error=True tasks):
                if any([
                        dep_stat in constants.CANCELED_TASK_STATUSES or (
                            dep_stat == constants.TASK_STATUS_CANCELLING)
                        for dep_stat in parent_task_statuses.values()]):
                    LOG.info(
                        "Marking task '%s' as cancelled since it has one or "
                        "more parent tasks which got canceled: %s",
                        task.id, parent_task_statuses)
                    db_api.set_task_status(
                        ctxt, task.id, constants.TASK_STATUS_CANCELED)
                    continue

                if not task.on_error:
                    # start all non-error tasks whose parent tasks have completed:
                    if all([
                            dep_stat == constants.TASK_STATUS_COMPLETED
                            for dep_stat in parent_task_statuses.values()]):
                        LOG.info(
                            "Starting task '%s' as all dependencies have "
                            "completed: %s", task.id, parent_task_statuses)
                        _start_task(task)
                    else:
                        LOG.debug(
                            "Skipping starting task '%s' as some parent "
                            "tasks have not completed: %s",
                            task.id, parent_task_statuses)
                    continue

                elif task.on_error:
                    # start all on-error tasks whose parents have
                    # either completed or error'd:
                    if all([
                            dep_stat in (
                                constants.TASK_STATUS_COMPLETED,
                                constants.TASK_STATUS_ERROR)
                            for dep_stat in parent_task_statuses.values()]):
                        LOG.info(
                            "Starting task '%s' as all dependencies have "
                            "completed: %s", task.id, parent_task_statuses)
                        _start_task(task)
                    else:
                        LOG.debug(
                            "Skipping starting on-error task '%s' as some "
                            "parent tasks have not completed: %s",
                            task.id, parent_task_statuses)
                    continue
                LOG.debug(
                    "No lifecycle decision was taken for pending task '%s'. "
                    "Current status: %s. Parent tasks: %s",
                    task.id, task.status, parent_task_statuses)

            elif task.status == constants.TASK_STATUS_ON_ERROR_ONLY:
                if not task.depends_on:
                    LOG.warn(
                        "Encountered on-error-only task '%s' with no "
                        "dependencies. These types of tasks should ideally "
                        "always be declared with dependencies.", task.id)
                    # skipping as it was `_cancel_task_execution`'s job
                    # to trigger such tasks.
                    continue

                error_deps = {}
                non_error_deps = {}
                # gather statuses for non-error and on-error parents:
                for dep_task_id in task.depends_on:
                    depend_task = db_api.get_task(ctxt, dep_task_id)
                    if depend_task.on_error:
                        error_deps[depend_task.id] = depend_task.status
                    else:
                        non_error_deps[depend_task.id] = depend_task.status

                # do not trigger on-error-only tasks if not all non-error
                # parent tasks have reached a final state:
                if not all([
                        dep_stat in constants.FINALIZED_TASK_STATUSES
                        for dep_stat in non_error_deps.values()]):
                    LOG.debug(
                        "Not triggering on-error-only task '%s' as some "
                        "of its parent non-error tasks have not reached a "
                        "terminal status: %s", task.id, non_error_deps)
                    continue

                # cancel any on-error-only task if no non-on-error parent
                # tasks completed successfully:
                if constants.TASK_STATUS_COMPLETED not in (
                        non_error_deps.values()):
                    LOG.info(
                        "Marking on-error-only task '%s' as canceled as all of "
                        "its parent non-error tasks have finalized but none "
                        "has completed successfully: %s",
                        task.id, non_error_deps)
                    db_api.set_task_status(
                        ctxt, task.id, constants.TASK_STATUS_CANCELED)
                    continue

                # do not trigger on-error-only tasks unless all on-error
                # parent tasks have reached a final state:
                if not all([
                        dep_stat in constants.FINALIZED_TASK_STATUSES
                        for dep_stat in error_deps.values()]):
                    LOG.debug(
                        "Not triggering on-error-only task '%s' as it "
                        "has parent on-error tasks which didn't "
                        "complete/error: %s", task.id, error_deps)
                    continue

                LOG.debug("Starting on-error-only task '%s'", task.id)
                _start_task(task)
                continue

        if started_tasks:
            LOG.info(
                "Started the following tasks for execution '%s': %s",
                execution.id, started_tasks)
        else:
            # check for deadlock:
            if self._check_clean_execution_deadlock(ctxt, execution) == (
                    constants.EXECUTION_STATUS_DEADLOCKED):
                LOG.error(
                    "Execution '%s' deadlocked after Replica state advancement"
                    ". Cleanup has been perfomed. Returning early.")
                return []

        # check if execution status has changed:
        latest_execution_status = self._get_execution_status(
            ctxt, execution, requery=True)
        if latest_execution_status != execution.status:
            LOG.info(
                "Execution '%s' transitioned from status %s to %s",
                execution.id, execution.status, latest_execution_status)
            self._set_tasks_execution_status(
                ctxt, execution.id, latest_execution_status)
        else:
            LOG.debug(
                "Execution '%s' has remained in status '%s' following "
                "state advancement.", execution.id, latest_execution_status)

        return started_tasks

    def _update_replica_volumes_info(self, ctxt, replica_id, instance,
                                     updated_task_info):
        """ WARN: the lock for the Replica must be pre-acquired. """
        db_api.set_transfer_action_info(
            ctxt, replica_id, instance,
            updated_task_info)

    def _update_volumes_info_for_migration_parent_replica(
            self, ctxt, migration_id, instance, updated_task_info):
        migration = db_api.get_migration(ctxt, migration_id)
        replica_id = migration.replica_id

        with lockutils.lock(replica_id):
            LOG.debug(
                "Updating volume_info in replica due to snapshot "
                "restore during migration. replica id: %s", replica_id)
            self._update_replica_volumes_info(
                ctxt, replica_id, instance, updated_task_info)

    def _handle_post_task_actions(self, ctxt, task, execution, task_info):
        task_type = task.task_type

        if task_type == constants.TASK_TYPE_RESTORE_REPLICA_DISK_SNAPSHOTS:
            # When restoring a snapshot in some import providers (OpenStack),
            # a new volume_id is generated. This needs to be updated in the
            # Replica instance as well.
            volumes_info = task_info.get("volumes_info")
            if volumes_info:
                self._update_volumes_info_for_migration_parent_replica(
                    ctxt, execution.action_id, task.instance,
                    {"volumes_info": volumes_info})

        elif task_type == constants.TASK_TYPE_DELETE_REPLICA_DISK_SNAPSHOTS:

            if not task_info.get("clone_disks"):
                # The migration completed. If the replica is executed again,
                # new volumes need to be deployed in place of the migrated
                # ones.
                self._update_volumes_info_for_migration_parent_replica(
                    ctxt, execution.action_id, task.instance,
                    {"volumes_info": None})

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
            # NOTE: perform the actual db update on the Replica's properties:
            db_api.update_replica(ctxt, execution.action_id, task_info)
            # NOTE: remember to update the `volumes_info`:
            # NOTE: considering this method is only called with a lock on the
            # `execution.action_id` (in a Replica update tasks' case that's the
            # ID of the Replica itself) we can safely call
            # `_update_replica_volumes_info` below:
            self._update_replica_volumes_info(
                ctxt, execution.action_id, task.instance,
                {"volumes_info": task_info.get("volumes_info")})

    @task_and_execution_synchronized
    def task_completed(self, ctxt, task_id, task_info):
        LOG.info("Task completed: %s", task_id)

        db_api.set_task_status(
            ctxt, task_id, constants.TASK_STATUS_COMPLETED)

        task = db_api.get_task(ctxt, task_id)

        execution = db_api.get_tasks_execution(ctxt, task.execution_id)
        action_id = execution.action_id
        with lockutils.lock(action_id):
            LOG.info("Setting instance %(instance)s "
                     "action info: %(task_info)s",
                     {"instance": task.instance, "task_info": task_info})
            updated_task_info = db_api.set_transfer_action_info(
                ctxt, action_id, task.instance, task_info)

            self._handle_post_task_actions(
                ctxt, task, execution, updated_task_info)

            newly_started_tasks = self._advance_execution_state(
                ctxt, execution)
            if newly_started_tasks:
                LOG.info(
                    "The following tasks were started for execution '%s' "
                    "following the completion of task '%s': %s" % (
                        execution.id, task.id, newly_started_tasks))
            else:
                LOG.debug(
                    "No new tasks were started for execution '%s' following "
                    "the successful completion of task '%s'.",
                    execution.id, task.id)

    def _cancel_execution_for_osmorphing_debugging(self, ctxt, execution):
        # go through all scheduled tasks and cancel them:
        for subtask in execution.tasks:
            if subtask.task_type == constants.TASK_TYPE_OS_MORPHING:
                continue

            if subtask.status == constants.TASK_STATUS_RUNNING:
                raise exception.CoriolisException(
                    "Task %s is still running although it should not!" % (
                        subtask.id))

            if subtask.status in [
                    constants.TASK_STATUS_PENDING,
                    constants.TASK_STATUS_ON_ERROR_ONLY]:
                db_api.set_task_status(
                    ctxt, subtask.id,
                    constants.TASK_STATUS_CANCELED_FOR_DEBUGGING)

    @task_and_execution_synchronized
    def set_task_error(self, ctxt, task_id, exception_details):
        LOG.error("Task error: %(task_id)s - %(ex)s",
                  {"task_id": task_id, "ex": exception_details})

        task = db_api.get_task(ctxt, task_id)

        final_status = constants.TASK_STATUS_ERROR
        if task.status == constants.TASK_STATUS_CANCELLING:
            final_status = constants.TASK_STATUS_CANCELED
        elif task.status == constants.TASK_STATUS_FORCE_CANCELED:
            # it means a force cancel has been issued before the
            # confirmation that the task was canceled came in:
            LOG.warn(
                "Only just received error confirmation for force-cancelled "
                "task '%s'. Leaving marked as force-cancelled.", task.id)
            final_status = constants.TASK_STATUS_FORCE_CANCELED
        LOG.debug(
            "Transitioning errored task '%s' from '%s' to '%s'",
            task.id, task.status, final_status)
        db_api.set_task_status(
            ctxt, task_id, final_status, exception_details)

        task = db_api.get_task(ctxt, task_id)
        execution = db_api.get_tasks_execution(ctxt, task.execution_id)

        action_id = execution.action_id
        action = db_api.get_action(ctxt, action_id)
        with lockutils.lock(action_id):
            if task.task_type == constants.TASK_TYPE_OS_MORPHING and (
                    CONF.conductor.debug_os_morphing_errors):
                LOG.debug(
                    "Attempting to cancel execution '%s' of action '%s' "
                    "for OSMorphing debugging.", execution.id, action_id)
                # NOTE: the OSMorphing task always runs by itself so no
                # further tasks should be running, but we double-check here:
                running = [
                    t for t in execution.tasks
                    if t.status == constants.TASK_STATUS_RUNNING
                    and t.task_type != constants.TASK_TYPE_OS_MORPHING]
                if not running:
                    self._cancel_execution_for_osmorphing_debugging(
                        ctxt, execution)
                    LOG.warn(
                        "All subtasks for Migration '%s' have been cancelled "
                        "to allow for OSMorphing debugging. The connection "
                        "info for the worker VM is: %s",
                        action_id, action.info.get(task.instance, {}).get(
                            'osmorphing_connection_info', {}))
                    self._set_tasks_execution_status(
                        ctxt, execution.id, constants.EXECUTION_STATUS_ERROR)
                else:
                    LOG.warn(
                        "Some tasks are running in parallel with the "
                        "OSMorphing task, a debug setup cannot be safely "
                        "achieved. Proceeding with cleanup tasks as usual.")
                    self._cancel_tasks_execution(ctxt, execution)
            else:
                self._cancel_tasks_execution(ctxt, execution)

        # NOTE: if this is a migration, make sure to delete
        # its associated reservation.
        if action.type == constants.EXECUTION_TYPE_MIGRATION:
            self._check_delete_reservation_for_transfer(action)

    @task_synchronized
    def task_event(self, ctxt, task_id, level, message):
        LOG.info("Task event: %s", task_id)
        db_api.add_task_event(ctxt, task_id, level, message)

    @task_synchronized
    def task_progress_update(self, ctxt, task_id, current_step, total_steps,
                             message):
        LOG.info("Task progress update: %s", task_id)
        db_api.add_task_progress_update(ctxt, task_id, current_step,
                                        total_steps, message)

    def _get_replica_schedule(self, ctxt, replica_id,
                              schedule_id, expired=True):
        schedule = db_api.get_replica_schedule(
            ctxt, replica_id, schedule_id, expired=expired)
        if not schedule:
            raise exception.NotFound("Schedule not found")
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
        schedule = self._get_replica_schedule(
            ctxt, replica_id, schedule_id, expired=True)
        if not schedule:
            raise exception.NotFound("Schedule not found")
        return schedule

    @replica_synchronized
    def update_replica(
            self, ctxt, replica_id, properties):
        replica = self._get_replica(ctxt, replica_id)
        self._check_replica_running_executions(ctxt, replica)
        self._check_valid_replica_tasks_execution(replica, force=True)
        execution = models.TasksExecution()
        execution.id = str(uuid.uuid4())
        execution.status = constants.EXECUTION_STATUS_RUNNING
        execution.action = replica
        execution.type = constants.EXECUTION_TYPE_REPLICA_UPDATE

        LOG.debug(
            "Replica '%s' info pre-replica-update: %s",
            replica_id, replica.info)
        for instance in execution.action.instances:
            # NOTE: "circular assignment" would lead to a `None` value
            # so we must operate on a copy:
            inst_info_copy = copy.deepcopy(replica.info[instance])
            inst_info_copy.update(properties)
            replica.info[instance] = inst_info_copy
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
                    update_source_replica_task.id])
        LOG.debug(
            "Replica '%s' info post-replica-update: %s",
            replica_id, replica.info)
        db_api.add_replica_tasks_execution(ctxt, execution)
        LOG.debug("Execution for Replica update tasks created: %s",
                  execution.id)
        self._begin_tasks(ctxt, execution, replica.info)
        return self.get_replica_tasks_execution(ctxt, replica_id, execution.id)

    def get_diagnostics(self, ctxt):
        return utils.get_diagnostics_info()
