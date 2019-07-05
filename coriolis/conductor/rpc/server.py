# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import copy
import functools
import uuid

from oslo_concurrency import lockutils
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


def endpoint_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, endpoint_id, *args, **kwargs):
        @lockutils.synchronized(endpoint_id)
        def inner():
            return func(self, ctxt, endpoint_id, *args, **kwargs)
        return inner()
    return wrapper


def replica_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, replica_id, *args, **kwargs):
        @lockutils.synchronized(replica_id)
        def inner():
            return func(self, ctxt, replica_id, *args, **kwargs)
        return inner()
    return wrapper


def schedule_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, replica_id, schedule_id, *args, **kwargs):
        @lockutils.synchronized(schedule_id)
        def inner():
            return func(self, ctxt, replica_id, schedule_id, *args, **kwargs)
        return inner()
    return wrapper


def task_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, task_id, *args, **kwargs):
        @lockutils.synchronized(task_id)
        def inner():
            return func(self, ctxt, task_id, *args, **kwargs)
        return inner()
    return wrapper


def migration_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, migration_id, *args, **kwargs):
        @lockutils.synchronized(migration_id)
        def inner():
            return func(self, ctxt, migration_id, *args, **kwargs)
        return inner()
    return wrapper


def tasks_execution_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, replica_id, execution_id, *args, **kwargs):
        @lockutils.synchronized(execution_id)
        def inner():
            return func(self, ctxt, replica_id, execution_id, *args, **kwargs)
        return inner()
    return wrapper


class ConductorServerEndpoint(object):
    def __init__(self):
        self._licensing_client = licensing_client.LicensingClient.from_env()
        self._rpc_worker_client = rpc_worker_client.WorkerClient()
        self._replica_cron_client = rpc_cron_client.ReplicaCronClient()

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

    def get_endpoint_instances(self, ctxt, endpoint_id, marker, limit,
                               instance_name_pattern):
        endpoint = self.get_endpoint(ctxt, endpoint_id)

        return self._rpc_worker_client.get_endpoint_instances(
            ctxt, endpoint.type, endpoint.connection_info, marker, limit,
            instance_name_pattern)

    def get_endpoint_instance(self, ctxt, endpoint_id, instance_name):
        endpoint = self.get_endpoint(ctxt, endpoint_id)

        return self._rpc_worker_client.get_endpoint_instance(
            ctxt, endpoint.type, endpoint.connection_info, instance_name)

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
                     on_error=False):
        task = models.Task()
        task.id = str(uuid.uuid4())
        task.instance = instance
        task.execution = execution
        task.task_type = task_type
        task.depends_on = depends_on
        task.on_error = on_error

        if not on_error:
            task.status = constants.TASK_STATUS_PENDING
        else:
            task.status = constants.TASK_STATUS_ON_ERROR_ONLY
            if depends_on:
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

        for instance in execution.action.instances:
            validate_replica_inputs_task = self._create_task(
                instance, constants.TASK_TYPE_VALIDATE_REPLICA_INPUTS,
                execution)

            get_instance_info_task = self._create_task(
                instance, constants.TASK_TYPE_GET_INSTANCE_INFO,
                execution, depends_on=[validate_replica_inputs_task.id])

            depends_on = [get_instance_info_task.id]
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
                execution, depends_on=[deploy_replica_disks_task.id])

            replicate_disks_task = self._create_task(
                instance, constants.TASK_TYPE_REPLICATE_DISKS,
                execution, depends_on=[
                    deploy_replica_source_resources_task.id,
                    deploy_replica_target_resources_task.id])

            self._create_task(
                instance,
                constants.TASK_TYPE_DELETE_REPLICA_SOURCE_RESOURCES,
                execution, depends_on=[replicate_disks_task.id],
                on_error=True)

            self._create_task(
                instance,
                constants.TASK_TYPE_DELETE_REPLICA_TARGET_RESOURCES,
                execution, depends_on=[replicate_disks_task.id],
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
        if execution.status == constants.EXECUTION_STATUS_RUNNING:
            raise exception.InvalidMigrationState(
                "Cannot delete a running replica tasks execution")
        db_api.delete_replica_tasks_execution(ctxt, execution_id)

    @tasks_execution_synchronized
    def cancel_replica_tasks_execution(self, ctxt, replica_id, execution_id,
                                       force):
        execution = self._get_replica_tasks_execution(
            ctxt, replica_id, execution_id)
        if execution.status != constants.EXECUTION_STATUS_RUNNING:
            raise exception.InvalidReplicaState(
                "The replica tasks execution is not running")
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

    def get_migrations(self, ctxt, include_tasks):
        return db_api.get_migrations(ctxt, include_tasks)

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

        if (force and sorted_executions[0].status !=
                constants.EXECUTION_STATUS_COMPLETED):
            raise exception.InvalidReplicaState(
                "The last replica tasks execution was not successful. "
                "Perform a forced migration if you wish to perform a "
                "migration without a successful last replica execution")
        elif not [e for e in sorted_executions
                  if e.status == constants.EXECUTION_STATUS_COMPLETED]:
            raise exception.InvalidReplicaState(
                "A replica must have been executed succesfully in order "
                "to be migrated")

    def _get_provider_types(self, ctxt, endpoint):
        provider_types = self.get_available_providers(ctxt).get(endpoint.type)
        if provider_types is None:
            raise exception.NotFound(
                "No provider found for: %s" % endpoint.type)
        return provider_types["types"]

    @replica_synchronized
    def deploy_replica_instances(self, ctxt, replica_id, clone_disks, force,
                                 skip_os_morphing=False):
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

        execution = models.TasksExecution()
        migration.executions = [execution]
        execution.status = constants.EXECUTION_STATUS_RUNNING
        execution.number = 1

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
                execution, depends_on=[next_task.id])

            self._create_task(
                instance, constants.TASK_TYPE_DELETE_REPLICA_DISK_SNAPSHOTS,
                execution, depends_on=[finalize_deployment_task.id],
                on_error=clone_disks)

            cleanup_deployment_task = self._create_task(
                instance,
                constants.TASK_TYPE_CLEANUP_FAILED_REPLICA_INSTANCE_DEPLOYMENT,
                execution, on_error=True)

            if not clone_disks:
                self._create_task(
                    instance,
                    constants.TASK_TYPE_RESTORE_REPLICA_DISK_SNAPSHOTS,
                    execution, depends_on=[cleanup_deployment_task.id],
                    on_error=True)

        db_api.add_migration(ctxt, migration)
        LOG.info("Migration created: %s", migration.id)

        self._begin_tasks(ctxt, execution, migration.info)

        return self.get_migration(ctxt, migration.id)

    def migrate_instances(self, ctxt, origin_endpoint_id,
                          destination_endpoint_id, source_environment,
                          destination_environment, instances, network_map,
                          storage_mappings, skip_os_morphing=False,
                          notes=None):
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
        migration.executions = [execution]
        migration.instances = instances
        migration.info = {}
        migration.notes = notes

        self._check_create_reservation_for_transfer(
            migration, licensing_client.RESERVATION_TYPE_MIGRATION)

        for instance in instances:
            task_validate = self._create_task(
                instance, constants.TASK_TYPE_VALIDATE_MIGRATION_INPUTS,
                execution)

            task_export = self._create_task(
                instance, constants.TASK_TYPE_EXPORT_INSTANCE, execution,
                depends_on=[task_validate.id])

            if (constants.PROVIDER_TYPE_INSTANCE_FLAVOR in
                    destination_provider_types):
                get_optimal_flavor_task = self._create_task(
                    instance, constants.TASK_TYPE_GET_OPTIMAL_FLAVOR,
                    execution, depends_on=[task_export.id])
                next_task = get_optimal_flavor_task.id
            else:
                next_task = task_export.id

            task_import = self._create_task(
                instance, constants.TASK_TYPE_IMPORT_INSTANCE,
                execution, depends_on=[next_task])

            task_deploy_disk_copy_resources = self._create_task(
                instance, constants.TASK_TYPE_DEPLOY_DISK_COPY_RESOURCES,
                execution, depends_on=[task_import.id])

            task_copy_disk = self._create_task(
                instance, constants.TASK_TYPE_COPY_DISK_DATA,
                execution, depends_on=[task_deploy_disk_copy_resources.id])

            task_delete_disk_copy_resources = self._create_task(
                instance, constants.TASK_TYPE_DELETE_DISK_COPY_RESOURCES,
                execution, depends_on=[task_copy_disk.id], on_error=True)

            if not skip_os_morphing:
                task_deploy_os_morphing_resources = self._create_task(
                    instance, constants.TASK_TYPE_DEPLOY_OS_MORPHING_RESOURCES,
                    execution, depends_on=[task_delete_disk_copy_resources.id])

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
                next_task = task_delete_disk_copy_resources

            self._create_task(
                instance, constants.TASK_TYPE_FINALIZE_IMPORT_INSTANCE,
                execution, depends_on=[next_task.id])

            self._create_task(
                instance,
                constants.TASK_TYPE_CLEANUP_FAILED_IMPORT_INSTANCE,
                execution, on_error=True)

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
        if execution.status == constants.EXECUTION_STATUS_RUNNING:
            raise exception.InvalidMigrationState(
                "Cannot delete a running migration")
        db_api.delete_migration(ctxt, migration_id)

    @migration_synchronized
    def cancel_migration(self, ctxt, migration_id, force):
        migration = self._get_migration(ctxt, migration_id)
        execution = migration.executions[0]
        if execution.status != constants.EXECUTION_STATUS_RUNNING:
            raise exception.InvalidMigrationState(
                "The migration is not running")
        execution = migration.executions[0]
        self._cancel_tasks_execution(ctxt, execution, force)
        self._check_delete_reservation_for_transfer(migration)

    def _cancel_tasks_execution(self, ctxt, execution, force=False):
        has_running_tasks = False
        for task in execution.tasks:
            if task.status == constants.TASK_STATUS_RUNNING:
                self._rpc_worker_client.cancel_task(
                    ctxt, task.host, task.id, task.process_id, force)
                has_running_tasks = True
            elif (task.status == constants.TASK_STATUS_PENDING and
                    not task.on_error):
                db_api.set_task_status(
                    ctxt, task.id, constants.TASK_STATUS_CANCELED)

        if not has_running_tasks:
            try:
                origin = self._get_task_origin(ctxt, execution.action)
                destination = self._get_task_destination(
                    ctxt, execution.action)

                for task in execution.tasks:
                    if task.status in [constants.TASK_STATUS_PENDING,
                                       constants.TASK_STATUS_ON_ERROR_ONLY]:
                        if task.on_error:
                            action = db_api.get_action(
                                ctxt, execution.action_id)
                            task_info = action.info.get(task.instance, {})

                            self._rpc_worker_client.begin_task(
                                ctxt, server=None,
                                task_id=task.id,
                                task_type=task.task_type,
                                origin=origin,
                                destination=destination,
                                instance=task.instance,
                                task_info=task_info)

                            has_running_tasks = True
            except exception.NotFound as ex:
                LOG.error("A required endpoint could not be found")
                LOG.exception(ex)

        if not has_running_tasks:
            self._set_tasks_execution_status(
                ctxt, execution.id, constants.EXECUTION_STATUS_ERROR)

    @staticmethod
    def _set_tasks_execution_status(ctxt, execution_id, execution_status):
        LOG.info("Tasks execution %(id)s completed with status: %(status)s",
                 {"id": execution_id, "status": execution_status})
        db_api.set_execution_status(ctxt, execution_id, execution_status)
        if ctxt.delete_trust_id:
            keystone.delete_trust(ctxt)

    @task_synchronized
    def set_task_host(self, ctxt, task_id, host, process_id):
        db_api.set_task_host(ctxt, task_id, host, process_id)
        db_api.set_task_status(
            ctxt, task_id, constants.TASK_STATUS_RUNNING)

    def _start_pending_tasks(self, ctxt, execution, parent_task, task_info):
        origin = self._get_task_origin(ctxt, execution.action)
        destination = self._get_task_destination(ctxt, execution.action)

        for task in execution.tasks:
            if task.status == constants.TASK_STATUS_PENDING:
                if task.depends_on and parent_task.id in task.depends_on:
                    start_task = True
                    for depend_task_id in task.depends_on:
                        if depend_task_id != parent_task.id:
                            depend_task = db_api.get_task(ctxt, depend_task_id)
                            if (depend_task.status !=
                                    constants.TASK_STATUS_COMPLETED):
                                start_task = False
                                break
                    if start_task:
                        # instance imports need to be executed on the same host
                        server = None
                        if (task.task_type ==
                                constants.TASK_TYPE_IMPORT_INSTANCE):
                            server = parent_task.host

                        self._rpc_worker_client.begin_task(
                            ctxt, server=server,
                            task_id=task.id,
                            task_type=task.task_type,
                            origin=origin,
                            destination=destination,
                            instance=task.instance,
                            task_info=task_info)

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
                constants.TASK_TYPE_FINALIZE_IMPORT_INSTANCE,
                constants.TASK_TYPE_FINALIZE_REPLICA_INSTANCE_DEPLOYMENT):
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
                except exception.SchemaValidationException as ex:
                    LOG.warn(
                        "Could not validate transfer result '%s' against the "
                        "VM export info schema. NOT saving value in Database. "
                        "Exception details: %s",
                        transfer_result, utils.get_exception_details())
            else:
                LOG.debug(
                    "No 'transfer_result' was returned for task type '%s' "
                    "for transfer action '%s'", task_type, execution.action_id)
        elif task_type == constants.TASK_TYPE_UPDATE_REPLICA:
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

    @task_synchronized
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

            if execution.status == constants.EXECUTION_STATUS_RUNNING:
                self._start_pending_tasks(
                    ctxt, execution, task, updated_task_info)

                if not [t for t in execution.tasks
                        if t.status in [constants.TASK_STATUS_RUNNING,
                                        constants.TASK_STATUS_PENDING]]:
                    # The execution is in error status if there's one or more
                    # tasks in error or canceled status
                    if [t for t in execution.tasks
                            if t.status in [constants.TASK_STATUS_ERROR,
                                            constants.TASK_STATUS_CANCELED]]:
                        execution_status = constants.EXECUTION_STATUS_ERROR
                    else:
                        execution_status = constants.EXECUTION_STATUS_COMPLETED

                    self._set_tasks_execution_status(
                        ctxt, execution.id, execution_status)

    @task_synchronized
    def set_task_error(self, ctxt, task_id, exception_details):
        LOG.error("Task error: %(task_id)s - %(ex)s",
                  {"task_id": task_id, "ex": exception_details})

        db_api.set_task_status(
            ctxt, task_id, constants.TASK_STATUS_ERROR, exception_details)

        task = db_api.get_task(ctxt, task_id)
        execution = db_api.get_tasks_execution(ctxt, task.execution_id)

        action_id = execution.action_id
        with lockutils.lock(action_id):
            self._cancel_tasks_execution(ctxt, execution)

        # NOTE: if this is a migration, make sure to delete
        # its associated reservation.
        action = db_api.get_action(ctxt, action_id)
        if action.type == "migration":
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
        execution = models.TasksExecution()
        execution.id = str(uuid.uuid4())
        execution.status = constants.EXECUTION_STATUS_RUNNING
        execution.action = replica

        LOG.debug(
            "Replica '%s' info pre-replica-update: %s",
            replica_id, replica.info)
        for instance in execution.action.instances:
            # NOTE: "circular assignment" would lead to a `None` value
            # so we must operate on a copy:
            inst_info_copy = copy.deepcopy(replica.info[instance])
            inst_info_copy.update(properties)
            replica.info[instance] = inst_info_copy
            self._create_task(
                instance, constants.TASK_TYPE_UPDATE_REPLICA,
                execution)
        LOG.debug(
            "Replica '%s' info post-replica-update: %s",
            replica_id, replica.info)
        db_api.add_replica_tasks_execution(ctxt, execution)
        LOG.debug("Execution for Replica update tasks created: %s",
                  execution.id)
        self._begin_tasks(ctxt, execution, replica.info)
        return self.get_replica_tasks_execution(ctxt, replica_id, execution.id)
