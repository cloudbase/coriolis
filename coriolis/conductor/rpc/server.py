# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import functools
import uuid

from oslo_concurrency import lockutils
from oslo_log import log as logging

from coriolis import constants
from coriolis.db import api as db_api
from coriolis.db.sqlalchemy import models
from coriolis import exception
from coriolis import utils
from coriolis.worker.rpc import client as rpc_worker_client

VERSION = "1.0"

LOG = logging.getLogger(__name__)


def replica_synchronized(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, replica_id, *args, **kwargs):
        @lockutils.synchronized(replica_id)
        def inner():
            return func(self, ctxt, replica_id, *args, **kwargs)
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
    def wrapper(self, ctxt, execution_id, *args, **kwargs):
        @lockutils.synchronized(execution_id)
        def inner():
            return func(self, ctxt, execution_id, *args, **kwargs)
        return inner()
    return wrapper


class ConductorServerEndpoint(object):
    def __init__(self):
        self._rpc_worker_client = rpc_worker_client.WorkerClient()

    @staticmethod
    def _create_task(instance, task_type, execution, depends_on=None):
        task = models.Task()
        task.id = str(uuid.uuid4())
        task.instance = instance
        task.execution = execution
        task.status = constants.TASK_STATUS_PENDING
        task.task_type = task_type
        task.depends_on = depends_on
        return task

    def _begin_tasks(self, ctxt, execution, task_info={}):
        for task in execution.tasks:
            if not task.depends_on:
                self._rpc_worker_client.begin_task(
                    ctxt, server=None,
                    task_id=task.id,
                    task_type=task.task_type,
                    origin=execution.action.origin,
                    destination=execution.action.destination,
                    instance=task.instance,
                    task_info=task_info.get(task.instance, {}))

    @replica_synchronized
    def execute_replica_tasks(self, ctxt, replica_id, shutdown_instances):
        replica = self._get_replica(ctxt, replica_id)
        self._check_running_executions(replica)
        execution = models.TasksExecution()
        execution.id = str(uuid.uuid4())
        execution.status = constants.EXECUTION_STATUS_RUNNING
        execution.action = replica

        for instance in execution.action.instances:
                depends_on = []
                if shutdown_instances:
                    shutdown_instance_task = self._create_task(
                        instance, constants.TASK_TYPE_SHUTDOWN_INSTANCE,
                        execution)
                    depends_on = [shutdown_instance_task.id]

                get_instance_info_task = self._create_task(
                    instance, constants.TASK_TYPE_GET_INSTANCE_INFO,
                    execution, depends_on=depends_on)

                deploy_replica_disks_task = self._create_task(
                    instance, constants.TASK_TYPE_DEPLOY_REPLICA_DISKS,
                    execution, depends_on=[get_instance_info_task.id])

                deploy_replica_export_resources_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_DEPLOY_REPLICA_SOURCE_RESOURCES,
                    execution, depends_on=[deploy_replica_disks_task.id])

                deploy_replica_resources_task = self._create_task(
                    instance,
                    constants.TASK_TYPE_DEPLOY_REPLICA_TARGET_RESOURCES,
                    execution, depends_on=[deploy_replica_disks_task.id])

                replicate_disks_task = self._create_task(
                    instance, constants.TASK_TYPE_REPLICATE_DISKS,
                    execution, depends_on=[
                        deploy_replica_export_resources_task.id,
                        deploy_replica_resources_task.id])

                self._create_task(
                    instance,
                    constants.TASK_TYPE_DELETE_REPLICA_SOURCE_RESOURCES,
                    execution, depends_on=[replicate_disks_task.id])

                self._create_task(
                    instance,
                    constants.TASK_TYPE_DELETE_REPLICA_TARGET_RESOURCES,
                    execution, depends_on=[replicate_disks_task.id])

        db_api.add_replica_tasks_execution(ctxt, execution)
        LOG.info("Replica tasks execution created: %s", execution.id)

        self._begin_tasks(ctxt, execution, replica.info)
        return self.get_replica_tasks_execution(ctxt, execution.id)

    @replica_synchronized
    def get_replica_tasks_executions(self, ctxt, replica_id,
                                     include_tasks=False):
        return db_api.get_replica_tasks_executions(
            ctxt, replica_id, include_tasks)

    @tasks_execution_synchronized
    def get_replica_tasks_execution(self, ctxt, execution_id):
        return self._get_replica_tasks_execution(
            ctxt, execution_id)

    @tasks_execution_synchronized
    def delete_replica_tasks_execution(self, ctxt, execution_id):
        execution = self._get_replica_tasks_execution(
            ctxt, execution_id)
        if execution.status == constants.EXECUTION_STATUS_RUNNING:
            raise exception.InvalidMigrationState(
                "Cannot delete a running replica tasks execution")
        db_api.delete_replica_tasks_execution(ctxt, execution_id)

    @tasks_execution_synchronized
    def cancel_replica_tasks_execution(self, ctxt, execution_id):
        execution = self._get_replica_tasks_execution(
            ctxt, execution_id)
        if execution.status != constants.EXECUTION_STATUS_RUNNING:
            raise exception.InvalidReplicaState(
                "The replica tasks execution is not running")
        self._cancel_tasks_execution(ctxt, execution)

    def _get_replica_tasks_execution(self, ctxt, execution_id):
        execution = db_api.get_replica_tasks_execution(
            ctxt, execution_id)
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
        self._check_running_executions(replica)
        db_api.delete_replica(ctxt, replica_id)

    @replica_synchronized
    def delete_replica_disks(self, ctxt, replica_id):
        replica = self._get_replica(ctxt, replica_id)
        self._check_running_executions(replica)

        execution = models.TasksExecution()
        execution.id = str(uuid.uuid4())
        execution.status = constants.EXECUTION_STATUS_RUNNING
        execution.action = replica

        has_tasks = False
        for instance in replica.instances:
            if (instance in replica.instances and
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
        return self.get_replica_tasks_execution(ctxt, execution.id)

    def create_instances_replica(self, ctxt, origin, destination, instances):
        replica = models.Replica()
        replica.id = str(uuid.uuid4())
        replica.origin = origin
        replica.destination = destination
        replica.instances = instances
        replica.executions = []
        replica.info = {}

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

    def _check_running_executions(self, action):
        if [e for e in action.executions
                if e.status == constants.EXECUTION_STATUS_RUNNING]:
            raise exception.InvalidActionTasksExecutionState(
                "Another tasks execution is in progress")

    @staticmethod
    def _check_valid_replica_tasks_execution(replica, forced=False):
        sorted_executions = sorted(
            replica.executions, key=lambda e: e.number, reverse=True)

        if (forced and sorted_executions[0].status !=
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

    @replica_synchronized
    def deploy_replica_instances(self, ctxt, replica_id, forced):
        replica = self._get_replica(ctxt, replica_id)
        self._check_running_executions(replica)
        self._check_valid_replica_tasks_execution(replica, forced)

        instances = replica.instances

        migration = models.Migration()
        migration.id = str(uuid.uuid4())
        migration.origin = replica.origin
        migration.destination = replica.destination
        migration.instances = instances
        migration.replica = replica
        migration.info = replica.info

        execution = models.TasksExecution()
        migration.executions = [execution]
        execution.status = constants.EXECUTION_STATUS_RUNNING
        execution.number = 1

        for instance in instances:
            create_snapshot_task = self._create_task(
                instance, constants.TASK_TYPE_CREATE_REPLICA_DISK_SNAPSHOTS,
                execution)

            deploy_replica_task = self._create_task(
                instance, constants.TASK_TYPE_DEPLOY_REPLICA_INSTANCE,
                execution, [create_snapshot_task.id])

            self._create_task(
                instance, constants.TASK_TYPE_DELETE_REPLICA_DISK_SNAPSHOTS,
                execution, [deploy_replica_task.id])

        db_api.add_migration(ctxt, migration)
        LOG.info("Migration created: %s", migration.id)

        self._begin_tasks(ctxt, execution, migration.info)

        return self.get_migration(ctxt, migration.id)

    def migrate_instances(self, ctxt, origin, destination, instances):
        migration = models.Migration()
        migration.id = str(uuid.uuid4())
        migration.origin = origin
        migration.destination = destination
        execution = models.TasksExecution()
        execution.status = constants.EXECUTION_STATUS_RUNNING
        execution.number = 1
        migration.executions = [execution]
        migration.instances = instances
        migration.info = {}

        for instance in instances:

            task_export = self._create_task(
                instance, constants.TASK_TYPE_EXPORT_INSTANCE, execution)

            self._create_task(
                instance, constants.TASK_TYPE_IMPORT_INSTANCE,
                execution, depends_on=[task_export.id])

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
    def cancel_migration(self, ctxt, migration_id):
        migration = self._get_migration(ctxt, migration_id)
        execution = migration.executions[0]
        if execution.status != constants.EXECUTION_STATUS_RUNNING:
            raise exception.InvalidMigrationState(
                "The migration is not running")
        execution = migration.executions[0]
        self._cancel_tasks_execution(ctxt, execution)

    def _cancel_tasks_execution(self, ctxt, execution):
        for task in execution.tasks:
            if task.status in [constants.TASK_STATUS_PENDING,
                               constants.TASK_STATUS_RUNNING]:
                if task.status == constants.TASK_STATUS_RUNNING:
                    self._rpc_worker_client.cancel_task(
                        ctxt, task.host, task.process_id)
                db_api.set_task_status(
                    ctxt, task.id, constants.TASK_STATUS_CANCELED)

        db_api.set_execution_status(
            ctxt, execution.id, constants.EXECUTION_STATUS_ERROR)

    @task_synchronized
    def set_task_host(self, ctxt, task_id, host, process_id):
        db_api.set_task_host(ctxt, task_id, host, process_id)
        db_api.set_task_status(
            ctxt, task_id, constants.TASK_STATUS_RUNNING)

    def _start_pending_tasks(self, ctxt, execution, parent_task, task_info):
        has_pending_tasks = False
        for task in execution.tasks:
            if task.status == constants.TASK_STATUS_PENDING:
                has_pending_tasks = True
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

                        action = execution.action
                        self._rpc_worker_client.begin_task(
                            ctxt, server=server,
                            task_id=task.id,
                            task_type=task.task_type,
                            origin=action.origin,
                            destination=action.destination,
                            instance=task.instance,
                            task_info=task_info)
        return has_pending_tasks

    @task_synchronized
    def task_completed(self, ctxt, task_id, task_info):
        LOG.info("Task completed: %s", task_id)

        db_api.set_task_status(
            ctxt, task_id, constants.TASK_STATUS_COMPLETED)

        task = db_api.get_task(
            ctxt, task_id, include_execution_tasks=True)

        execution = task.execution
        with lockutils.lock(execution.action_id):
            LOG.info("Setting instance %(instance)s "
                     "action info: %(task_info)s",
                     {"instance": task.instance, "task_info": task_info})
            updated_task_info = db_api.set_transfer_action_info(
                ctxt, execution.action_id, task.instance, task_info)

            if execution.status == constants.EXECUTION_STATUS_RUNNING:
                has_pending_tasks = self._start_pending_tasks(
                    ctxt, execution, task, updated_task_info)

                if not has_pending_tasks:
                    LOG.info("Tasks execution completed: %s", execution.id)
                    db_api.set_execution_status(
                        ctxt, execution.id,
                        constants.EXECUTION_STATUS_COMPLETED)

    @task_synchronized
    def set_task_error(self, ctxt, task_id, exception_details):
        LOG.error("Task error: %(task_id)s - %(ex)s",
                  {"task_id": task_id, "ex": exception_details})

        db_api.set_task_status(
            ctxt, task_id, constants.TASK_STATUS_ERROR, exception_details)

        task = db_api.get_task(
            ctxt, task_id, include_execution_tasks=True)
        execution = task.execution

        with lockutils.lock(execution.action_id):
            for task in execution.tasks:
                if task.status == constants.TASK_STATUS_PENDING:
                    db_api.set_task_status(
                        ctxt, task.id, constants.TASK_STATUS_CANCELED)

            LOG.error("Tasks execution failed: %s", execution.id)
            db_api.set_execution_status(
                ctxt, execution.id, constants.EXECUTION_STATUS_ERROR)

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
