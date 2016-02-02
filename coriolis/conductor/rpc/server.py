import uuid

from oslo_log import log as logging

from coriolis import constants
from coriolis.db import api as db_api
from coriolis.db.sqlalchemy import models
from coriolis import exception
from coriolis.worker.rpc import client as rpc_worker_client

VERSION = "1.0"

LOG = logging.getLogger(__name__)


class ConductorServerEndpoint(object):
    def __init__(self):
        self._rpc_worker_client = rpc_worker_client.WorkerClient()

    def get_migrations(self, ctxt):
        return db_api.get_migrations(ctxt)

    def get_migration(self, ctxt, migration_id):
        return db_api.get_migration(ctxt, migration_id)

    def migrate_instances(self, ctxt, origin, destination, instances):
        migration = models.Migration()
        migration.id = str(uuid.uuid4())
        migration.status = constants.MIGRATION_STATUS_RUNNING
        migration.origin = origin
        migration.destination = destination

        for instance in instances:
            task_export = models.Task()
            task_export.id = str(uuid.uuid4())
            task_export.migration = migration
            task_export.instance = instance
            task_export.status = constants.TASK_STATUS_PENDING
            task_export.task_type = constants.TASK_TYPE_EXPORT_INSTANCE

            task_import = models.Task()
            task_import.id = str(uuid.uuid4())
            task_import.migration = migration
            task_import.instance = instance
            task_import.status = constants.TASK_STATUS_PENDING
            task_import.task_type = constants.TASK_TYPE_IMPORT_INSTANCE
            task_import.depends_on = [task_export.id]

        db_api.add_migration(ctxt, migration)
        LOG.info("Migration created: %s", migration.id)

        for task in migration.tasks:
            if not task.depends_on:
                self._rpc_worker_client.begin_task(
                    ctxt, server=None,
                    task_id=task.id,
                    task_type=task.task_type,
                    origin=migration.origin,
                    destination=migration.destination,
                    instance=task.instance,
                    task_info=None)

        return self.get_migration(ctxt, migration.id)

    def _get_migration(self, ctxt, migration_id):
        migration = db_api.get_migration(ctxt, migration_id)
        if not migration:
            raise exception.NotFound("Migration not found: %s" % migration_id)
        return migration

    def delete_migration(self, ctxt, migration_id):
        migration = self._get_migration(ctxt, migration_id)
        for task in migration.tasks:
            if task.status == constants.TASK_STATUS_RUNNING:
                raise exception.CoriolisException(
                    "Cannot delete a running migration")
        db_api.delete_migration(ctxt, migration_id)

    def cancel_migration(self, ctxt, migration_id):
        migration = self._get_migration(ctxt, migration_id)
        if migration.status != constants.MIGRATION_STATUS_RUNNING:
            raise exception.InvalidParameterValue(
                "The migration is not running: %s" % migration_id)

        for task in migration.tasks:
            if task.status == constants.TASK_STATUS_RUNNING:
                self._rpc_worker_client.cancel_task(
                    ctxt, task.host, task.process_id)

    def set_task_host(self, ctxt, task_id, host, process_id):
        db_api.set_task_host(ctxt, task_id, host, process_id)
        db_api.set_task_status(
            ctxt, task_id, constants.TASK_STATUS_RUNNING)

    def _start_pending_tasks(self, ctxt, migration, parent_task, task_info):
        has_pending_tasks = False
        for task in migration.tasks:
            if (task.depends_on and parent_task.id in task.depends_on and
                    task.status == constants.TASK_STATUS_PENDING):
                has_pending_tasks = True
                # instance imports needs to be executed on the same host
                server = None
                if task.task_type == constants.TASK_TYPE_IMPORT_INSTANCE:
                    server = parent_task.host

                self._rpc_worker_client.begin_task(
                    ctxt, server=server,
                    task_id=task.id,
                    task_type=task.task_type,
                    origin=migration.origin,
                    destination=migration.destination,
                    instance=task.instance,
                    task_info=task_info)
        return has_pending_tasks

    def task_completed(self, ctxt, task_id, task_info):
        LOG.info("Task completed: %s", task_id)

        db_api.set_task_status(
            ctxt, task_id, constants.TASK_STATUS_COMPLETED)

        task = db_api.get_task(
            ctxt, task_id, include_migration_tasks=True)

        migration = task.migration
        has_pending_tasks = self._start_pending_tasks(ctxt, migration, task,
                                                      task_info)

        if not has_pending_tasks:
            LOG.info("Migration completed: %s", migration.id)
            db_api.set_migration_status(
                ctxt, migration.id, constants.MIGRATION_STATUS_COMPLETED)

    def set_task_error(self, ctxt, task_id, exception_details):
        LOG.error("Task error: %(task_id)s - %(ex)s",
                  {"task_id": task_id, "ex": exception_details})

        db_api.set_task_status(
            ctxt, task_id, constants.TASK_STATUS_ERROR, exception_details)

        task = db_api.get_task(
            ctxt, task_id, include_migration_tasks=True)
        migration = task.migration

        for task in migration.tasks:
            if task.status == constants.TASK_STATUS_PENDING:
                db_api.set_task_status(
                    ctxt, task.id, constants.TASK_STATUS_CANCELED)

        LOG.error("Migration failed: %s", migration.id)
        db_api.set_migration_status(
            ctxt, migration.id, constants.MIGRATION_STATUS_ERROR)

    def task_progress_update(self, ctxt, task_id, current_step, total_steps,
                             message):
        LOG.info("Task progress update: %s", task_id)
        db_api.add_task_progress_update(ctxt, task_id, current_step,
                                        total_steps, message)
