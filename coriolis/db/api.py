# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_config import cfg
from oslo_db import api as db_api
from oslo_db import options as db_options
from oslo_db.sqlalchemy import enginefacade
from sqlalchemy import orm

from coriolis.db.sqlalchemy import models
from coriolis import exception

CONF = cfg.CONF
db_options.set_defaults(CONF)


_BACKEND_MAPPING = {'sqlalchemy': 'coriolis.db.sqlalchemy.api'}
IMPL = db_api.DBAPI.from_config(CONF, backend_mapping=_BACKEND_MAPPING)


def get_engine():
    return IMPL.get_engine()


def get_session():
    return IMPL.get_session()


def db_sync(engine, version=None):
    """Migrate the database to `version` or the most recent version."""
    return IMPL.db_sync(engine, version=version)


def db_version(engine):
    """Display the current database version."""
    return IMPL.db_version(engine)


def _session(context):
    return (context and context.session) or get_session()


def _model_query(context, *args):
    session = _session(context)
    return session.query(*args)


def _soft_delete_aware_query(context, *args, **kwargs):
    """Query helper that accounts for context's `show_deleted` field.
    :param show_deleted: if True, overrides context's show_deleted field.
    """
    query = _model_query(context, *args)
    show_deleted = kwargs.get('show_deleted') or context.show_deleted

    if not show_deleted:
        query = query.filter_by(deleted_at=None)
    return query


@enginefacade.reader
def get_migrations(context, include_tasks=False):
    q = _soft_delete_aware_query(context, models.Migration)
    if include_tasks:
        q = _get_migration_task_query_options(q)
    return q.filter_by(project_id=context.tenant).all()


def _get_migration_task_query_options(query):
    return query.options(
        orm.joinedload("tasks").joinedload("progress_updates")).options(
            orm.joinedload("tasks").joinedload("events"))


@enginefacade.reader
def get_migration(context, migration_id):
    q = _soft_delete_aware_query(context, models.Migration)
    q = _get_migration_task_query_options(q)
    return q.filter_by(project_id=context.tenant, id=migration_id).first()


@enginefacade.writer
def add_migration(context, migration):
    migration.user_id = context.user
    migration.project_id = context.tenant
    context.session.add(migration)


@enginefacade.writer
def delete_migration(context, migration_id):
    count = _soft_delete_aware_query(context, models.Migration).filter_by(
        project_id=context.tenant, id=migration_id).soft_delete()
    if count == 0:
        raise exception.NotFound("0 entries were soft deleted")


@enginefacade.writer
def set_migration_status(context, migration_id, status):
    migration = _soft_delete_aware_query(context, models.Migration).filter_by(
        project_id=context.tenant, id=migration_id).first()
    if not migration:
        raise exception.NotFound("Migration not found: %s" % migration_id)

    migration.status = status


def _get_task(context, task_id):
    task = _soft_delete_aware_query(context, models.Task).filter_by(
        id=task_id).first()
    if not task:
        raise exception.NotFound("Task not found: %s" % task_id)
    return task


@enginefacade.writer
def set_task_status(context, task_id, status, exception_details=None):
    task = _get_task(context, task_id)
    task.status = status
    task.exception_details = exception_details


@enginefacade.writer
def set_task_host(context, task_id, host, process_id):
    task = _get_task(context, task_id)
    task.host = host
    task.process_id = process_id


@enginefacade.reader
def get_task(context, task_id, include_migration_tasks=False):
    join_options = orm.joinedload("migration")
    if include_migration_tasks:
        join_options = join_options.joinedload("tasks")

    return _soft_delete_aware_query(context, models.Task).options(
        join_options).filter_by(id=task_id).first()


@enginefacade.writer
def add_task_event(context, task_id, level, message):
    task_event = models.TaskEvent()
    task_event.task_id = task_id
    task_event.level = level
    task_event.message = message
    context.session.add(task_event)


@enginefacade.writer
def add_task_progress_update(context, task_id, current_step, total_steps,
                             message):
    task_progress_update = models.TaskProgressUpdate()
    task_progress_update.task_id = task_id
    task_progress_update.current_step = current_step
    task_progress_update.total_steps = total_steps
    task_progress_update.message = message
    context.session.add(task_progress_update)
