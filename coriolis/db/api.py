# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_config import cfg
from oslo_db import api as db_api
from oslo_db import options as db_options
from oslo_db.sqlalchemy import enginefacade
from oslo_utils import timeutils
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import orm
from sqlalchemy.sql import null

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


def is_user_context(context):
    """Indicates if the request context is a normal user."""
    if not context:
        return False
    if not context.user_id or not context.project_id:
        return False
    if context.is_admin:
        return False
    return True


def _model_query(context, *args):
    session = _session(context)
    return session.query(*args)


def _get_replica_schedules_filter(context, replica_id=None,
                                  schedule_id=None, expired=True):
    now = timeutils.utcnow()
    q = _soft_delete_aware_query(context, models.ReplicaSchedule)
    q = q.join(models.Replica)
    sched_filter = q.filter()
    if is_user_context(context):
        sched_filter = sched_filter.filter(
            models.Replica.project_id == context.tenant)

    if replica_id:
        sched_filter = sched_filter.filter(
            models.Replica.id == replica_id)
    if schedule_id:
        sched_filter = sched_filter.filter(
            models.ReplicaSchedule.id == schedule_id)
    if not expired:
        sched_filter = sched_filter.filter(
            or_(models.ReplicaSchedule.expiration_date == null(),
                models.ReplicaSchedule.expiration_date > now))
    return sched_filter


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
def get_endpoints(context):
    q = _soft_delete_aware_query(context, models.Endpoint)
    if is_user_context(context):
        q = q.filter(
            models.Endpoint.project_id == context.tenant)
    return q.filter().all()


@enginefacade.reader
def get_endpoint(context, endpoint_id):
    q = _soft_delete_aware_query(context, models.Endpoint)
    if is_user_context(context):
        q = q.filter(
            models.Endpoint.project_id == context.tenant)
    return q.filter(
        models.Endpoint.id == endpoint_id).first()


@enginefacade.writer
def add_endpoint(context, endpoint):
    endpoint.user_id = context.user
    endpoint.project_id = context.tenant
    context.session.add(endpoint)


@enginefacade.writer
def update_endpoint(context, endpoint_id, updated_values):
    endpoint = get_endpoint(context, endpoint_id)
    if not endpoint:
        raise exception.NotFound("Endpoint not found")
    for n in ["name", "description", "connection_info"]:
        if n in updated_values:
            setattr(endpoint, n, updated_values[n])


@enginefacade.writer
def delete_endpoint(context, endpoint_id):
    args = {"id": endpoint_id}
    if is_user_context(context):
        args["project_id"] = context.tenant
    count = _soft_delete_aware_query(context, models.Endpoint).filter_by(
        **args).soft_delete()
    if count == 0:
        raise exception.NotFound("0 entries were soft deleted")


@enginefacade.reader
def get_replica_tasks_executions(context, replica_id, include_tasks=False):
    q = _soft_delete_aware_query(context, models.TasksExecution)
    q = q.join(models.Replica)
    if include_tasks:
        q = _get_tasks_with_details_options(q)
    if is_user_context(context):
        q = q.filter(models.Replica.project_id == context.tenant)
    return q.filter(
        models.Replica.id == replica_id).all()


@enginefacade.reader
def get_replica_tasks_execution(context, replica_id, execution_id):
    q = _soft_delete_aware_query(context, models.TasksExecution).join(
        models.Replica)
    q = _get_tasks_with_details_options(q)
    if is_user_context(context):
        q = q.filter(models.Replica.project_id == context.tenant)
    return q.filter(
        models.Replica.id == replica_id,
        models.TasksExecution.id == execution_id).first()


@enginefacade.writer
def add_replica_tasks_execution(context, execution):
    if is_user_context(context):
        if execution.action.project_id != context.tenant:
            raise exception.NotAuthorized()

    # include deleted records
    max_number = _model_query(
        context, func.max(models.TasksExecution.number)).filter_by(
            action_id=execution.action.id).first()[0] or 0
    execution.number = max_number + 1

    context.session.add(execution)


@enginefacade.writer
def delete_replica_tasks_execution(context, execution_id):
    q = _soft_delete_aware_query(context, models.TasksExecution).filter(
        models.TasksExecution.id == execution_id)
    if is_user_context(context):
        if not q.join(models.Replica).filter(
                models.Replica.project_id == context.tenant).first():
            raise exception.NotAuthorized()
    count = q.soft_delete()
    if count == 0:
        raise exception.NotFound("0 entries were soft deleted")


@enginefacade.reader
def get_replica_schedules(context, replica_id=None, expired=True):
    sched_filter = _get_replica_schedules_filter(
        context, replica_id=replica_id, expired=expired)
    return sched_filter.all()


@enginefacade.reader
def get_replica_schedule(context, replica_id, schedule_id, expired=True):
    sched_filter = _get_replica_schedules_filter(
        context, replica_id=replica_id, schedule_id=schedule_id,
        expired=expired)
    return sched_filter.first()


@enginefacade.writer
def update_replica_schedule(context, replica_id, schedule_id,
                            updated_values, pre_update_callable=None,
                            post_update_callable=None):
    # NOTE(gsamfira): we need to refactor the DB layer a bit to allow
    # two-phase transactions or at least allow running these functions
    # inside a single transaction block.
    schedule = get_replica_schedule(context, replica_id, schedule_id)
    if pre_update_callable:
        pre_update_callable(schedule=schedule)
    for val in ["schedule", "expiration_date", "enabled", "shutdown_instance"]:
        if val in updated_values:
            setattr(schedule, val, updated_values[val])
    if post_update_callable:
        # at this point nothing has really been sent to the DB,
        # but we may need to act upon the new changes elsewhere
        # before we actually commit to the database
        post_update_callable(context, schedule)


@enginefacade.writer
def delete_replica_schedule(context, replica_id,
                            schedule_id, pre_delete_callable=None,
                            post_delete_callable=None):
    # NOTE(gsamfira): we need to refactor the DB layer a bit to allow
    # two-phase transactions or at least allow running these functions
    # inside a single transaction block.

    q = _soft_delete_aware_query(context, models.ReplicaSchedule).filter(
        models.ReplicaSchedule.id == schedule_id,
        models.ReplicaSchedule.replica_id == replica_id)
    schedule = q.first()
    if not schedule:
        raise exception.NotFound(
            "No such schedule")
    if is_user_context(context):
        if not q.join(models.Replica).filter(
                models.Replica.project_id == context.tenant).first():
                raise exception.NotAuthorized()
    if pre_delete_callable:
        pre_delete_callable(context, schedule)
    count = q.soft_delete()
    if post_delete_callable:
        post_delete_callable(context, schedule)
    if count == 0:
        raise exception.NotFound("0 entries were soft deleted")


@enginefacade.writer
def add_replica_schedule(context, schedule, post_create_callable=None):
    # NOTE(gsamfira): we need to refactor the DB layer a bit to allow
    # two-phase transactions or at least allow running these functions
    # inside a single transaction block.

    if schedule.replica.project_id != context.tenant:
        raise exception.NotAuthorized()
    context.session.add(schedule)
    if post_create_callable:
        post_create_callable(context, schedule)


def _get_replica_with_tasks_executions_options(q):
    return q.options(orm.joinedload(models.Replica.executions))


@enginefacade.reader
def get_replicas(context, include_tasks_executions=False):
    q = _soft_delete_aware_query(context, models.Replica)
    if include_tasks_executions:
        q = _get_replica_with_tasks_executions_options(q)
    q = q.filter()
    if is_user_context(context):
        q = q.filter(
            models.Replica.project_id == context.tenant)
    return q.all()


@enginefacade.reader
def get_replica(context, replica_id):
    q = _soft_delete_aware_query(context, models.Replica)
    q = _get_replica_with_tasks_executions_options(q)
    if is_user_context(context):
        q = q.filter(
            models.Replica.project_id == context.tenant)
    return q.filter(
        models.Replica.id == replica_id).first()


@enginefacade.reader
def get_endpoint_replicas_count(context, endpoint_id):
    origin_args = {'origin_endpoint_id': endpoint_id}
    q_origin_count = _soft_delete_aware_query(
        context, models.Replica).filter_by(**origin_args).count()

    destination_args = {'destination_endpoint_id': endpoint_id}
    q_destination_count = _soft_delete_aware_query(
        context, models.Replica).filter_by(**destination_args).count()

    return q_origin_count + q_destination_count


@enginefacade.writer
def add_replica(context, replica):
    replica.user_id = context.user
    replica.project_id = context.tenant
    context.session.add(replica)


@enginefacade.writer
def _delete_transfer_action(context, cls, id):
    args = {"base_id": id}
    if is_user_context(context):
        args["project_id"] = context.tenant
    count = _soft_delete_aware_query(context, cls).filter_by(
        **args).soft_delete()
    if count == 0:
        raise exception.NotFound("0 entries were soft deleted")

    _soft_delete_aware_query(context, models.TasksExecution).filter_by(
        action_id=id).soft_delete()


@enginefacade.writer
def delete_replica(context, replica_id):
    _delete_transfer_action(context, models.Replica, replica_id)


@enginefacade.reader
def get_replica_migrations(context, replica_id):
    q = _soft_delete_aware_query(context, models.Migration)
    q = q.join("replica")
    q = q.options(orm.joinedload("executions"))
    if is_user_context(context):
        q = q.filter(
            models.Migration.project_id == context.tenant)
    return q.filter(
        models.Replica.id == replica_id).all()


@enginefacade.reader
def get_migrations(context, include_tasks=False):
    q = _soft_delete_aware_query(context, models.Migration)
    if include_tasks:
        q = _get_migration_task_query_options(q)
    else:
        q = q.options(orm.joinedload("executions"))
    args = {}
    if is_user_context(context):
        args["project_id"] = context.tenant
    return q.filter_by(**args).all()


def _get_tasks_with_details_options(query):
    return query.options(
        orm.joinedload("action")).options(
            orm.joinedload("tasks").
            joinedload("progress_updates")).options(
                orm.joinedload("tasks").
                joinedload("events"))


def _get_migration_task_query_options(query):
    return query.options(
        orm.joinedload("executions").
        joinedload("tasks").
        joinedload("progress_updates")).options(
        orm.joinedload("executions").
        joinedload("tasks").
        joinedload("events")).options(
        orm.joinedload("executions").
        joinedload("action"))


@enginefacade.reader
def get_migration(context, migration_id):
    q = _soft_delete_aware_query(context, models.Migration)
    q = _get_migration_task_query_options(q)
    args = {"id": migration_id}
    if is_user_context(context):
        args["project_id"] = context.tenant
    return q.filter_by(**args).first()


@enginefacade.writer
def add_migration(context, migration):
    migration.user_id = context.user
    migration.project_id = context.tenant
    context.session.add(migration)


@enginefacade.writer
def delete_migration(context, migration_id):
    _delete_transfer_action(context, models.Migration, migration_id)


@enginefacade.writer
def set_execution_status(context, execution_id, status):
    execution = _soft_delete_aware_query(
        context, models.TasksExecution).join(
            models.TasksExecution.action)
    if is_user_context(context):
        execution = execution.filter(
            models.BaseTransferAction.project_id == context.tenant)
    execution = execution.filter(
        models.TasksExecution.id == execution_id).first()
    if not execution:
        raise exception.NotFound(
            "Tasks execution not found: %s" % execution_id)

    execution.status = status


@enginefacade.reader
def get_action(context, action_id):
    action = _soft_delete_aware_query(
        context, models.BaseTransferAction)
    if is_user_context(context):
        action = action.filter(
            models.BaseTransferAction.project_id == context.tenant)
    action = action.filter(
        models.BaseTransferAction.base_id == action_id).first()
    if not action:
        raise exception.NotFound(
            "Transfer action not found: %s" % action_id)
    return action


@enginefacade.writer
def set_transfer_action_info(context, action_id, instance, instance_info):
    action = get_action(context, action_id)

    # Copy is needed, otherwise sqlalchemy won't save the changes
    action_info = action.info.copy()
    if instance in action_info:
        instance_info_old = action_info[instance].copy()
        instance_info_old.update(instance_info)
        action_info[instance] = instance_info_old
    else:
        action_info[instance] = instance_info
    action.info = action_info

    return action_info[instance]


@enginefacade.reader
def get_tasks_execution(context, execution_id):
    q = _soft_delete_aware_query(context, models.TasksExecution)
    q = q.join(models.BaseTransferAction)
    q = q.options(orm.joinedload("action"))
    q = q.options(orm.joinedload("tasks"))
    if is_user_context(context):
        q = q.filter(
            models.BaseTransferAction.project_id == context.tenant)
    execution = q.filter(
        models.TasksExecution.id == execution_id).first()
    if not execution:
        raise exception.NotFound(
            "Tasks execution not found: %s" % execution_id)
    return execution


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
def get_task(context, task_id):
    q = _soft_delete_aware_query(context, models.Task)
    return q.filter_by(id=task_id).first()


@enginefacade.writer
def add_task_event(context, task_id, level, message):
    task_event = models.TaskEvent()
    task_event.task_id = task_id
    task_event.level = level
    task_event.message = message
    context.session.add(task_event)


def _get_progress_update(context, task_id, current_step):
    q = _soft_delete_aware_query(context, models.TaskProgressUpdate)
    return q.filter(
        models.TaskProgressUpdate.task_id == task_id,
        models.TaskProgressUpdate.current_step == current_step).first()


@enginefacade.writer
def add_task_progress_update(context, task_id, current_step, total_steps,
                             message):
    task_progress_update = _get_progress_update(context, task_id, current_step)
    if not task_progress_update:
        task_progress_update = models.TaskProgressUpdate()
        context.session.add(task_progress_update)

    task_progress_update.task_id = task_id
    task_progress_update.current_step = current_step
    task_progress_update.total_steps = total_steps
    task_progress_update.message = message
