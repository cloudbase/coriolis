# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_config import cfg
from oslo_db import api as db_api
from oslo_db import options as db_options
from oslo_db.sqlalchemy import enginefacade
from oslo_log import log as logging
from oslo_utils import timeutils
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import orm
from sqlalchemy.sql import null

from coriolis.db.sqlalchemy import models
from coriolis import exception
from coriolis import utils

CONF = cfg.CONF
db_options.set_defaults(CONF)

LOG = logging.getLogger(__name__)

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


def _update_sqlalchemy_object_fields(obj, updateable_fields, values_to_update):
    """ Updates the given 'values_to_update' on the provided sqlalchemy object
    as long as they are included as 'updateable_fields'.
    :param obj: object: sqlalchemy object
    :param updateable_fields: list(str): list of fields which are updateable
    :param values_to_update: dict: dict with the key/vals to update
    """
    if not isinstance(values_to_update, dict):
        raise exception.InvalidInput(
            "Properties to update for DB object of type '%s' must be a dict, "
            "got the following (type %s): %s" % (
                type(obj), type(values_to_update), values_to_update))

    non_updateable_fields = set(
        values_to_update.keys()).difference(
            set(updateable_fields))
    if non_updateable_fields:
        raise exception.Conflict(
            "Fields %s for '%s' database cannot be updated. "
            "Only updateable fields are: %s" % (
                non_updateable_fields, type(obj), updateable_fields))

    for field_name, field_val in values_to_update.items():
        if not hasattr(obj, field_name):
            raise exception.InvalidInput(
                "No region field named '%s' to update." % field_name)
        setattr(obj, field_name, field_val)
    LOG.debug(
        "Successfully updated the following fields on DB object "
        "of type '%s': %s" % (type(obj), values_to_update.keys()))


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
    show_deleted = kwargs.get('show_deleted')
    if context and context.show_deleted:
        show_deleted = True

    if not show_deleted:
        query = query.filter_by(deleted_at=None)
    return query


@enginefacade.reader
def get_endpoints(context):
    q = _soft_delete_aware_query(context, models.Endpoint).options(
        orm.joinedload('mapped_regions'))
    if is_user_context(context):
        q = q.filter(
            models.Endpoint.project_id == context.tenant)
    return q.filter().all()


@enginefacade.reader
def get_endpoint(context, endpoint_id):
    q = _soft_delete_aware_query(context, models.Endpoint).options(
        orm.joinedload('mapped_regions'))
    if is_user_context(context):
        q = q.filter(
            models.Endpoint.project_id == context.tenant)
    return q.filter(
        models.Endpoint.id == endpoint_id).first()


@enginefacade.writer
def add_endpoint(context, endpoint):
    endpoint.user_id = context.user
    endpoint.project_id = context.tenant
    _session(context).add(endpoint)


@enginefacade.writer
def update_endpoint(context, endpoint_id, updated_values):
    endpoint = get_endpoint(context, endpoint_id)
    if not endpoint:
        raise exception.NotFound("Endpoint with ID '%s' found" % endpoint_id)


    if not isinstance(updated_values, dict):
        raise exception.InvalidInput(
            "Update payload for endpoints must be a dict. Got the following "
            "(type: %s): %s" % (type(updated_values), updated_values))

    def _try_unmap_regions(region_ids):
         for region_to_unmap in region_ids:
            try:
                LOG.debug(
                    "Attempting to unmap region '%s' from endpoint '%s'",
                    region_to_unmap, endpoint_id)
                delete_endpoint_region_mapping(
                    context, endpoint_id, region_to_unmap)
            except Exception as ex:
                LOG.warn(
                    "Exception occurred while attempting to unmap region '%s' "
                    "from endpoint '%s'. Ignoring. Error was: %s",
                    region_to_unmap, endpoint_id,
                    utils.get_exception_details())

    newly_mapped_regions = []
    regions_to_unmap = []
    # NOTE: `.pop()` required for  `_update_sqlalchemy_object_fields` call:
    desired_region_mappings = updated_values.pop('mapped_regions', None)
    if desired_region_mappings is not None:
        # ensure all requested regions exist:
        for region_id in desired_region_mappings:
            region = get_region(context, region_id)
            if not region:
                raise exception.NotFound(
                    "Could not find region with ID '%s' for associating "
                    "with endpoint '%s' during update process." % (
                        region_id, endpoint_id))

        # get all existing mappings:
        existing_region_mappings = [
            mapping.region_id
            for mapping in get_region_mappings_for_endpoint(
                context, endpoint_id)]

        # check and add new mappings:
        to_map = set(
            desired_region_mappings).difference(set(existing_region_mappings))
        regions_to_unmap = set(
            existing_region_mappings).difference(set(desired_region_mappings))

        LOG.debug(
            "Remapping regions for endpoint '%s' from %s to %s",
            endpoint_id, existing_region_mappings, desired_region_mappings)

        region_id = None
        try:
            for region_id in to_map:
                mapping = models.EndpointRegionMapping()
                mapping.region_id = region_id
                mapping.endpoint_id = endpoint_id
                add_endpoint_region_mapping(context, mapping)
                newly_mapped_regions.append(region_id)
        except Exception as ex:
            LOG.warn(
                "Exception occurred while adding region mapping for '%s' to "
                "endpoint '%s'. Cleaning up created mappings (%s). Error was: "
                "%s", region_id, endpoint_id, newly_mapped_regions,
                utils.get_exception_details())
            _try_unmap_regions(newly_mapped_regions)
            raise


    updateable_fields = ["name", "description", "connection_info"]
    try:
        _update_sqlalchemy_object_fields(
            endpoint, updateable_fields, updated_values)
    except Exception as ex:
        LOG.warn(
            "Exception occurred while updating fields of endpoint '%s'. "
            "Cleaning ""up created mappings (%s). Error was: %s",
            endpoint_id, newly_mapped_regions, utils.get_exception_details())
        _try_unmap_regions(newly_mapped_regions)
        raise

    # remove all of the old region mappings:
    LOG.debug(
        "Unmapping the following regions during update of endpoint '%s': %s",
        endpoint_id, regions_to_unmap)
    _try_unmap_regions(regions_to_unmap)


@enginefacade.writer
def delete_endpoint(context, endpoint_id):
    endpoint = get_endpoint(context, endpoint_id)
    args = {"id": endpoint_id}
    if is_user_context(context):
        args["project_id"] = context.tenant
    count = _soft_delete_aware_query(context, models.Endpoint).filter_by(
        **args).soft_delete()
    if count == 0:
        raise exception.NotFound("0 Endpoint entries were soft deleted")
    # NOTE(aznashwan): many-to-many tables with soft deletion on either end of
    # the association are not handled properly so we must manually delete each
    # association ourselves:
    for reg in endpoint.mapped_regions:
        delete_endpoint_region_mapping(context, endpoint_id, reg.id)


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

    _session(context).add(execution)


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
    _session(context).add(schedule)
    if post_create_callable:
        post_create_callable(context, schedule)


def _get_replica_with_tasks_executions_options(q):
    return q.options(orm.joinedload(models.Replica.executions))


@enginefacade.reader
def get_replicas(context,
                 include_tasks_executions=False,
                 include_info=False,
                 to_dict=True):
    q = _soft_delete_aware_query(context, models.Replica)
    if include_tasks_executions:
        q = _get_replica_with_tasks_executions_options(q)
    if include_info is False:
        q = q.options(orm.defer('info'))
    q = q.filter()
    if is_user_context(context):
        q = q.filter(
            models.Replica.project_id == context.tenant)
    db_result = q.all()
    if to_dict:
        return [
            i.to_dict(
                include_info=include_info,
                include_executions=include_tasks_executions)
            for i in db_result]
    return db_result


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
    _session(context).add(replica)


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
def get_migrations(context, include_tasks=False,
                   include_info=False, to_dict=True):
    q = _soft_delete_aware_query(context, models.Migration)
    if include_tasks:
        q = _get_migration_task_query_options(q)
    else:
        q = q.options(orm.joinedload("executions"))
    if include_info is False:
        q = q.options(orm.defer('info'))

    args = {}
    if is_user_context(context):
        args["project_id"] = context.tenant
    result = q.filter_by(**args).all()
    if to_dict:
        return [i.to_dict(
            include_info=include_info,
            include_tasks=include_tasks) for i in result]
    return result


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
    _session(context).add(migration)


@enginefacade.writer
def delete_migration(context, migration_id):
    _delete_transfer_action(context, models.Migration, migration_id)


@enginefacade.writer
def set_execution_status(
        context, execution_id, status, update_action_status=True):
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
    if update_action_status:
        set_action_last_execution_status(
            context, execution.action_id, status)


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
def set_action_last_execution_status(
        context, action_id, last_execution_status):
    action = get_action(context, action_id)
    action.last_execution_status = last_execution_status


@enginefacade.writer
def update_transfer_action_info_for_instance(
        context, action_id, instance, new_instance_info):
    """ Updates the info for the given action with the provided dict.
    Returns the updated value.
    Sub-fields of the dict already in the info will get overwritten entirely!
    """
    action = get_action(context, action_id)
    if not new_instance_info:
        LOG.debug(
            "No new info provided for action '%s' and instance '%s'. "
            "Nothing to update in the DB.",
            action_id, instance)
        return action.info.get(instance, {})

    # Copy is needed, otherwise sqlalchemy won't save the changes
    action_info = action.info.copy()
    if instance in action_info:
        instance_info_old = action_info[instance]
        old_keys = set(instance_info_old.keys())
        new_keys = set(new_instance_info.keys())
        overwritten_keys = old_keys.intersection(new_keys)
        if overwritten_keys:
            LOG.debug(
                "Overwriting the values of the following keys for info of "
                "instance '%s' of action with ID '%s': %s",
                instance, action_id, overwritten_keys)
        newly_added_keys = new_keys.difference(old_keys)
        if newly_added_keys:
            LOG.debug(
                "The following new keys will be added for info of instance "
                "'%s' in action with ID '%s': %s",
                instance, action_id, newly_added_keys)

        instance_info_old_copy = instance_info_old.copy()
        instance_info_old_copy.update(new_instance_info)
        action_info[instance] = instance_info_old_copy
    action.info = action_info

    return action_info[instance]


@enginefacade.writer
def set_transfer_action_result(context, action_id, instance, result):
    """ Adds the result for the given 'instance' in the 'transfer_result'
    JSON in the 'base_transfer_action' table.
    """
    action = get_action(context, action_id)

    transfer_result = {}
    if action.transfer_result:
        transfer_result = action.transfer_result.copy()
    transfer_result[instance] = result
    action.transfer_result = transfer_result

    return transfer_result[instance]


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
def set_task_host_properties(context, task_id, host=None, process_id=None):
    task = _get_task(context, task_id)
    if host:
        task.host = host
    if process_id:
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
    _session(context).add(task_event)


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
        _session(context).add(task_progress_update)

    task_progress_update.task_id = task_id
    task_progress_update.current_step = current_step
    task_progress_update.total_steps = total_steps
    task_progress_update.message = message


@enginefacade.writer
def update_replica(context, replica_id, updated_values):
    replica = get_replica(context, replica_id)
    if not replica:
        raise exception.NotFound("Replica not found")

    mapped_info_fields = {
        'destination_environment': 'target_environment'}

    updateable_fields = [
        "source_environment", "destination_environment", "notes",
        "network_map", "storage_mappings"]
    for field in updateable_fields:
        if mapped_info_fields.get(field, field) in updated_values:
            LOG.debug(
                "Updating the '%s' field of Replica '%s' to: '%s'",
                field, replica_id, updated_values[
                    mapped_info_fields.get(field, field)])
            setattr(
                replica, field,
                updated_values[mapped_info_fields.get(field, field)])

    non_updateable_fields = set(
        updated_values.keys()).difference({
            mapped_info_fields.get(field, field)
            for field in updateable_fields})
    if non_updateable_fields:
        LOG.warn(
            "The following Replica fields can NOT be updated: %s",
            non_updateable_fields)

    # the oslo_db library uses this method for both the `created_at` and
    # `updated_at` fields
    setattr(replica, 'updated_at', timeutils.utcnow())


@enginefacade.writer
def add_region(context, region):
    _session(context).add(region)


@enginefacade.reader
def get_regions(context):
    q = _soft_delete_aware_query(context, models.Region)
    q = q.options(orm.joinedload('mapped_endpoints'))
    q = q.options(orm.joinedload('mapped_services'))
    return q.all()


@enginefacade.reader
def get_region(context, region_id):
    q = _soft_delete_aware_query(context, models.Region)
    q = q.options(orm.joinedload('mapped_endpoints'))
    q = q.options(orm.joinedload('mapped_services'))
    return q.filter(
        models.Region.id == region_id).first()


@enginefacade.writer
def update_region(context, region_id, updated_values):
    if not region_id:
        raise exception.InvalidInput(
            "No region ID specified for updating.")
    region = get_region(context, region_id)
    if not region:
        raise exception.NotFound(
            "Region with ID '%s' does not exist." % region_id)

    updateable_fields = ["name", "description", "enabled"]
    _update_sqlalchemy_object_fields(
        region, updateable_fields, updated_values)


@enginefacade.writer
def delete_region(context, region_id):
    region = get_region(context, region_id)
    count = _soft_delete_aware_query(context, models.Region).filter_by(
        id=region_id).soft_delete()
    if count == 0:
        raise exception.NotFound("0 region entries were soft deleted")
    # NOTE(aznashwan): many-to-many tables with soft deletion on either end of
    # the association are not handled properly so we must manually delete each
    # association ourselves:
    for endp in region.mapped_endpoints:
        delete_endpoint_region_mapping(context, endp.id, region_id)
    for svc in region.mapped_services:
        delete_service_region_mapping(context, svc.id, region_id)

@enginefacade.writer
def add_endpoint_region_mapping(context, endpoint_region_mapping):
    region_id = endpoint_region_mapping.region_id
    endpoint_id = endpoint_region_mapping.endpoint_id

    if None in [region_id, endpoint_id]:
        raise exception.InvalidInput(
            "Provided endpoint region mapping params for the region ID "
            "('%s') and the endpoint ID ('%s') must both be non-null." % (
                region_id, endpoint_id))

    _session(context).add(endpoint_region_mapping)


@enginefacade.reader
def get_endpoint_region_mapping(context, endpoint_id, region_id):
    q = _soft_delete_aware_query(context, models.EndpointRegionMapping)
    q = q.filter(
        models.EndpointRegionMapping.region == region_id)
    q = q.filter(
        models.EndpointRegionMapping.endpoint_id == endpoint_id)
    return q.all()


@enginefacade.writer
def delete_endpoint_region_mapping(context, endpoint_id, region_id):
    args = {"endpoint_id": endpoint_id, "region_id": region_id}
    # TODO(aznashwan): many-to-many realtionships have no sane way of
    # supporting soft deletion from the sqlalchemy layer wihout
    # writing join condictions, so we hard-`delete()` instead of
    # `soft_delete()` util we find a better option:
    count = _soft_delete_aware_query(
        context, models.EndpointRegionMapping).filter_by(
            **args).delete()
    if count == 0:
        raise exception.NotFound(
            "There is no mapping between endpoint '%s' and region '%s'." % (
                endpoint_id, region_id))
    LOG.debug(
        "Deleted mapping between endpoint '%s' and region '%s' from DB",
        endpoint_id, region_id)


@enginefacade.reader
def get_region_mappings_for_endpoint(
        context, endpoint_id, enabled_regions_only=False):
    q = _soft_delete_aware_query(context, models.EndpointRegionMapping)
    q = q.join(models.Region)
    q = q.filter(
        models.EndpointRegionMapping.endpoint_id == endpoint_id)
    if enabled_regions_only:
        q = q.filter(
            models.Region.enabled == True)
    return q.all()


@enginefacade.reader
def get_mapped_endpoints_for_region(context, region_id):
    q = _soft_delete_aware_query(context, models.Endpoint)
    q = q.join(models.EndpointRegionMapping)
    q = q.filter(
        models.EndpointRegionMapping.endpoint_id == region_id)
    return q.all()


@enginefacade.writer
def add_service(context, service):
    _session(context).add(service)


@enginefacade.reader
def get_services(context):
    q = _soft_delete_aware_query(context, models.Service).options(
        orm.joinedload('mapped_regions'))
    return q.all()


@enginefacade.reader
def get_service(context, service_id):
    q = _soft_delete_aware_query(context, models.Service).options(
        orm.joinedload('mapped_regions'))
    return q.filter(
        models.Service.id == service_id).first()


@enginefacade.reader
def find_service(context, host, binary, topic=None):
    args = {"host": host, "binary": binary}
    if topic:
        args["topic"] = topic
    q = _soft_delete_aware_query(context, models.Service).options(
         orm.joinedload('mapped_regions')).filter_by(**args)
    return q.first()


@enginefacade.writer
def update_service(context, service_id, updated_values):
    if not service_id:
        raise exception.InvalidInput(
            "No service ID specified for updating.")
    service = get_service(context, service_id)
    if not service:
        raise exception.NotFound(
            "Service with ID '%s' does not exist." % service_id)

    if not isinstance(updated_values, dict):
        raise exception.InvalidInput(
            "Update payload for services must be a dict. Got the following "
            "(type: %s): %s" % (type(updated_values), updated_values))

    def _try_unmap_regions(region_ids):
         for region_to_unmap in region_ids:
            try:
                LOG.debug(
                    "Attempting to unmap region '%s' from service '%s'",
                    region_to_unmap, service_id)
                delete_service_region_mapping(
                    context, service_id, region_to_unmap)
            except Exception as ex:
                LOG.warn(
                    "Exception occurred while attempting to unmap region '%s' "
                    "from service '%s'. Ignoring. Error was: %s",
                    region_to_unmap, service_id,
                    utils.get_exception_details())

    newly_mapped_regions = []
    regions_to_unmap = []
    # NOTE: `.pop()` required for  `_update_sqlalchemy_object_fields` call:
    desired_region_mappings = updated_values.pop('mapped_regions', None)
    if desired_region_mappings is not None:
        # ensure all requested regions exist:
        for region_id in desired_region_mappings:
            region = get_region(context, region_id)
            if not region:
                raise exception.NotFound(
                    "Could not find region with ID '%s' for associating "
                    "with serce '%s' during update process." % (
                        region_id, service_id))

        # get all existing mappings:
        existing_region_mappings = [
            mapping.region_id
            for mapping in get_region_mappings_for_service(
                context, service_id)]

        # check and add new mappings:
        to_map = set(
            desired_region_mappings).difference(set(existing_region_mappings))
        regions_to_unmap = set(
            existing_region_mappings).difference(set(desired_region_mappings))

        LOG.debug(
            "Remapping regions for service '%s' from %s to %s",
            service_id, existing_region_mappings, desired_region_mappings)

        region_id = None
        try:
            for region_id in to_map:
                mapping = models.ServiceRegionMapping()
                mapping.region_id = region_id
                mapping.service_id = service_id
                add_service_region_mapping(context, mapping)
                newly_mapped_regions.append(region_id)
        except Exception as ex:
            LOG.warn(
                "Exception occurred while adding region mapping for '%s' to "
                "service '%s'. Cleaning up created mappings (%s). Error was: "
                "%s", region_id, service_id, newly_mapped_regions,
                utils.get_exception_details())
            _try_unmap_regions(newly_mapped_regions)
            raise


    updateable_fields = ["enabled", "status", "providers", "specs"]
    try:
        _update_sqlalchemy_object_fields(
            service, updateable_fields, updated_values)
    except Exception as ex:
        LOG.warn(
            "Exception occurred while updating fields of service '%s'. "
            "Cleaning ""up created mappings (%s). Error was: %s",
            service_id, newly_mapped_regions, utils.get_exception_details())
        _try_unmap_regions(newly_mapped_regions)
        raise

    # remove all of the old region mappings:
    LOG.debug(
        "Unmapping the following regions during update of service '%s': %s",
        service_id, regions_to_unmap)
    _try_unmap_regions(regions_to_unmap)


@enginefacade.writer
def delete_service(context, service_id):
    service = get_service(context, service_id)
    count = _soft_delete_aware_query(context, models.Service).filter_by(
        id=service_id).soft_delete()
    if count == 0:
        raise exception.NotFound("0 service entries were soft deleted")
    # NOTE(aznashwan): many-to-many tables with soft deletion on either end of
    # the association are not handled properly so we must manually delete each
    # association ourselves:
    for reg in service.mapped_regions:
        delete_service_region_mapping(context, service_id, reg.id)


@enginefacade.writer
def add_service_region_mapping(context, service_region_mapping):
    region_id = service_region_mapping.region_id
    service_id = service_region_mapping.service_id

    if None in [region_id, service_id]:
        raise exception.InvalidInput(
            "Provided service region mapping params for the region ID "
            "('%s') and the service ID ('%s') must both be non-null." % (
                region_id, service_id))

    _session(context).add(service_region_mapping)


@enginefacade.reader
def get_service_region_mapping(context, service_id, region_id):
    q = _soft_delete_aware_query(context, models.ServiceRegionMapping)
    q = q.filter(
        models.ServiceRegionMapping.region == region_id)
    q = q.filter(
        models.ServiceRegionMapping.service_id == service_id)
    return q.all()


@enginefacade.writer
def delete_service_region_mapping(context, service_id, region_id):
    args = {"service_id": service_id, "region_id": region_id}
    # TODO(aznashwan): many-to-many realtionships have no sane way of
    # supporting soft deletion from the sqlalchemy layer wihout
    # writing join condictions, so we hard-`delete()` instead of
    # `soft_delete()` util we find a better option:
    count = _soft_delete_aware_query(
        context, models.ServiceRegionMapping).filter_by(
            **args).delete()
    if count == 0:
        raise exception.NotFound(
            "There is no mapping between service '%s' and region '%s'." % (
                service_id, region_id))


@enginefacade.reader
def get_region_mappings_for_service(
        context, service_id, enabled_regions_only=False):
    q = _soft_delete_aware_query(context, models.ServiceRegionMapping)
    q = q.join(models.Region)
    q = q.filter(
        models.ServiceRegionMapping.service_id == service_id)
    if enabled_regions_only:
        q = q.filter(
            models.Region.enabled == True)
    return q.all()


@enginefacade.reader
def get_mapped_services_for_region(context, region_id):
    q = _soft_delete_aware_query(context, models.Service)
    q = q.join(models.ServiceRegionMapping)
    q = q.filter(
        models.ServiceRegionMapping.service_id == region_id)
    return q.all()
