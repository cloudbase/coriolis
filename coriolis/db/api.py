# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import uuid

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


def _update_sqlalchemy_object_fields(
        obj, updateable_fields, values_to_update):
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


def _get_transfer_schedules_filter(context, transfer_id=None,
                                   schedule_id=None, expired=True):
    now = timeutils.utcnow()
    q = _soft_delete_aware_query(context, models.TransferSchedule)
    q = q.join(models.Transfer)
    sched_filter = q.filter()
    if is_user_context(context):
        sched_filter = sched_filter.filter(
            models.Transfer.project_id == context.project_id)

    if transfer_id:
        sched_filter = sched_filter.filter(
            models.Transfer.id == transfer_id)
    if schedule_id:
        sched_filter = sched_filter.filter(
            models.TransferSchedule.id == schedule_id)
    if not expired:
        sched_filter = sched_filter.filter(
            or_(models.TransferSchedule.expiration_date == null(),
                models.TransferSchedule.expiration_date > now))
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
            models.Endpoint.project_id == context.project_id)
    return q.filter().all()


@enginefacade.reader
def get_endpoint(context, endpoint_id):
    q = _soft_delete_aware_query(context, models.Endpoint).options(
        orm.joinedload('mapped_regions'))
    if is_user_context(context):
        q = q.filter(
            models.Endpoint.project_id == context.project_id)
    return q.filter(
        models.Endpoint.id == endpoint_id).first()


@enginefacade.writer
def add_endpoint(context, endpoint):
    endpoint.user_id = context.user
    endpoint.project_id = context.project_id
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
            except Exception:
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
        except Exception:
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
    except Exception:
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
        args["project_id"] = context.project_id
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
def get_transfer_tasks_executions(context, transfer_id, include_tasks=False,
                                  include_task_info=False, to_dict=False):
    q = _soft_delete_aware_query(context, models.TasksExecution)
    q = q.join(models.Transfer)
    if include_task_info:
        q = q.options(orm.joinedload('action').undefer('info'))
    if include_tasks:
        q = _get_tasks_with_details_options(q)
    if is_user_context(context):
        q = q.filter(models.Transfer.project_id == context.project_id)

    db_result = q.filter(
        models.Transfer.id == transfer_id).all()
    if to_dict:
        return [e.to_dict() for e in db_result]
    return db_result


@enginefacade.reader
def get_transfer_tasks_execution(context, transfer_id, execution_id,
                                 include_task_info=False, to_dict=False):
    q = _soft_delete_aware_query(context, models.TasksExecution).join(
        models.Transfer)
    if include_task_info:
        q = q.options(orm.joinedload('action').undefer('info'))
    q = _get_tasks_with_details_options(q)
    if is_user_context(context):
        q = q.filter(models.Transfer.project_id == context.project_id)

    db_result = q.filter(
        models.Transfer.id == transfer_id,
        models.TasksExecution.id == execution_id).first()
    if to_dict and db_result is not None:
        return db_result.to_dict()
    return db_result


@enginefacade.writer
def add_transfer_tasks_execution(context, execution):
    if is_user_context(context):
        if execution.action.project_id != context.project_id:
            raise exception.NotAuthorized()

    # include deleted records
    max_number = _model_query(
        context,
        func.max(
            models.TasksExecution.number)).filter(
                models.TasksExecution.action_id == (
                    execution.action.id)).first()[0] or 0
    execution.number = max_number + 1

    _session(context).add(execution)


@enginefacade.writer
def delete_transfer_tasks_execution(context, execution_id):
    q = _soft_delete_aware_query(context, models.TasksExecution).filter(
        models.TasksExecution.id == execution_id)
    if is_user_context(context):
        if not q.join(models.Transfer).filter(
                models.Transfer.project_id == context.project_id).first():
            raise exception.NotAuthorized()
    count = q.soft_delete()
    if count == 0:
        raise exception.NotFound("0 entries were soft deleted")


@enginefacade.reader
def get_transfer_schedules(context, transfer_id=None, expired=True):
    sched_filter = _get_transfer_schedules_filter(
        context, transfer_id=transfer_id, expired=expired)
    return sched_filter.all()


@enginefacade.reader
def get_transfer_schedule(context, transfer_id, schedule_id, expired=True):
    sched_filter = _get_transfer_schedules_filter(
        context, transfer_id=transfer_id, schedule_id=schedule_id,
        expired=expired)
    return sched_filter.first()


@enginefacade.writer
def update_transfer_schedule(context, transfer_id, schedule_id,
                             updated_values, pre_update_callable=None,
                             post_update_callable=None):
    # NOTE(gsamfira): we need to refactor the DB layer a bit to allow
    # two-phase transactions or at least allow running these functions
    # inside a single transaction block.
    schedule = get_transfer_schedule(context, transfer_id, schedule_id)
    if pre_update_callable:
        pre_update_callable(schedule=schedule)
    updateable_attributes = [
        "schedule", "expiration_date", "enabled", "shutdown_instance",
        "auto_deploy"]
    for val in updateable_attributes:
        if val in updated_values:
            setattr(schedule, val, updated_values[val])
    if post_update_callable:
        # at this point nothing has really been sent to the DB,
        # but we may need to act upon the new changes elsewhere
        # before we actually commit to the database
        post_update_callable(context, schedule)


@enginefacade.writer
def delete_transfer_schedule(context, transfer_id,
                             schedule_id, pre_delete_callable=None,
                             post_delete_callable=None):
    # NOTE(gsamfira): we need to refactor the DB layer a bit to allow
    # two-phase transactions or at least allow running these functions
    # inside a single transaction block.

    q = _soft_delete_aware_query(context, models.TransferSchedule).filter(
        models.TransferSchedule.id == schedule_id,
        models.TransferSchedule.transfer_id == transfer_id)
    schedule = q.first()
    if not schedule:
        raise exception.NotFound(
            "No such schedule")
    if is_user_context(context):
        if not q.join(models.Transfer).filter(
                models.Transfer.project_id == context.project_id).first():
            raise exception.NotAuthorized()
    if pre_delete_callable:
        pre_delete_callable(context, schedule)
    count = q.soft_delete()
    if post_delete_callable:
        post_delete_callable(context, schedule)
    if count == 0:
        raise exception.NotFound("0 entries were soft deleted")


@enginefacade.writer
def add_transfer_schedule(context, schedule, post_create_callable=None):
    # NOTE(gsamfira): we need to refactor the DB layer a bit to allow
    # two-phase transactions or at least allow running these functions
    # inside a single transaction block.

    if schedule.transfer.project_id != context.project_id:
        raise exception.NotAuthorized()
    _session(context).add(schedule)
    if post_create_callable:
        post_create_callable(context, schedule)


def _get_transfer_with_tasks_executions_options(q):
    return q.options(orm.joinedload(models.Transfer.executions))


@enginefacade.reader
def get_transfers(context,
                  transfer_scenario=None,
                  include_tasks_executions=False,
                  include_task_info=False,
                  to_dict=False):
    q = _soft_delete_aware_query(context, models.Transfer)
    if include_tasks_executions:
        q = _get_transfer_with_tasks_executions_options(q)
    if include_task_info:
        q = q.options(orm.undefer('info'))
    q = q.filter()
    if transfer_scenario:
        q = q.filter(models.Transfer.scenario == transfer_scenario)
    if is_user_context(context):
        q = q.filter(
            models.Transfer.project_id == context.project_id)
    db_result = q.all()
    if to_dict:
        return [
            i.to_dict(
                include_task_info=include_task_info,
                include_executions=include_tasks_executions)
            for i in db_result]
    return db_result


@enginefacade.reader
def get_transfer(context, transfer_id,
                 transfer_scenario=None,
                 include_task_info=False,
                 to_dict=False):
    q = _soft_delete_aware_query(context, models.Transfer)
    q = _get_transfer_with_tasks_executions_options(q)
    if include_task_info:
        q = q.options(orm.undefer('info'))
    if transfer_scenario:
        q = q.filter(
            models.Transfer.scenario == transfer_scenario)
    if is_user_context(context):
        q = q.filter(
            models.Transfer.project_id == context.project_id)

    transfer = q.filter(
        models.Transfer.id == transfer_id).first()
    if to_dict and transfer is not None:
        return transfer.to_dict(include_task_info=include_task_info)

    return transfer


@enginefacade.reader
def get_endpoint_transfers_count(
        context, endpoint_id, transfer_scenario=None):

    scenario_filter_kwargs = {}
    if transfer_scenario:
        scenario_filter_kwargs = {"scenario": transfer_scenario}

    origin_args = {'origin_endpoint_id': endpoint_id}
    origin_args.update(scenario_filter_kwargs)
    q_origin_count = _soft_delete_aware_query(
        context, models.Transfer).filter_by(**origin_args).count()

    destination_args = {'destination_endpoint_id': endpoint_id}
    destination_args.update(scenario_filter_kwargs)
    q_destination_count = _soft_delete_aware_query(
        context, models.Transfer).filter_by(**destination_args).count()

    return q_origin_count + q_destination_count


@enginefacade.writer
def add_transfer(context, transfer):
    transfer.user_id = context.user
    transfer.project_id = context.project_id
    _session(context).add(transfer)


@enginefacade.writer
def _delete_transfer_action(context, cls, id):
    args = {"base_id": id}
    if is_user_context(context):
        args["project_id"] = context.project_id
    count = _soft_delete_aware_query(context, cls).filter_by(
        **args).soft_delete()
    if count == 0:
        raise exception.NotFound("0 entries were soft deleted")

    _soft_delete_aware_query(context, models.TasksExecution).filter_by(
        action_id=id).soft_delete()


@enginefacade.writer
def delete_transfer(context, transfer_id):
    _delete_transfer_action(context, models.Transfer, transfer_id)


@enginefacade.reader
def get_transfer_deployments(context, transfer_id):
    q = _soft_delete_aware_query(context, models.Deployment)
    q = q.join("transfer")
    q = q.options(orm.joinedload("executions"))
    if is_user_context(context):
        q = q.filter(
            models.Deployment.project_id == context.project_id)
    return q.filter(
        models.Transfer.id == transfer_id).all()


@enginefacade.reader
def get_deployments(context,
                    include_tasks=False,
                    include_task_info=False,
                    to_dict=False):
    q = _soft_delete_aware_query(context, models.Deployment)
    if include_tasks:
        q = _get_deployment_task_query_options(q)
    else:
        q = q.options(orm.joinedload("executions"))
    if include_task_info:
        q = q.options(orm.undefer('info'))

    args = {}
    if is_user_context(context):
        args["project_id"] = context.project_id
    result = q.filter_by(**args).all()
    if to_dict:
        return [i.to_dict(
            include_task_info=include_task_info,
            include_tasks=include_tasks) for i in result]
    return result


def _get_tasks_with_details_options(query):
    return query.options(
        orm.joinedload("action")).options(
            orm.joinedload("tasks").
            joinedload("progress_updates")).options(
                orm.joinedload("tasks").
                joinedload("events"))


def _get_deployment_task_query_options(query):
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
def get_deployment(context, deployment_id, include_task_info=False,
                   to_dict=False):
    q = _soft_delete_aware_query(context, models.Deployment)
    q = _get_deployment_task_query_options(q)
    if include_task_info:
        q = q.options(orm.undefer('info'))
    args = {"id": deployment_id}
    if is_user_context(context):
        args["project_id"] = context.project_id
    db_result = q.filter_by(**args).first()

    if to_dict and db_result is not None:
        return db_result.to_dict(include_task_info=include_task_info)
    return db_result


@enginefacade.writer
def add_deployment(context, deployment):
    deployment.user_id = context.user or deployment.transfer.user_id
    deployment.project_id = (
        context.project_id or deployment.transfer.project_id)
    _session(context).add(deployment)


@enginefacade.writer
def delete_deployment(context, deployment_id):
    _delete_transfer_action(context, models.Deployment, deployment_id)


@enginefacade.writer
def set_execution_status(
        context, execution_id, status, update_action_status=True):
    execution = _soft_delete_aware_query(
        context, models.TasksExecution).join(
            models.TasksExecution.action)
    if is_user_context(context):
        execution = execution.filter(
            models.BaseTransferAction.project_id == context.project_id)
    execution = execution.filter(
        models.TasksExecution.id == execution_id).first()
    if not execution:
        raise exception.NotFound(
            "Tasks execution not found: %s" % execution_id)

    execution.status = status
    if update_action_status:
        set_action_last_execution_status(
            context, execution.action_id, status)
    return execution


@enginefacade.reader
def get_action(context, action_id, include_task_info=False):
    action = _soft_delete_aware_query(
        context, models.BaseTransferAction)
    if include_task_info:
        action = action.options(orm.undefer('info'))
    if is_user_context(context):
        action = action.filter(
            models.BaseTransferAction.project_id == context.project_id)
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
    action = get_action(context, action_id, include_task_info=True)
    if not new_instance_info:
        LOG.debug(
            "No new info provided for action '%s' and instance '%s'. "
            "Nothing to update in the DB.",
            action_id, instance)
        return action.info.get(instance, {})

    # Copy is needed, otherwise sqlalchemy won't save the changes
    action_info = action.info.copy()
    instance_info_old = {}
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
            models.BaseTransferAction.project_id == context.project_id)
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
    task_event.id = str(uuid.uuid4())
    task_event.index = 0
    last_event = _get_last_task_event(context, task_id)
    if last_event:
        task_event.index = last_event.index + 1
    task_event.task_id = task_id
    task_event.level = level
    task_event.message = message
    _session(context).add(task_event)
    return task_event


@enginefacade.reader
def _get_last_task_event(context, task_id):
    q = _soft_delete_aware_query(
        context, models.TaskEvent)
    last_event = q.filter(
        models.TaskEvent.task_id == task_id).order_by(
            models.TaskEvent.index.desc()).first()
    return last_event


@enginefacade.reader
def _get_last_task_progress_update(context, task_id):
    q = _soft_delete_aware_query(
        context, models.TaskProgressUpdate)
    last_update = q.filter(
        models.TaskProgressUpdate.task_id == task_id).order_by(
            models.TaskProgressUpdate.index.desc()).first()
    return last_update


@enginefacade.reader
def _get_last_minion_pool_event(context, pool_id):
    q = _soft_delete_aware_query(
        context, models.MinionPoolEvent)
    last_event = q.filter(
        models.MinionPoolEvent.pool_id == pool_id).order_by(
            models.MinionPoolEvent.index.desc()).first()
    return last_event


@enginefacade.reader
def _get_last_minion_pool_progress_update(context, pool_id):
    q = _soft_delete_aware_query(
        context, models.MinionPoolProgressUpdate)
    last_event = q.filter(
        models.MinionPoolProgressUpdate.pool_id == pool_id).order_by(
            models.MinionPoolProgressUpdate.index.desc()).first()
    return last_event


@enginefacade.writer
def add_minion_pool_event(context, pool_id, level, message):
    pool_event = models.MinionPoolEvent()
    pool_event.id = str(uuid.uuid4())
    pool_event.pool_id = pool_id
    pool_event.level = level
    pool_event.message = message

    pool_event.index = 0
    last_pool_event = _get_last_minion_pool_event(context, pool_id)
    if last_pool_event:
        pool_event.index = last_pool_event.index + 1

    _session(context).add(pool_event)
    return pool_event


def _get_minion_pool_progress_update(context, pool_id, index):
    q = _soft_delete_aware_query(context, models.MinionPoolProgressUpdate)
    return q.filter(
        models.MinionPoolProgressUpdate.pool_id == pool_id,
        models.MinionPoolProgressUpdate.index == index).first()


@enginefacade.writer
def add_minion_pool_progress_update(
        context, pool_id, message, initial_step=0, total_steps=0):
    pool_progress_update = models.MinionPoolProgressUpdate()
    pool_progress_update.id = str(uuid.uuid4())
    pool_progress_update.pool_id = pool_id
    pool_progress_update.current_step = initial_step
    pool_progress_update.total_steps = total_steps
    pool_progress_update.message = message
    pool_progress_update.index = 0
    last_progress_update = _get_last_minion_pool_progress_update(
        context, pool_id)
    if last_progress_update:
        pool_progress_update.index = last_progress_update.index + 1

    _session(context).add(pool_progress_update)
    return pool_progress_update


@enginefacade.writer
def update_minion_pool_progress_update(
        context, pool_id, update_index, new_current_step,
        new_total_steps=None, new_message=None):
    pool_progress_update = _get_minion_pool_progress_update(
        context, pool_id, update_index)
    if not pool_progress_update:
        raise exception.NotFound(
            "Could not find progress update for minion pool with ID '%s' and "
            "index %s in the DB for updating." % (pool_id, update_index))

    pool_progress_update.current_step = new_current_step
    if new_total_steps is not None:
        pool_progress_update.total_steps = new_total_steps
    if new_message is not None:
        pool_progress_update.message = new_message
    return pool_progress_update


def _get_progress_update(context, task_id, index):
    q = _soft_delete_aware_query(context, models.TaskProgressUpdate)
    return q.filter(
        models.TaskProgressUpdate.task_id == task_id,
        models.TaskProgressUpdate.index == index).first()


@enginefacade.writer
def add_task_progress_update(
        context, task_id, message, initial_step=0, total_steps=0):
    task_progress_update = models.TaskProgressUpdate()
    task_event_id = str(uuid.uuid4())
    task_progress_update.id = task_event_id
    task_progress_update.task_id = task_id
    task_progress_update.current_step = initial_step
    task_progress_update.total_steps = total_steps
    max_msg_len = models.MAX_EVENT_MESSAGE_LENGHT
    if len(message) > max_msg_len:
        LOG.warn(
            f"Progress message for task '{task_id}' with ID '{task_event_id}'"
            f"is too long. Truncating before insertion. "
            f"Original message was: '{message}'")
        message = f"{message[:max_msg_len-len('...')]}..."
    task_progress_update.message = message

    task_progress_update.index = 0
    last_progress_update = _get_last_task_progress_update(context, task_id)
    if last_progress_update:
        task_progress_update.index = last_progress_update.index + 1

    _session(context).add(task_progress_update)
    return task_progress_update


@enginefacade.writer
def update_task_progress_update(
        context, task_id, update_index, new_current_step,
        new_total_steps=None, new_message=None):
    task_progress_update = _get_progress_update(
        context, task_id, update_index)
    if not task_progress_update:
        raise exception.NotFound(
            "Could not find progress update for task with ID '%s' and "
            "index %s in the DB for updating." % (task_id, update_index))

    task_progress_update.current_step = new_current_step
    if new_total_steps is not None:
        task_progress_update.total_steps = new_total_steps
    if new_message is not None:
        max_msg_len = models.MAX_EVENT_MESSAGE_LENGHT
        if len(new_message) > max_msg_len:
            task_event_id = task_progress_update.id
            LOG.warn(
                f"Progress message for task '{task_id}' with ID "
                f"'{task_event_id}' is too long. Truncating before insertion."
                f" Original message was: '{new_message}'")
            new_message = f"{new_message[:max_msg_len-len('...')]}..."
        task_progress_update.message = new_message


@enginefacade.writer
def update_transfer(context, transfer_id, updated_values):
    transfer = get_transfer(context, transfer_id)
    if not transfer:
        raise exception.NotFound("Transfer not found")

    mapped_info_fields = {
        'destination_environment': 'target_environment'}

    updateable_fields = [
        "source_environment", "destination_environment", "notes",
        "network_map", "storage_mappings",
        "origin_minion_pool_id", "destination_minion_pool_id",
        "instance_osmorphing_minion_pool_mappings", "clone_disks",
        "skip_os_morphing"]
    for field in updateable_fields:
        if mapped_info_fields.get(field, field) in updated_values:
            LOG.debug(
                "Updating the '%s' field of Transfer '%s' to: '%s'",
                field, transfer_id, updated_values[
                    mapped_info_fields.get(field, field)])
            setattr(
                transfer, field,
                updated_values[mapped_info_fields.get(field, field)])

    non_updateable_fields = set(
        updated_values.keys()).difference({
            mapped_info_fields.get(field, field)
            for field in updateable_fields})
    if non_updateable_fields:
        LOG.warn(
            "The following Transfer fields can NOT be updated: %s",
            non_updateable_fields)

    # the oslo_db library uses this method for both the `created_at` and
    # `updated_at` fields
    setattr(transfer, 'updated_at', timeutils.utcnow())


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
        models.EndpointRegionMapping.region_id == region_id)
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
            models.Region.enabled == True)  # noqa: E712
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
            except Exception:
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
        except Exception:
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
    except Exception:
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
        models.ServiceRegionMapping.region_id == region_id)
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
            models.Region.enabled == True)  # noqa: E712
    return q.all()


@enginefacade.reader
def get_mapped_services_for_region(context, region_id):
    q = _soft_delete_aware_query(context, models.Service)
    q = q.join(models.ServiceRegionMapping)
    q = q.filter(
        models.ServiceRegionMapping.service_id == region_id)
    return q.all()


@enginefacade.writer
def add_minion_machine(context, minion_machine):
    minion_machine.user_id = context.user
    minion_machine.project_id = context.project_id
    # inherit pool user/tenant if none are given:
    if None in [minion_machine.user_id, minion_machine.project_id]:
        pool = get_minion_pool(context, minion_machine.pool_id)
        if not minion_machine.user_id:
            minion_machine.user_id = pool.user_id
        if not minion_machine.project_id:
            minion_machine.project_id = pool.project_id
    _session(context).add(minion_machine)


@enginefacade.reader
def get_minion_machines(context, allocated_action_id=None):
    q = _soft_delete_aware_query(context, models.MinionMachine)
    if allocated_action_id:
        q = q.filter(
            models.MinionMachine.allocated_action == allocated_action_id)
    return q.all()


@enginefacade.reader
def get_minion_machine(context, minion_machine_id):
    q = _soft_delete_aware_query(context, models.MinionMachine)
    return q.filter(
        models.MinionMachine.id == minion_machine_id).first()


@enginefacade.writer
def update_minion_machine(context, minion_machine_id, updated_values):
    if not minion_machine_id:
        raise exception.InvalidInput(
            "No minion_machine ID specified for updating.")
    minion_machine = get_minion_machine(context, minion_machine_id)
    if not minion_machine:
        raise exception.NotFound(
            "MinionMachine with ID '%s' does not exist." % minion_machine_id)

    updateable_fields = [
        "connection_info", "provider_properties", "allocation_status",
        "backup_writer_connection_info", "allocated_action",
        "last_used_at", "power_status"]
    _update_sqlalchemy_object_fields(
        minion_machine, updateable_fields, updated_values)


@enginefacade.writer
def set_minion_machine_allocation_status(context, minion_machine_id, status):
    machine = get_minion_machine(context, minion_machine_id)
    if not machine:
        raise exception.NotFound(
            "Minion machine with ID '%s' not found" % minion_machine_id)
    LOG.debug(
        "Transitioning minion machine '%s' (pool '%s') from status '%s' to "
        "'%s' in the DB",
        minion_machine_id, machine.pool_id, machine.allocation_status, status)
    machine.allocation_status = status
    setattr(machine, 'updated_at', timeutils.utcnow())


@enginefacade.writer
def set_minion_machines_allocation_statuses(
        context, minion_machine_ids, action_id, allocation_status,
        refresh_allocation_time=True):
    machines = get_minion_machines(context)
    existing_machine_id_mappings = {
        machine.id: machine for machine in machines}
    missing = [
        mid for mid in minion_machine_ids
        if mid not in existing_machine_id_mappings]
    if missing:
        raise exception.NotFound(
            "The following minion machines could not be found: %s" % (
                missing))

    for machine_id in minion_machine_ids:
        machine = existing_machine_id_mappings[machine_id]
        LOG.debug(
            "Changing allocation status in DB for minion machine '%s' "
            "from '%s' to '%s' and allocated action from '%s' to '%s'" % (
                machine.id, machine.allocation_status, allocation_status,
                machine.allocated_action, action_id))
        machine.allocated_action = action_id
        if refresh_allocation_time:
            machine.last_used_at = timeutils.utcnow()
        machine.allocation_status = allocation_status


@enginefacade.writer
def delete_minion_machine(context, minion_machine_id):
    # TODO(aznashwan): update models to be soft-delete-aware to
    # avoid needing to hard-delete here:
    count = _soft_delete_aware_query(context, models.MinionMachine).filter_by(
        id=minion_machine_id).delete()
    if count == 0:
        raise exception.NotFound("0 MinionMachine entries were soft deleted")


@enginefacade.writer
def add_minion_pool(context, minion_pool):
    minion_pool.user_id = context.user
    minion_pool.project_id = context.project_id
    _session(context).add(minion_pool)


@enginefacade.writer
def delete_minion_pool(context, minion_pool_id):
    args = {"id": minion_pool_id}
    if is_user_context(context):
        args["project_id"] = context.project_id
    count = _soft_delete_aware_query(context, models.MinionPool).filter_by(
        **args).soft_delete()
    if count == 0:
        raise exception.NotFound("0 entries were soft deleted")


@enginefacade.reader
def get_minion_pool(
        context, minion_pool_id, include_machines=False,
        include_events=False, include_progress_updates=False):
    q = _soft_delete_aware_query(context, models.MinionPool)
    if include_machines:
        q = q.options(orm.selectinload('minion_machines'))
    if include_events:
        q = q.options(orm.selectinload('events'))
    if include_progress_updates:
        q = q.options(orm.selectinload('progress_updates'))
    if is_user_context(context):
        q = q.filter(
            models.MinionPool.project_id == context.project_id)
    return q.filter(
        models.MinionPool.id == minion_pool_id).first()


@enginefacade.reader
def get_minion_pools(
        context, include_machines=False, include_events=False,
        include_progress_updates=False, to_dict=True):
    q = _soft_delete_aware_query(context, models.MinionPool)
    q = q.filter()
    if is_user_context(context):
        q = q.filter(
            models.MinionPool.project_id == context.project_id)
    if include_machines:
        q = q.options(orm.joinedload('minion_machines'))
    if include_events:
        q = q.options(orm.joinedload('events'))
    if include_progress_updates:
        q = q.options(orm.joinedload('progress_updates'))
    db_result = q.all()
    if to_dict:
        return [
            i.to_dict(
                include_machines=include_machines,
                include_events=include_events,
                include_progress_updates=include_progress_updates)
            for i in db_result]
    return db_result


@enginefacade.writer
def set_minion_pool_status(context, minion_pool_id, status):
    pool = get_minion_pool(
        context, minion_pool_id, include_machines=False)
    if not pool:
        raise exception.NotFound(
            "Minion pool '%s' not found" % minion_pool_id)
    LOG.debug(
        "Transitioning minion pool '%s' from status '%s' to '%s' in DB",
        minion_pool_id, pool.status, status)
    pool.status = status
    setattr(pool, 'updated_at', timeutils.utcnow())


@enginefacade.writer
def update_minion_pool(context, minion_pool_id, updated_values):
    lifecycle = get_minion_pool(
        context, minion_pool_id, include_machines=False)
    if not lifecycle:
        raise exception.NotFound(
            "Minion pool '%s' not found" % minion_pool_id)

    updateable_fields = [
        "minimum_minions", "maximum_minions", "minion_max_idle_time",
        "minion_retention_strategy", "environment_options",
        "shared_resources", "notes", "name", "os_type"]
    for field in updateable_fields:
        if field in updated_values:
            LOG.debug(
                "Updating the '%s' field of Minion Pool '%s' to: '%s'",
                field, minion_pool_id, updated_values[field])
            setattr(lifecycle, field, updated_values[field])

    non_updateable_fields = set(
        updated_values.keys()).difference(updateable_fields)
    if non_updateable_fields:
        LOG.warn(
            "The following Minion Pool fields can NOT be updated: %s",
            non_updateable_fields)

    # the oslo_db library uses this method for both the `created_at` and
    # `updated_at` fields
    setattr(lifecycle, 'updated_at', timeutils.utcnow())
