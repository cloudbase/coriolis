# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.


from oslo_log import log as logging

from coriolis.api.v1.views import utils as view_utils
from coriolis import constants


LOG = logging.getLogger(__name__)


def _sort_tasks(tasks, filter_error_only_tasks=True):
    """
    Sorts the given list of dicts representing tasks.
    Tasks are sorted primarily based on their index.
    """
    if filter_error_only_tasks:
        tasks = [
            t for t in tasks
            if t['status'] != (
                constants.TASK_STATUS_ON_ERROR_ONLY)]
    return sorted(
        tasks, key=lambda t: t.get('index', 0))


def format_replica_tasks_execution(execution, keys=None):
    if "tasks" in execution:
        execution["tasks"] = _sort_tasks(execution["tasks"])

    execution_dict = view_utils.format_opt(execution, keys)

    return execution_dict


def single(execution, keys=None):
    return {"execution": format_replica_tasks_execution(execution, keys)}


def collection(executions, keys=None):
    formatted_executions = [format_replica_tasks_execution(m, keys)
                            for m in executions]
    return {'executions': formatted_executions}
