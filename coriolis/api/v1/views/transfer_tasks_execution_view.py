# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.


from oslo_log import log as logging

from coriolis import constants
from coriolis.api.v1.views import utils as view_utils

LOG = logging.getLogger(__name__)


def _sort_tasks(tasks: list, filter_error_only_tasks: bool = True) -> list:
    """
    Sorts the given list of dicts representing tasks.
    Tasks are sorted primarily based on their index.
    """
    if filter_error_only_tasks:
        tasks = [
            t for t in tasks if t['status'] != (constants.TASK_STATUS_ON_ERROR_ONLY)
        ]
    return sorted(tasks, key=lambda t: t.get('index', 0))


def format_transfer_tasks_execution(execution: dict, keys=None) -> dict:
    if "tasks" in execution:
        execution["tasks"] = _sort_tasks(execution["tasks"])

    execution_dict = view_utils.format_opt(execution, keys)

    return execution_dict


def single(execution: dict, keys=None) -> dict[str, dict]:
    return {"execution": format_transfer_tasks_execution(execution, keys)}


def collection(executions: list, keys=None) -> dict[str, list]:
    formatted_executions = [
        format_transfer_tasks_execution(m, keys) for m in executions
    ]
    return {'executions': formatted_executions}
