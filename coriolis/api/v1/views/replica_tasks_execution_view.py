# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools

from coriolis import constants
from coriolis import utils


def _sort_tasks(tasks):
    non_error_only_tasks = [t for t in tasks if
                            not t["depends_on"] and not t["on_error"]]

    def _add_non_error_tasks(task_id):
        for t in tasks:
            if (t["depends_on"] and task_id in t["depends_on"] and
                    t not in non_error_only_tasks):
                non_error_only_tasks.append(t)
                _add_non_error_tasks(t["id"])

    for t in non_error_only_tasks:
        _add_non_error_tasks(t["id"])

    # Include error only tasks only if executed
    error_only_tasks = [t for t in tasks if t["status"] !=
                        constants.TASK_STATUS_ON_ERROR_ONLY and
                        t not in non_error_only_tasks]

    sorted_tasks = utils.topological_graph_sorting(
        non_error_only_tasks, sort_key="task_type")
    sorted_tasks += utils.topological_graph_sorting(
        error_only_tasks, sort_key="task_type")
    return sorted_tasks


def format_replica_tasks_execution(req, execution, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    if "tasks" in execution:
        execution["tasks"] = _sort_tasks(execution["tasks"])

    return dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in execution.items()))


def single(req, execution):
    return {"execution": format_replica_tasks_execution(req, execution)}


def collection(req, executions):
    formatted_executions = [format_replica_tasks_execution(req, m)
                            for m in executions]
    return {'executions': formatted_executions}
