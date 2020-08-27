# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools

from coriolis import constants
from coriolis import utils


def _sort_tasks(tasks, filter_error_only_tasks=True):
    """ Sorts the given list of dicts representing tasks.
    Tasks are sorted primarily based on their index.
    """
    if filter_error_only_tasks:
        tasks = [
            t for t in tasks
            if t['status'] != (
                constants.TASK_STATUS_ON_ERROR_ONLY)]
    return sorted(
        tasks, key=lambda t: t.get('index', 0))


def format_minion_pool_tasks_execution(req, execution, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    if "tasks" in execution:
        execution["tasks"] = _sort_tasks(execution["tasks"])

    execution_dict = dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in execution.items()))

    return execution_dict


def single(req, execution):
    return {"execution": format_minion_pool_tasks_execution(req, execution)}


def collection(req, executions):
    formatted_executions = [
        format_minion_pool_tasks_execution(req, m) for m in executions]
    return {'executions': formatted_executions}
