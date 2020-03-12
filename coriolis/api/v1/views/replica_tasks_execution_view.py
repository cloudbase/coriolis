# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools

from oslo_config import cfg as conf
from oslo_log import log as logging

from coriolis import constants
from coriolis import utils


LOG = logging.getLogger(__name__)


REPLICA_EXECUTION_API_OPTS = [
    conf.BoolOpt("include_task_info_in_replica_executions_api",
                 default=False,
                 help="Whether or not to expose the internal 'info' field of "
                      "a Replica execution as part of a `GET` request.")]

CONF = conf.CONF
CONF.register_opts(REPLICA_EXECUTION_API_OPTS)


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


def format_replica_tasks_execution(req, execution, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    if "tasks" in execution:
        execution["tasks"] = _sort_tasks(execution["tasks"])

    execution_dict = dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in execution.items()))

    if not CONF.include_task_info_in_replica_executions_api and (
            "action" in execution_dict):
        action_dict = execution_dict["action"]
        if "info" in action_dict:
            action_dict.pop("info")

    return execution_dict


def single(req, execution):
    return {"execution": format_replica_tasks_execution(req, execution)}


def collection(req, executions):
    formatted_executions = [format_replica_tasks_execution(req, m)
                            for m in executions]
    return {'executions': formatted_executions}
