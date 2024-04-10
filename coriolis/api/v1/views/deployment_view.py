# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import replica_tasks_execution_view as view
from coriolis.api.v1.views import utils as view_utils


def _format_deployment(deployment, keys=None):
    deployment_dict = view_utils.format_opt(deployment, keys)

    if len(deployment_dict.get("executions", [])):
        execution = view.format_replica_tasks_execution(
            deployment_dict["executions"][0], keys)
        del deployment_dict["executions"]
    else:
        execution = {}

    tasks = execution.get("tasks")
    if tasks:
        deployment_dict["tasks"] = tasks

    return deployment_dict


def single(deployment, keys=None):
    return {"deployment": _format_deployment(deployment, keys)}


def collection(deployments, keys=None):
    formatted_deployments = [_format_deployment(m, keys)
                             for m in deployments]
    return {'deployments': formatted_deployments}
