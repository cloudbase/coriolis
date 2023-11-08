# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import replica_tasks_execution_view as view
from coriolis.api.v1.views import utils as view_utils


def _format_replica(replica, keys=None):
    replica_dict = view_utils.format_opt(replica, keys)

    executions = replica_dict.get('executions', [])
    replica_dict['executions'] = [
        view.format_replica_tasks_execution(ex)
        for ex in executions]

    return replica_dict


def single(replica, keys=None):
    return {"replica": _format_replica(replica, keys)}


def collection(replicas, keys=None):
    formatted_replicas = [_format_replica(m, keys)
                          for m in replicas]
    return {'replicas': formatted_replicas}
