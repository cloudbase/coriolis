# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools

from coriolis.api.v1.views import replica_tasks_execution_view as view


def _format_replica(req, replica, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    replica_dict = dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in replica.items()))

    executions = replica_dict.get('executions', [])
    replica_dict['executions'] = [
        view.format_replica_tasks_execution(req, ex)
        for ex in executions]

    return replica_dict


def single(req, replica):
    return {"replica": _format_replica(req, replica)}


def collection(req, replicas):
    formatted_replicas = [_format_replica(req, m)
                          for m in replicas]
    return {'replicas': formatted_replicas}
