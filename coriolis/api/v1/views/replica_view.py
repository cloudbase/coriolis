# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools

from oslo_config import cfg as conf

from coriolis.api.v1.views import replica_tasks_execution_view as view


REPLICA_API_OPTS = [
    conf.BoolOpt("include_task_info_in_replicas_api",
                 default=False,
                 help="Whether or not to expose the internal 'info' field of "
                      "a Replica as part of a `GET` request.")]

CONF = conf.CONF
CONF.register_opts(REPLICA_API_OPTS)


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

    if not CONF.include_task_info_in_replicas_api and (
            "info" in replica_dict):
        replica_dict.pop("info")

    return replica_dict


def single(req, replica):
    return {"replica": _format_replica(req, replica)}


def collection(req, replicas):
    formatted_replicas = [_format_replica(req, m)
                          for m in replicas]
    return {'replicas': formatted_replicas}
