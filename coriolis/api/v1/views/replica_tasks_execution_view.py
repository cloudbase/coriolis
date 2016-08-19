# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_replica_tasks_execution(req, execution, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    return dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in execution.items()))


def single(req, execution):
    return {"execution": _format_replica_tasks_execution(req, execution)}


def collection(req, executions):
    formatted_executions = [_format_replica_tasks_execution(req, m)
                            for m in executions]
    return {'executions': formatted_executions}
