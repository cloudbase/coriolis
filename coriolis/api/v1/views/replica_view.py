# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_replica(req, replica, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    return dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in replica.items()))


def single(req, replica):
    return {"replica": _format_replica(req, replica)}


def collection(req, replicas):
    formatted_replicas = [_format_replica(req, m)
                          for m in replicas]
    return {'replicas': formatted_replicas}
