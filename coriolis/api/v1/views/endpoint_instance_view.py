# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_instance(req, instance, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    return dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in instance.items()))


def single(req, instance):
    return {"instance": _format_instance(req, instance)}


def collection(req, instances):
    formatted_instances = [_format_instance(req, m)
                           for m in instances]
    return {'instances': formatted_instances}
