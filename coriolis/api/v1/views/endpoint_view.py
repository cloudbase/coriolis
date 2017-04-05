# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_endpoint(req, endpoint, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    return dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in endpoint.items()))


def single(req, endpoint):
    return {"endpoint": _format_endpoint(req, endpoint)}


def collection(req, endpoints):
    formatted_endpoints = [_format_endpoint(req, m)
                           for m in endpoints]
    return {'endpoints': formatted_endpoints}
