# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_endpoint(req, endpoint, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    endpoint_dict = dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in endpoint.items()))
    mapped_regions = endpoint_dict.get('mapped_regions', [])
    endpoint_dict['mapped_regions'] = [
        reg['id'] for reg in mapped_regions]

    return endpoint_dict


def single(req, endpoint):
    return {"endpoint": _format_endpoint(req, endpoint)}


def collection(req, endpoints):
    formatted_endpoints = [_format_endpoint(req, m)
                           for m in endpoints]
    return {'endpoints': formatted_endpoints}
