# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_region(req, region, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    region_dict = dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in region.items()))

    mapped_endpoints = region_dict.get('mapped_endpoints', [])
    region_dict['mapped_endpoints'] = [
        endp['endpoint_id'] for endp in mapped_endpoints]

    return region_dict


def single(req, region):
    return {"region": _format_region(req, region)}


def collection(req, regions):
    formatted_regions = [
        _format_region(req, r) for r in regions]
    return {'regions': formatted_regions}
