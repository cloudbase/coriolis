# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_minion_pool(req, minion_pool, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    minion_pool_dict = dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in minion_pool.items()))

    # TODO(aznashwan): remove these redundancies once the base
    # DB action model hirearchy will be overhauled:
    for key in ["origin_endpoint_id", "destination_endpoint_id"]:
        if key in minion_pool_dict:
            minion_pool_dict["endpoint_id"] = minion_pool_dict.pop(key)
    for key in ["source_environment", "destination_environment"]:
        if key in minion_pool_dict:
            minion_pool_dict["environment_options"] = minion_pool_dict.pop(key)

    return minion_pool_dict


def single(req, minion_pool):
    return {"minion_pool": _format_minion_pool(req, minion_pool)}


def collection(req, minion_pools):
    formatted_minion_pools = [
        _format_minion_pool(req, r) for r in minion_pools]
    return {'minion_pools': formatted_minion_pools}
