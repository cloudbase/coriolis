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

    return minion_pool_dict


def single(req, minion_pool):
    return {"minion_pool": _format_minion_pool(req, minion_pool)}


def collection(req, minion_pools):
    formatted_minion_pools = [
        _format_minion_pool(req, r) for r in minion_pools]
    return {'minion_pools': formatted_minion_pools}
