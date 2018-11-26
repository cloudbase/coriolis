# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_storage(req, storage, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    return dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in storage.items()))


def collection(req, storage):
    formatted_storages = _format_storage(req, storage)
    return {'storage': formatted_storages}
