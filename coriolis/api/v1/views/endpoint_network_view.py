# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_network(req, network, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    return dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in network.items()))


def single(req, network):
    return {"network": _format_network(req, network)}


def collection(req, networks):
    formatted_networks = [_format_network(req, m) for m in networks]
    return {'networks': formatted_networks}
