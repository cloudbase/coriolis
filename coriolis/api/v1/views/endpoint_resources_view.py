# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_resource(req, resource, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    return dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in resource.items()))


def instance_single(req, instance):
    return {"instance": _format_resource(req, instance)}


def instances_collection(req, instances):
    formatted_instances = [_format_resource(req, m)
                           for m in instances]
    return {'instances': formatted_instances}


def network_single(req, network):
    return {"network": _format_resource(req, network)}


def networks_collection(req, networks):
    formatted_networks = [_format_resource(req, m) for m in networks]
    return {'networks': formatted_networks}


def storage_collection(req, storage):
    formatted_storages = _format_resource(req, storage)
    return {'storage': formatted_storages}
