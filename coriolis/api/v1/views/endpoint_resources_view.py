# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import utils as view_utils


def instance_single(instance, keys=None):
    return {"instance": view_utils.format_opt(instance, keys)}


def instances_collection(instances, keys=None):
    formatted_instances = [view_utils.format_opt(m, keys)
                           for m in instances]
    return {'instances': formatted_instances}


def network_single(network, keys=None):
    return {"network": view_utils.format_opt(network, keys)}


def networks_collection(networks, keys=None):
    formatted_networks = [view_utils.format_opt(m, keys)
                          for m in networks]
    return {'networks': formatted_networks}


def storage_collection(storage, keys=None):
    formatted_storages = view_utils.format_opt(storage, keys)
    return {'storage': formatted_storages}
