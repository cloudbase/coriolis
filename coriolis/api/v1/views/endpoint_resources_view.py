# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import utils as view_utils


def instance_single(instance: dict, keys=None) -> dict[str, dict]:
    return {"instance": view_utils.format_opt(instance, keys)}


def instances_collection(instances: list[dict], keys=None) -> dict[str, list]:
    formatted_instances = [view_utils.format_opt(m, keys) for m in instances]
    return {'instances': formatted_instances}


def network_single(network, keys=None):
    return {"network": view_utils.format_opt(network, keys)}


def networks_collection(networks: list[dict], keys=None) -> dict[str, list]:
    formatted_networks = [view_utils.format_opt(m, keys) for m in networks]
    return {'networks': formatted_networks}


def storage_collection(storage: dict[str, list], keys=None) -> dict[str, dict]:
    formatted_storages = view_utils.format_opt(storage, keys)
    return {'storage': formatted_storages}
