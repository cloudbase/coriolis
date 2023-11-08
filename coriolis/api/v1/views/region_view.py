# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import utils as view_utils


def _format_region(region, keys=None):
    region_dict = view_utils.format_opt(region, keys)

    mapped_endpoints = region_dict.get('mapped_endpoints', [])
    region_dict['mapped_endpoints'] = [
        endp['id'] for endp in mapped_endpoints]

    mapped_services = region_dict.get('mapped_services', [])
    region_dict['mapped_services'] = [
        svc['id'] for svc in mapped_services]

    return region_dict


def single(region, keys=None):
    return {"region": _format_region(region, keys)}


def collection(regions, keys=None):
    formatted_regions = [
        _format_region(r, keys) for r in regions]
    return {'regions': formatted_regions}
