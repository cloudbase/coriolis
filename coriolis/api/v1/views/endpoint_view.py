# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from typing import Any

from coriolis.api.v1.views import utils as view_utils


def _format_endpoint(endpoint, keys=None) -> dict:
    endpoint_dict = view_utils.format_opt(endpoint, keys)
    mapped_regions = endpoint_dict.get('mapped_regions', [])
    endpoint_dict['mapped_regions'] = [reg['id'] for reg in mapped_regions]

    return endpoint_dict


def single(endpoint, keys=None) -> dict[str, dict]:
    return {"endpoint": _format_endpoint(endpoint, keys)}


def collection(endpoints: list[Any], keys=None) -> dict[str, list]:
    formatted_endpoints = [_format_endpoint(m, keys) for m in endpoints]
    return {'endpoints': formatted_endpoints}
