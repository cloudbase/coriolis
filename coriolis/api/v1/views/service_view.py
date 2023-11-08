# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import utils as view_utils


def _format_service(service, keys=None):
    service_dict = view_utils.format_opt(service, keys)

    mapped_regions = service_dict.get('mapped_regions', [])
    service_dict['mapped_regions'] = [
        mapping['id'] for mapping in mapped_regions]

    return service_dict


def single(service, keys=None):
    return {"service": _format_service(service, keys)}


def collection(services, keys=None):
    formatted_services = [
        _format_service(r, keys) for r in services]
    return {'services': formatted_services}
