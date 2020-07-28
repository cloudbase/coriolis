# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def _format_service(req, service, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    service_dict = dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in service.items()))

    mapped_regions = service_dict.get('mapped_regions', [])
    service_dict['mapped_regions'] = [
        mapping['region_id'] for mapping in mapped_regions]

    return service_dict


def single(req, service):
    return {"service": _format_service(req, service)}


def collection(req, services):
    formatted_services = [
        _format_service(req, r) for r in services]
    return {'services': formatted_services}
