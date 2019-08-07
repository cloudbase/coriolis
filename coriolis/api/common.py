# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import utils


def get_paging_params(req):
    marker = req.GET.get("marker")
    limit = req.GET.get("limit")
    if limit is not None:
        limit = utils.parse_int_value(limit)
    return marker, limit


def get_force_param(req):
    force_param = req.GET.get('force', '')
    return (str(force_param).lower() == "true")
