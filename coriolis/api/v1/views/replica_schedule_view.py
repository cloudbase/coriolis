# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import utils as view_utils


def single(schedule, keys=None):
    return {"schedule": view_utils.format_opt(schedule, keys)}


def collection(schedules, keys=None):
    formatted_schedules = [view_utils.format_opt(m, keys)
                           for m in schedules]
    return {'schedules': formatted_schedules}
