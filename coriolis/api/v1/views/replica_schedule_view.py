# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools


def format_schedule(req, schedule, keys=None):
    def transform(key, value):
        if keys and key not in keys:
            return
        yield (key, value)

    return dict(itertools.chain.from_iterable(
        transform(k, v) for k, v in schedule.items()))


def single(req, schedule):
    return {"schedule": format_schedule(req, schedule)}


def collection(req, schedules):
    formatted_schedules = [format_schedule(req, m)
                           for m in schedules]
    return {'schedules': formatted_schedules}
