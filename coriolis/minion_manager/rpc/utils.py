# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import functools

from oslo_concurrency import lockutils

from coriolis import constants


def get_minion_pool_lock(minion_pool_id, external=True):
    return lockutils.lock(
        constants.MINION_POOL_LOCK_NAME_FORMAT % minion_pool_id,
        external=external)


def minion_pool_synchronized(minion_pool_id, func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        @lockutils.synchronized(
            constants.MINION_POOL_LOCK_NAME_FORMAT % minion_pool_id,
            external=True)
        def inner():
            return func(*args, **kwargs)
        return inner()
    return wrapper


def minion_pool_synchronized_op(func):
    @functools.wraps(func)
    def wrapper(self, ctxt, minion_pool_id, *args, **kwargs):
        return minion_pool_synchronized(minion_pool_id, func)(
            self, ctxt, minion_pool_id, *args, **kwargs)
    return wrapper


def minion_machine_synchronized(minion_pool_id, minion_machine_id, func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        @lockutils.synchronized(
            constants.MINION_MACHINE_LOCK_NAME_FORMAT % (
                minion_pool_id, minion_machine_id),
            external=True)
        def inner():
            return func(*args, **kwargs)
        return inner()
    return wrapper
