# Copyright 2019 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import exception

from oslo_config import cfg
from oslo_cache import core as cache

opts = [
    cfg.BoolOpt('caching', default=False),
    cfg.IntOpt('cache_time', default=7200),
]

CONF = cfg.CONF
CONF.register_opts(opts)
cache.configure(CONF)
cache_region = cache.create_region()
cache.configure_cache_region(CONF, cache_region)


def get_cache_decorator(provider):
    if type(provider) is not str or provider == "":
        raise exception.CoriolisException(
            "Invalid provider name")
    MEMOIZE = cache.get_memoization_decorator(
        CONF, cache_region, provider)
    return MEMOIZE
