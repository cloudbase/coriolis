# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_config import cfg
import oslo_messaging as messaging

from coriolis import rpc

VERSION = "1.0"

scheduler_opts = [
    cfg.IntOpt("scheduler_rpc_timeout",
               help="Number of seconds until RPC calls to the "
                    "scheduler timeout.")
]

CONF = cfg.CONF
CONF.register_opts(scheduler_opts, 'scheduler')


class SchedulerClient(rpc.BaseRPCClient):
    def __init__(self, timeout=None):
        target = messaging.Target(topic='coriolis_scheduler', version=VERSION)
        if timeout is None:
            timeout = CONF.scheduler.scheduler_rpc_timeout
        super(SchedulerClient, self).__init__(
            target, timeout=timeout)

    def get_diagnostics(self, ctxt):
        return self._call(ctxt, 'get_diagnostics')

    def get_workers_for_specs(
            self, ctxt, provider_requirements=None,
            region_sets=None, enabled=None):
        return self._call(
            ctxt, 'get_workers_for_specs', region_sets=region_sets,
            enabled=enabled, provider_requirements=provider_requirements)
