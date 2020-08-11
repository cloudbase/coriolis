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


class SchedulerClient(object):
    def __init__(self, timeout=None):
        target = messaging.Target(topic='coriolis_scheduler', version=VERSION)
        if timeout is None:
            timeout = CONF.scheduler.scheduler_rpc_timeout
        self._client = rpc.get_client(target, timeout=timeout)

    def get_diagnostics(self, ctxt):
        return self._client.call(ctxt, 'get_diagnostics')

    def get_workers_for_specs(
            self, ctxt, provider_requirements=None,
            region_ids=None, enabled=None):
        return self._client.call(
            ctxt, 'get_workers_for_specs', region_ids=region_ids,
            enabled=enabled, provider_requirements=provider_requirements)

    '''
    def get_workers_for_action(
            self, ctxt, endpoint_type, provider_type, region_ids=None):
        return self._client.call(
            ctxt, 'get_workers_for_action', endpoint_type=endpoint_type,
            provider_type=provider_type, region_ids=region_ids)

    def get_workers_for_task(
            self, ctxt, task_type, source_endpoint_type,
            destination_endpoint_type, source_region_ids=None,
            destination_region_ids=None):
        return self._client.call(
            ctxt, 'get_workers_for_task', task_type=task_type,
            source_endpoint_type=source_endpoint_type,
            destination_endpoint_type=destination_endpoint_type,
            source_region_ids=source_region_ids,
            destination_region_ids=destination_region_ids)
    '''
