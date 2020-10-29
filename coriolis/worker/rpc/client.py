# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_config import cfg
import oslo_messaging as messaging

from coriolis import constants
from coriolis import rpc

VERSION = "1.0"


worker_opts = [
    cfg.IntOpt("worker_rpc_timeout",
               help="Number of seconds until RPC calls to the worker timeout.")
]

CONF = cfg.CONF
CONF.register_opts(worker_opts, 'worker')


class WorkerClient(rpc.BaseRPCClient):
    def __init__(
            self, timeout=None, host=None,
            base_worker_topic=constants.WORKER_MAIN_MESSAGING_TOPIC):
        topic = base_worker_topic
        if host is not None:
            topic = constants.SERVICE_MESSAGING_TOPIC_FORMAT % ({
                "main_topic": base_worker_topic,
                "host": host})
        target = messaging.Target(topic=topic, version=VERSION)
        if timeout is None:
            timeout = CONF.worker.worker_rpc_timeout
        super(WorkerClient, self).__init__(
            target, timeout=timeout)

    def begin_task(self, ctxt, task_id, task_type, origin, destination,
                   instance, task_info):
        self._cast(
            ctxt, 'exec_task', task_id=task_id, task_type=task_type,
            origin=origin, destination=destination, instance=instance,
            task_info=task_info, asynchronous=True)

    def run_task(self, ctxt, server, task_id, task_type, origin, destination,
                 instance, task_info):
        cctxt = self._client.prepare(server=server)
        cctxt.cast(
            ctxt, 'exec_task', task_id=task_id, task_type=task_type,
            origin=origin, destination=destination, instance=instance,
            task_info=task_info, asynchronous=False)

    def cancel_task(self, ctxt, task_id, process_id, force):
        return self._call(
            ctxt, 'cancel_task',
            task_id=task_id, process_id=process_id, force=force)

    def get_endpoint_instances(self, ctxt, platform_name, connection_info,
                               source_environment, marker=None, limit=None,
                               instance_name_pattern=None):
        return self._call(
            ctxt, 'get_endpoint_instances',
            platform_name=platform_name,
            connection_info=connection_info,
            source_environment=source_environment,
            marker=marker,
            limit=limit,
            instance_name_pattern=instance_name_pattern)

    def get_endpoint_instance(self, ctxt, platform_name, connection_info,
                              source_environment, instance_name):
        return self._call(
            ctxt, 'get_endpoint_instance',
            platform_name=platform_name,
            connection_info=connection_info,
            source_environment=source_environment,
            instance_name=instance_name)

    def get_endpoint_destination_options(
            self, ctxt, platform_name, connection_info, env, option_names):
        return self._call(
            ctxt, 'get_endpoint_destination_options',
            platform_name=platform_name,
            connection_info=connection_info,
            env=env,
            option_names=option_names)

    def get_endpoint_source_options(
            self, ctxt, platform_name, connection_info, env, option_names):
        return self._call(
            ctxt, 'get_endpoint_source_options',
            platform_name=platform_name,
            connection_info=connection_info,
            env=env,
            option_names=option_names)

    def get_endpoint_networks(self, ctxt, platform_name, connection_info, env):
        return self._call(
            ctxt, 'get_endpoint_networks',
            platform_name=platform_name,
            connection_info=connection_info,
            env=env)

    def validate_endpoint_connection(self, ctxt, platform_name,
                                     connection_info):
        return self._call(
            ctxt, 'validate_endpoint_connection',
            platform_name=platform_name,
            connection_info=connection_info)

    def validate_endpoint_target_environment(
            self, ctxt, platform_name, target_env):
        return self._call(
            ctxt, 'validate_endpoint_target_environment',
            platform_name=platform_name,
            target_env=target_env)

    def validate_endpoint_source_environment(
            self, ctxt, platform_name, source_env):
        return self._call(
            ctxt, 'validate_endpoint_source_environment',
            platform_name=platform_name,
            source_env=source_env)

    def get_endpoint_storage(self, ctxt, platform_name, connection_info, env):
        return self._call(
            ctxt, 'get_endpoint_storage',
            platform_name=platform_name,
            connection_info=connection_info,
            env=env)

    def get_available_providers(self, ctxt):
        return self._call(
            ctxt, 'get_available_providers')

    def get_provider_schemas(self, ctxt, platform_name, provider_type):
        return self._call(
            ctxt, 'get_provider_schemas',
            platform_name=platform_name,
            provider_type=provider_type)

    def get_diagnostics(self, ctxt):
        return self._call(ctxt, 'get_diagnostics')

    def get_service_status(self, ctxt):
        return self._call(ctxt, 'get_service_status')

    def get_endpoint_source_minion_pool_options(
            self, ctxt, platform_name, connection_info, env, option_names):
        return self._call(
            ctxt, 'get_endpoint_source_minion_pool_options',
            platform_name=platform_name, connection_info=connection_info,
            env=env, option_names=option_names)

    def get_endpoint_destination_minion_pool_options(
            self, ctxt, platform_name, connection_info, env, option_names):
        return self._call(
            ctxt, 'get_endpoint_destination_minion_pool_options',
            platform_name=platform_name, connection_info=connection_info,
            env=env, option_names=option_names)

    def validate_endpoint_source_minion_pool_options(
            self, ctxt, platform_name, pool_environment):
        return self._call(
            ctxt, 'validate_endpoint_source_minion_pool_options',
            platform_name=platform_name, pool_environment=pool_environment)

    def validate_endpoint_destination_minion_pool_options(
            self, ctxt, platform_name, pool_environment):
        return self._call(
            ctxt, 'validate_endpoint_destination_minion_pool_options',
            platform_name=platform_name, pool_environment=pool_environment)
