# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from typing import Any

import oslo_messaging as messaging
from oslo_config import cfg

from coriolis import constants, rpc

VERSION = "1.0"


worker_opts = [
    cfg.IntOpt(
        "worker_rpc_timeout",
        help="Number of seconds until RPC calls to the worker timeout.",
    )
]

CONF = cfg.CONF
CONF.register_opts(worker_opts, 'worker')


class WorkerClient(rpc.BaseRPCClient):
    def __init__(
        self,
        timeout: int | None = None,
        host: str | None = None,
        base_worker_topic: str = constants.WORKER_MAIN_MESSAGING_TOPIC,
    ):
        topic = base_worker_topic
        if host is not None:
            topic = constants.SERVICE_MESSAGING_TOPIC_FORMAT % (
                {"main_topic": base_worker_topic, "host": host}
            )
        target = messaging.Target(topic=topic, version=VERSION)
        if timeout is None:
            timeout = CONF.worker.worker_rpc_timeout
        super(WorkerClient, self).__init__(target, timeout=timeout)

    @classmethod
    def from_service_definition(
        cls,
        service,
        timeout: int | None = None,
        topic_override=None,
    ) -> "WorkerClient":
        if service.get('topic') != constants.WORKER_MAIN_MESSAGING_TOPIC:
            raise ValueError(
                "Unknown topic '%s' for worker service client. Only "
                "acceptable value is '%s': %s"
                % (service.get('topic'), constants.WORKER_MAIN_MESSAGING_TOPIC, service)
            )
        topic = constants.WORKER_MAIN_MESSAGING_TOPIC
        if topic_override:
            topic = topic_override
        return cls(timeout=timeout, base_worker_topic=topic, host=service.get('host'))

    def begin_task(
        self,
        ctxt,
        task_id: str,
        task_type: str,
        origin: dict,
        destination: dict,
        instance: str,
        task_info: dict,
    ) -> None:
        self._cast(
            ctxt,
            'exec_task',
            task_id=task_id,
            task_type=task_type,
            origin=origin,
            destination=destination,
            instance=instance,
            task_info=task_info,
            report_to_conductor=True,
        )

    def run_task(
        self,
        ctxt,
        task_id: str,
        task_type: str,
        origin: dict,
        destination: dict,
        instance: str,
        task_info: dict,
    ) -> dict:
        return self._call(
            ctxt,
            'exec_task',
            task_id=task_id,
            task_type=task_type,
            origin=origin,
            destination=destination,
            instance=instance,
            task_info=task_info,
            report_to_conductor=False,
        )

    def cancel_task(
        self,
        ctxt,
        task_id: str,
        process_id: int,
        force: bool,
    ) -> None:
        return self._call(
            ctxt, 'cancel_task', task_id=task_id, process_id=process_id, force=force
        )

    def get_endpoint_instances(
        self,
        ctxt,
        platform_name: str,
        connection_info: dict[str, str],
        source_environment: dict[Any, Any],
        marker=None,
        limit=None,
        instance_name_pattern: str | None = None,
        refresh: bool = False,
    ) -> list[dict]:
        return self._call(
            ctxt,
            'get_endpoint_instances',
            platform_name=platform_name,
            connection_info=connection_info,
            source_environment=source_environment,
            marker=marker,
            limit=limit,
            instance_name_pattern=instance_name_pattern,
            refresh=refresh,
        )

    def get_endpoint_instance(
        self,
        ctxt,
        platform_name: str,
        connection_info: dict[str, str],
        source_environment: dict[Any, Any],
        instance_name: str,
    ) -> dict:
        return self._call(
            ctxt,
            'get_endpoint_instance',
            platform_name=platform_name,
            connection_info=connection_info,
            source_environment=source_environment,
            instance_name=instance_name,
        )

    def get_endpoint_destination_options(
        self,
        ctxt,
        platform_name: str,
        connection_info: dict[str, str],
        env: dict[Any, Any],
        option_names: dict[Any, Any],
    ) -> list[dict]:
        return self._call(
            ctxt,
            'get_endpoint_destination_options',
            platform_name=platform_name,
            connection_info=connection_info,
            env=env,
            option_names=option_names,
        )

    def get_endpoint_source_options(
        self,
        ctxt,
        platform_name: str,
        connection_info: dict[str, str],
        env: dict[Any, Any],
        option_names: dict[Any, Any],
    ) -> list[dict]:
        return self._call(
            ctxt,
            'get_endpoint_source_options',
            platform_name=platform_name,
            connection_info=connection_info,
            env=env,
            option_names=option_names,
        )

    def get_endpoint_networks(
        self,
        ctxt,
        platform_name: str,
        connection_info: dict[str, str],
        env: dict[Any, Any],
    ) -> list[dict]:
        return self._call(
            ctxt,
            'get_endpoint_networks',
            platform_name=platform_name,
            connection_info=connection_info,
            env=env,
        )

    def validate_endpoint_connection(
        self,
        ctxt,
        platform_name: str,
        connection_info: dict[str, str],
    ) -> tuple:
        return self._call(
            ctxt,
            'validate_endpoint_connection',
            platform_name=platform_name,
            connection_info=connection_info,
        )

    def validate_endpoint_target_environment(
        self,
        ctxt,
        platform_name: str,
        target_env: dict,
    ) -> tuple[bool, None]:
        return self._call(
            ctxt,
            'validate_endpoint_target_environment',
            platform_name=platform_name,
            target_env=target_env,
        )

    def validate_endpoint_source_environment(
        self,
        ctxt,
        platform_name: str,
        source_env: dict,
    ) -> tuple[bool, None]:
        return self._call(
            ctxt,
            'validate_endpoint_source_environment',
            platform_name=platform_name,
            source_env=source_env,
        )

    def get_endpoint_storage(
        self,
        ctxt,
        platform_name: str,
        connection_info: dict[str, str],
        env: dict[Any, Any],
    ) -> dict[str, list]:
        return self._call(
            ctxt,
            'get_endpoint_storage',
            platform_name=platform_name,
            connection_info=connection_info,
            env=env,
        )

    def get_endpoint_inventory_csv(
        self,
        ctxt,
        platform_name: str,
        connection_info: dict[str, str],
        source_environment: dict[Any, Any],
    ) -> str:
        return self._call(
            ctxt,
            'get_endpoint_inventory_csv',
            platform_name=platform_name,
            connection_info=connection_info,
            source_environment=source_environment,
        )

    def get_available_providers(self, ctxt) -> dict[str, dict]:
        return self._call(ctxt, 'get_available_providers')

    def get_provider_schemas(
        self,
        ctxt,
        platform_name: str,
        provider_type: int,
    ) -> dict[str, dict]:
        return self._call(
            ctxt,
            'get_provider_schemas',
            platform_name=platform_name,
            provider_type=provider_type,
        )

    def get_diagnostics(self, ctxt) -> dict:
        return self._call(ctxt, 'get_diagnostics')

    def get_service_status(self, ctxt) -> dict:
        return self._call(ctxt, 'get_service_status')

    def get_endpoint_source_minion_pool_options(
        self,
        ctxt,
        platform_name: str,
        connection_info: dict[str, str],
        env: dict[Any, Any],
        option_names: dict[Any, Any],
    ) -> list[dict]:
        return self._call(
            ctxt,
            'get_endpoint_source_minion_pool_options',
            platform_name=platform_name,
            connection_info=connection_info,
            env=env,
            option_names=option_names,
        )

    def get_endpoint_destination_minion_pool_options(
        self,
        ctxt,
        platform_name: str,
        connection_info: dict[str, str],
        env: dict[Any, Any],
        option_names: dict[Any, Any],
    ) -> list[dict]:
        return self._call(
            ctxt,
            'get_endpoint_destination_minion_pool_options',
            platform_name=platform_name,
            connection_info=connection_info,
            env=env,
            option_names=option_names,
        )

    def validate_endpoint_source_minion_pool_options(
        self,
        ctxt,
        platform_name: str,
        pool_environment: dict[Any, Any],
    ) -> tuple[bool, None]:
        return self._call(
            ctxt,
            'validate_endpoint_source_minion_pool_options',
            platform_name=platform_name,
            pool_environment=pool_environment,
        )

    def validate_endpoint_destination_minion_pool_options(
        self,
        ctxt,
        platform_name: str,
        pool_environment: dict[Any, Any],
    ) -> tuple[bool, None]:
        return self._call(
            ctxt,
            'validate_endpoint_destination_minion_pool_options',
            platform_name=platform_name,
            pool_environment=pool_environment,
        )

    def execute_auto_deployment(self, ctxt, transfer_id, deployer_execution, **kwargs):
        self._cast(
            ctxt,
            'execute_auto_deployment',
            transfer_id=transfer_id,
            deployer_execution=deployer_execution,
            **kwargs,
        )
