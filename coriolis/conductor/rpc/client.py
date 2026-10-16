# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from typing import Any

import oslo_messaging as messaging
from oslo_config import cfg
from oslo_log import log as logging

from coriolis import constants, events, rpc

VERSION = "1.0"
LOG = logging.getLogger(__name__)

conductor_opts = [
    cfg.IntOpt(
        "conductor_rpc_timeout",
        help="Number of seconds until RPC calls to the conductor timeout.",
    )
]

CONF = cfg.CONF
CONF.register_opts(conductor_opts, 'conductor')


class ConductorClient(rpc.BaseRPCClient):
    def __init__(
        self, timeout=None, topic: str = constants.CONDUCTOR_MAIN_MESSAGING_TOPIC
    ):
        target = messaging.Target(topic=topic, version=VERSION)
        if timeout is None:
            timeout = CONF.conductor.conductor_rpc_timeout
        super(ConductorClient, self).__init__(target, timeout=timeout)

    def create_endpoint(
        self,
        ctxt,
        name: str,
        endpoint_type: str,
        description: str,
        connection_info: dict[str, str],
        mapped_regions: list[str],
    ):
        return self._call(
            ctxt,
            'create_endpoint',
            name=name,
            endpoint_type=endpoint_type,
            description=description,
            connection_info=connection_info,
            mapped_regions=mapped_regions,
        )

    def update_endpoint(
        self,
        ctxt,
        endpoint_id: str,
        updated_values: dict,
    ):
        return self._call(
            ctxt,
            'update_endpoint',
            endpoint_id=endpoint_id,
            updated_values=updated_values,
        )

    def get_endpoints(self, ctxt) -> list[Any]:
        return self._call(ctxt, 'get_endpoints')

    def get_endpoint(self, ctxt, endpoint_id: str):
        return self._call(ctxt, 'get_endpoint', endpoint_id=endpoint_id)

    def delete_endpoint(self, ctxt, endpoint_id: str) -> None:
        return self._call(ctxt, 'delete_endpoint', endpoint_id=endpoint_id)

    def get_endpoint_instances(
        self,
        ctxt,
        endpoint_id: str,
        source_environment: dict[Any, Any],
        marker=None,
        limit=None,
        instance_name_pattern: str | None = None,
        refresh: bool = False,
    ) -> list[dict]:
        return self._call(
            ctxt,
            'get_endpoint_instances',
            endpoint_id=endpoint_id,
            source_environment=source_environment,
            marker=marker,
            limit=limit,
            instance_name_pattern=instance_name_pattern,
            refresh=refresh,
        )

    def get_endpoint_instance(
        self,
        ctxt,
        endpoint_id: str,
        source_environment: dict[Any, Any],
        instance_name: str,
    ) -> dict:
        return self._call(
            ctxt,
            'get_endpoint_instance',
            endpoint_id=endpoint_id,
            source_environment=source_environment,
            instance_name=instance_name,
        )

    def get_endpoint_source_options(
        self,
        ctxt,
        endpoint_id: str,
        env: dict[Any, Any],
        option_names: dict[Any, Any],
    ) -> list[dict]:
        return self._call(
            ctxt,
            'get_endpoint_source_options',
            endpoint_id=endpoint_id,
            env=env,
            option_names=option_names,
        )

    def get_endpoint_destination_options(
        self,
        ctxt,
        endpoint_id: str,
        env: dict[Any, Any],
        option_names: dict[Any, Any],
    ) -> list[dict]:
        return self._call(
            ctxt,
            'get_endpoint_destination_options',
            endpoint_id=endpoint_id,
            env=env,
            option_names=option_names,
        )

    def get_endpoint_networks(
        self,
        ctxt,
        endpoint_id: str,
        env: dict[Any, Any],
    ) -> list[dict]:
        return self._call(
            ctxt, 'get_endpoint_networks', endpoint_id=endpoint_id, env=env
        )

    def get_endpoint_storage(
        self,
        ctxt,
        endpoint_id: str,
        env: dict[Any, Any],
    ) -> dict[str, list]:
        return self._call(
            ctxt, 'get_endpoint_storage', endpoint_id=endpoint_id, env=env
        )

    def get_endpoint_inventory_csv(
        self,
        ctxt,
        endpoint_id: str,
        source_environment: dict[Any, Any],
    ) -> str:
        return self._call(
            ctxt,
            'get_endpoint_inventory_csv',
            endpoint_id=endpoint_id,
            source_environment=source_environment,
        )

    def validate_endpoint_connection(self, ctxt, endpoint_id: str) -> tuple:
        return self._call(ctxt, 'validate_endpoint_connection', endpoint_id=endpoint_id)

    def validate_endpoint_target_environment(
        self,
        ctxt,
        endpoint_id: str,
        target_env: dict,
    ) -> tuple[bool, None]:
        return self._call(
            ctxt,
            'validate_endpoint_target_environment',
            endpoint_id=endpoint_id,
            target_env=target_env,
        )

    def validate_endpoint_source_environment(
        self,
        ctxt,
        endpoint_id: str,
        source_env: dict,
    ) -> tuple[bool, None]:
        return self._call(
            ctxt,
            'validate_endpoint_source_environment',
            endpoint_id=endpoint_id,
            source_env=source_env,
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

    def execute_transfer_tasks(
        self,
        ctxt,
        transfer_id: str,
        shutdown_instances: bool = False,
        auto_deploy: bool = False,
    ) -> dict:
        return self._call(
            ctxt,
            'execute_transfer_tasks',
            transfer_id=transfer_id,
            shutdown_instances=shutdown_instances,
            auto_deploy=auto_deploy,
        )

    def get_transfer_tasks_executions(
        self,
        ctxt,
        transfer_id: str,
        include_tasks: bool = False,
        marker: str | None = None,
        limit: int | None = None,
        sort_keys: list | None = None,
        sort_dirs: list | None = None,
        filters: dict | None = None,
    ) -> list:
        return self._call(
            ctxt,
            'get_transfer_tasks_executions',
            transfer_id=transfer_id,
            include_tasks=include_tasks,
            marker=marker,
            limit=limit,
            sort_keys=sort_keys,
            sort_dirs=sort_dirs,
            filters=filters,
        )

    def get_transfer_tasks_execution(
        self,
        ctxt,
        transfer_id: str,
        execution_id: str,
        include_task_info: bool = False,
    ) -> dict:
        return self._call(
            ctxt,
            'get_transfer_tasks_execution',
            transfer_id=transfer_id,
            execution_id=execution_id,
            include_task_info=include_task_info,
        )

    def delete_transfer_tasks_execution(
        self,
        ctxt,
        transfer_id: str,
        execution_id: str,
    ) -> None:
        return self._call(
            ctxt,
            'delete_transfer_tasks_execution',
            transfer_id=transfer_id,
            execution_id=execution_id,
        )

    def cancel_transfer_tasks_execution(
        self,
        ctxt,
        transfer_id: str,
        execution_id: str,
        force: bool,
    ) -> None:
        return self._call(
            ctxt,
            'cancel_transfer_tasks_execution',
            transfer_id=transfer_id,
            execution_id=execution_id,
            force=force,
        )

    def create_instances_transfer(
        self,
        ctxt,
        transfer_scenario: str,
        origin_endpoint_id: str,
        destination_endpoint_id: str,
        origin_minion_pool_id: str | None,
        destination_minion_pool_id: str | None,
        instance_osmorphing_minion_pool_mappings: dict[Any, Any],
        source_environment: dict,
        destination_environment: dict[str, dict],
        instances: list[str],
        network_map: dict[Any, Any],
        storage_mappings: dict[Any, Any],
        notes: str | None = None,
        user_scripts=None,
        clone_disks: bool = True,
        skip_os_morphing: bool = False,
    ) -> dict:
        return self._call(
            ctxt,
            'create_instances_transfer',
            transfer_scenario=transfer_scenario,
            origin_endpoint_id=origin_endpoint_id,
            destination_endpoint_id=destination_endpoint_id,
            origin_minion_pool_id=origin_minion_pool_id,
            destination_minion_pool_id=destination_minion_pool_id,
            instance_osmorphing_minion_pool_mappings=(
                instance_osmorphing_minion_pool_mappings
            ),
            destination_environment=destination_environment,
            instances=instances,
            notes=notes,
            network_map=network_map,
            storage_mappings=storage_mappings,
            source_environment=source_environment,
            user_scripts=user_scripts,
            clone_disks=clone_disks,
            skip_os_morphing=skip_os_morphing,
        )

    def get_transfers(
        self,
        ctxt,
        include_tasks_executions: bool = False,
        include_task_info: bool = False,
        marker: str | None = None,
        limit: int | None = None,
        sort_keys: list | None = None,
        sort_dirs: list | None = None,
        filters: dict | None = None,
    ) -> list[dict]:
        return self._call(
            ctxt,
            'get_transfers',
            include_tasks_executions=include_tasks_executions,
            include_task_info=include_task_info,
            marker=marker,
            limit=limit,
            sort_keys=sort_keys,
            sort_dirs=sort_dirs,
            filters=filters,
        )

    def get_transfer(
        self,
        ctxt,
        transfer_id: str,
        include_task_info: bool = False,
    ) -> dict:
        return self._call(
            ctxt,
            'get_transfer',
            transfer_id=transfer_id,
            include_task_info=include_task_info,
        )

    def delete_transfer(self, ctxt, transfer_id: str) -> None:
        self._call(ctxt, 'delete_transfer', transfer_id=transfer_id)

    def delete_transfer_disks(self, ctxt, transfer_id: str) -> dict:
        return self._call(ctxt, 'delete_transfer_disks', transfer_id=transfer_id)

    def get_deployments(
        self,
        ctxt,
        include_tasks: bool = False,
        include_task_info: bool = False,
        marker=None,
        limit=None,
        sort_keys: list | None = None,
        sort_dirs: list | None = None,
        filters: dict | None = None,
    ) -> list:
        return self._call(
            ctxt,
            'get_deployments',
            include_tasks=include_tasks,
            include_task_info=include_task_info,
            marker=marker,
            limit=limit,
            sort_keys=sort_keys,
            sort_dirs=sort_dirs,
            filters=filters,
        )

    def get_deployment(
        self,
        ctxt,
        deployment_id: str,
        include_task_info: bool = False,
    ) -> dict:
        return self._call(
            ctxt,
            'get_deployment',
            deployment_id=deployment_id,
            include_task_info=include_task_info,
        )

    def confirm_deployer_completed(
        self,
        ctxt,
        deployment_id: str,
        force: bool = False,
    ) -> None:
        return self._cast(
            ctxt, 'confirm_deployer_completed', deployment_id=deployment_id, force=force
        )

    def report_deployer_failure(
        self,
        ctxt,
        deployemnt_id: str,
        deployer_error_details: str,
    ) -> None:
        return self._cast(
            ctxt,
            'report_deployer_failure',
            deployment_id=deployemnt_id,
            deployer_error_details=deployer_error_details,
        )

    def deploy_transfer_instances(
        self,
        ctxt,
        transfer_id: str,
        force: bool,
        wait_for_execution: str | None = None,
        instance_osmorphing_minion_pool_mappings: dict | None = None,
        clone_disks: bool | None = None,
        skip_os_morphing: bool | None = None,
        user_scripts: dict | None = None,
        trust_id: str | None = None,
    ) -> dict:
        return self._call(
            ctxt,
            'deploy_transfer_instances',
            transfer_id=transfer_id,
            wait_for_execution=wait_for_execution,
            instance_osmorphing_minion_pool_mappings=(
                instance_osmorphing_minion_pool_mappings
            ),
            clone_disks=clone_disks,
            force=force,
            skip_os_morphing=skip_os_morphing,
            user_scripts=user_scripts,
            trust_id=trust_id,
        )

    def delete_deployment(self, ctxt, deployment_id: str) -> None:
        self._call(ctxt, 'delete_deployment', deployment_id=deployment_id)

    def cancel_deployment(self, ctxt, deployment_id: str, force: bool) -> None:
        self._call(ctxt, 'cancel_deployment', deployment_id=deployment_id, force=force)

    def set_task_host(self, ctxt, task_id: str, host: str) -> None:
        self._call(ctxt, 'set_task_host', task_id=task_id, host=host)

    def set_task_process(self, ctxt, task_id: str, process_id: int) -> None:
        self._call(ctxt, 'set_task_process', task_id=task_id, process_id=process_id)

    def task_completed(self, ctxt, task_id: str, task_result: dict) -> None:
        self._call(ctxt, 'task_completed', task_id=task_id, task_result=task_result)

    def confirm_task_cancellation(self, ctxt, task_id, cancellation_details):
        self._call(
            ctxt,
            'confirm_task_cancellation',
            task_id=task_id,
            cancellation_details=cancellation_details,
        )

    def set_task_error(
        self,
        ctxt,
        task_id: str,
        exception_details: str,
    ) -> None:
        self._call(
            ctxt, 'set_task_error', task_id=task_id, exception_details=exception_details
        )

    def add_task_event(self, ctxt, task_id, level, message):
        self._cast(
            ctxt, 'add_task_event', task_id=task_id, level=level, message=message
        )

    def add_task_progress_update(
        self,
        ctxt,
        task_id: str,
        message: str,
        initial_step: int = 0,
        total_steps: int = 0,
        return_event: bool = False,
    ):
        operation = self._cast
        if return_event:
            operation = self._call
        return operation(
            ctxt,
            'add_task_progress_update',
            task_id=task_id,
            message=message,
            initial_step=initial_step,
            total_steps=total_steps,
        )

    def update_task_progress_update(
        self,
        ctxt,
        task_id: str,
        progress_update_index: int,
        new_current_step: int,
        new_total_steps=None,
        new_message=None,
        sync: bool = False,
    ) -> None:
        # Use _call on the last step to make sure the DB is updated
        # and the UI is showing the correct progress.
        # Intermediate steps use cast to avoid blocking on every tick.
        if sync:
            return self._call(
                ctxt,
                'update_task_progress_update',
                task_id=task_id,
                progress_update_index=progress_update_index,
                new_current_step=new_current_step,
                new_total_steps=new_total_steps,
                new_message=new_message,
            )
        return self._cast(
            ctxt,
            'update_task_progress_update',
            task_id=task_id,
            progress_update_index=progress_update_index,
            new_current_step=new_current_step,
            new_total_steps=new_total_steps,
            new_message=new_message,
        )

    def create_transfer_schedule(
        self,
        ctxt,
        transfer_id: str,
        schedule: dict[str, int],
        enabled: bool,
        exp_date,
        shutdown_instance: bool,
        auto_deploy: bool,
    ):
        return self._call(
            ctxt,
            'create_transfer_schedule',
            transfer_id=transfer_id,
            schedule=schedule,
            enabled=enabled,
            exp_date=exp_date,
            shutdown_instance=shutdown_instance,
            auto_deploy=auto_deploy,
        )

    def update_transfer_schedule(
        self,
        ctxt,
        transfer_id: str,
        schedule_id: str,
        updated_values: dict,
    ):
        return self._call(
            ctxt,
            'update_transfer_schedule',
            transfer_id=transfer_id,
            schedule_id=schedule_id,
            updated_values=updated_values,
        )

    def delete_transfer_schedule(
        self,
        ctxt,
        transfer_id: str,
        schedule_id: str,
    ) -> None:
        return self._call(
            ctxt,
            'delete_transfer_schedule',
            transfer_id=transfer_id,
            schedule_id=schedule_id,
        )

    def get_transfer_schedules(
        self,
        ctxt,
        transfer_id: str | None = None,
        expired: bool = True,
    ):
        return self._call(
            ctxt, 'get_transfer_schedules', transfer_id=transfer_id, expired=expired
        )

    def get_transfer_schedule(
        self,
        ctxt,
        transfer_id: str,
        schedule_id: str,
        expired: bool = True,
    ):
        return self._call(
            ctxt,
            'get_transfer_schedule',
            transfer_id=transfer_id,
            schedule_id=schedule_id,
            expired=expired,
        )

    def update_transfer(
        self,
        ctxt,
        transfer_id: str,
        updated_properties: dict,
    ) -> dict:
        return self._call(
            ctxt,
            'update_transfer',
            transfer_id=transfer_id,
            updated_properties=updated_properties,
        )

    def get_diagnostics(self, ctxt):
        return self._call(ctxt, 'get_diagnostics')

    def get_all_diagnostics(self, ctxt) -> list[dict]:
        return self._call(ctxt, 'get_all_diagnostics')

    def create_region(
        self,
        ctxt,
        region_name: str,
        description: str = "",
        enabled: bool = True,
    ):
        return self._call(
            ctxt,
            'create_region',
            region_name=region_name,
            description=description,
            enabled=enabled,
        )

    def get_regions(self, ctxt):
        return self._call(ctxt, 'get_regions')

    def get_region(self, ctxt, region_id: str):
        return self._call(ctxt, 'get_region', region_id=region_id)

    def update_region(
        self,
        ctxt,
        region_id: str,
        updated_values: dict[str, str],
    ):
        return self._call(
            ctxt, 'update_region', region_id=region_id, updated_values=updated_values
        )

    def delete_region(self, ctxt, region_id: str) -> None:
        return self._call(ctxt, 'delete_region', region_id=region_id)

    def register_service(
        self,
        ctxt,
        host: str,
        binary: str,
        topic: str,
        enabled: bool,
        mapped_regions: list | None,
        providers: dict | None = None,
        specs: dict | None = None,
    ):
        return self._call(
            ctxt,
            'register_service',
            host=host,
            binary=binary,
            topic=topic,
            enabled=enabled,
            mapped_regions=mapped_regions,
            providers=providers,
            specs=specs,
        )

    def check_service_registered(
        self,
        ctxt,
        host: str,
        binary: str,
        topic: str,
    ) -> None:
        return self._call(
            ctxt, 'check_service_registered', host=host, binary=binary, topic=topic
        )

    def get_services(self, ctxt):
        return self._call(ctxt, 'get_services')

    def get_service(self, ctxt, service_id: str):
        return self._call(ctxt, 'get_service', service_id=service_id)

    def update_service(
        self,
        ctxt,
        service_id: str,
        updated_values: dict,
    ):
        return self._call(
            ctxt, 'update_service', service_id=service_id, updated_values=updated_values
        )

    def delete_service(self, ctxt, service_id: str) -> None:
        return self._call(ctxt, 'delete_service', service_id=service_id)

    def confirm_transfer_minions_allocation(
        self,
        ctxt,
        transfer_id: str,
        minion_machine_allocations: dict[str, dict],
    ) -> None:
        self._call(
            ctxt,
            'confirm_transfer_minions_allocation',
            transfer_id=transfer_id,
            minion_machine_allocations=minion_machine_allocations,
        )

    def report_transfer_minions_allocation_error(
        self,
        ctxt,
        transfer_id: str,
        minion_allocation_error_details: str,
    ) -> None:
        self._call(
            ctxt,
            'report_transfer_minions_allocation_error',
            transfer_id=transfer_id,
            minion_allocation_error_details=minion_allocation_error_details,
        )

    def confirm_deployment_minions_allocation(
        self,
        ctxt,
        deployment_id: str,
        minion_machine_allocations: dict[str, dict],
    ) -> None:
        self._call(
            ctxt,
            'confirm_deployment_minions_allocation',
            deployment_id=deployment_id,
            minion_machine_allocations=minion_machine_allocations,
        )

    def report_deployment_minions_allocation_error(
        self,
        ctxt,
        deployment_id: str,
        minion_allocation_error_details: str,
    ) -> None:
        self._call(
            ctxt,
            'report_deployment_minions_allocation_error',
            deployment_id=deployment_id,
            minion_allocation_error_details=minion_allocation_error_details,
        )


class ConductorTaskRpcEventHandler(events.BaseEventHandler):
    def __init__(self, ctxt, task_id: str):
        self._ctxt = ctxt
        self._task_id = task_id
        self._rpc_conductor_client_instance = None

    @property
    def _rpc_conductor_client(self) -> ConductorClient:
        # NOTE(aznashwan): it is unsafe to fork processes with pre-instantiated
        # oslo_messaging clients as the underlying thread queues will
        # be invalidated.
        if self._rpc_conductor_client_instance is None:
            self._rpc_conductor_client_instance = ConductorClient()
        return self._rpc_conductor_client_instance

    @classmethod
    def get_progress_update_identifier(cls, progress_update) -> int:
        return progress_update['index']

    def add_progress_update(
        self,
        message: str,
        initial_step: int = 0,
        total_steps: int = 0,
        return_event: bool = False,
    ):
        LOG.info(
            "Sending progress update for task '%s' to conductor: %s",
            self._task_id,
            message,
        )
        return self._rpc_conductor_client.add_task_progress_update(
            self._ctxt,
            self._task_id,
            message,
            initial_step=initial_step,
            total_steps=total_steps,
            return_event=return_event,
        )

    def update_progress_update(
        self,
        update_identifier: int,
        new_current_step: int,
        new_total_steps=None,
        new_message=None,
        sync: bool = False,
    ) -> None:
        LOG.info(
            "Updating progress update '%s' for task '%s' with new step %s",
            update_identifier,
            self._task_id,
            new_current_step,
        )
        self._rpc_conductor_client.update_task_progress_update(
            self._ctxt,
            self._task_id,
            update_identifier,
            new_current_step,
            new_total_steps=new_total_steps,
            new_message=new_message,
            sync=sync,
        )

    def add_event(self, message, level=constants.TASK_EVENT_INFO):
        self._rpc_conductor_client.add_task_event(
            self._ctxt, self._task_id, level, message
        )
