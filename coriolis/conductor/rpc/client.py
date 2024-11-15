# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging

from coriolis import constants
from coriolis import events
from coriolis import rpc

VERSION = "1.0"
LOG = logging.getLogger(__name__)

conductor_opts = [
    cfg.IntOpt("conductor_rpc_timeout",
               help="Number of seconds until RPC calls to the "
                    "conductor timeout.")
]

CONF = cfg.CONF
CONF.register_opts(conductor_opts, 'conductor')


class ConductorClient(rpc.BaseRPCClient):
    def __init__(self, timeout=None,
                 topic=constants.CONDUCTOR_MAIN_MESSAGING_TOPIC):
        target = messaging.Target(topic=topic, version=VERSION)
        if timeout is None:
            timeout = CONF.conductor.conductor_rpc_timeout
        super(ConductorClient, self).__init__(
            target, timeout=timeout)

    def create_endpoint(self, ctxt, name, endpoint_type, description,
                        connection_info, mapped_regions):
        return self._call(
            ctxt, 'create_endpoint', name=name, endpoint_type=endpoint_type,
            description=description, connection_info=connection_info,
            mapped_regions=mapped_regions)

    def update_endpoint(self, ctxt, endpoint_id, updated_values):
        return self._call(
            ctxt, 'update_endpoint',
            endpoint_id=endpoint_id,
            updated_values=updated_values)

    def get_endpoints(self, ctxt):
        return self._call(
            ctxt, 'get_endpoints')

    def get_endpoint(self, ctxt, endpoint_id):
        return self._call(
            ctxt, 'get_endpoint', endpoint_id=endpoint_id)

    def delete_endpoint(self, ctxt, endpoint_id):
        return self._call(
            ctxt, 'delete_endpoint', endpoint_id=endpoint_id)

    def get_endpoint_instances(self, ctxt, endpoint_id, source_environment,
                               marker=None, limit=None,
                               instance_name_pattern=None):
        return self._call(
            ctxt, 'get_endpoint_instances',
            endpoint_id=endpoint_id,
            source_environment=source_environment,
            marker=marker,
            limit=limit,
            instance_name_pattern=instance_name_pattern)

    def get_endpoint_instance(
            self, ctxt, endpoint_id, source_environment, instance_name):
        return self._call(
            ctxt, 'get_endpoint_instance',
            endpoint_id=endpoint_id,
            source_environment=source_environment,
            instance_name=instance_name)

    def get_endpoint_source_options(
            self, ctxt, endpoint_id, env, option_names):
        return self._call(
            ctxt, 'get_endpoint_source_options',
            endpoint_id=endpoint_id,
            env=env, option_names=option_names)

    def get_endpoint_destination_options(
            self, ctxt, endpoint_id, env, option_names):
        return self._call(
            ctxt, 'get_endpoint_destination_options',
            endpoint_id=endpoint_id,
            env=env, option_names=option_names)

    def get_endpoint_networks(self, ctxt, endpoint_id, env):
        return self._call(
            ctxt, 'get_endpoint_networks',
            endpoint_id=endpoint_id,
            env=env)

    def get_endpoint_storage(self, ctxt, endpoint_id, env):
        return self._call(
            ctxt, 'get_endpoint_storage',
            endpoint_id=endpoint_id,
            env=env)

    def validate_endpoint_connection(self, ctxt, endpoint_id):
        return self._call(
            ctxt, 'validate_endpoint_connection',
            endpoint_id=endpoint_id)

    def validate_endpoint_target_environment(
            self, ctxt, endpoint_id, target_env):
        return self._call(
            ctxt, 'validate_endpoint_target_environment',
            endpoint_id=endpoint_id, target_env=target_env)

    def validate_endpoint_source_environment(
            self, ctxt, endpoint_id, source_env):
        return self._call(
            ctxt, 'validate_endpoint_source_environment',
            endpoint_id=endpoint_id, source_env=source_env)

    def get_available_providers(self, ctxt):
        return self._call(
            ctxt, 'get_available_providers')

    def get_provider_schemas(self, ctxt, platform_name, provider_type):
        return self._call(
            ctxt, 'get_provider_schemas',
            platform_name=platform_name,
            provider_type=provider_type)

    def execute_replica_tasks(self, ctxt, replica_id,
                              shutdown_instances=False):
        return self._call(
            ctxt, 'execute_replica_tasks', replica_id=replica_id,
            shutdown_instances=shutdown_instances)

    def get_replica_tasks_executions(self, ctxt, replica_id,
                                     include_tasks=False):
        return self._call(
            ctxt, 'get_replica_tasks_executions',
            replica_id=replica_id,
            include_tasks=include_tasks)

    def get_replica_tasks_execution(self, ctxt, replica_id, execution_id,
                                    include_task_info=False):
        return self._call(
            ctxt, 'get_replica_tasks_execution', replica_id=replica_id,
            execution_id=execution_id, include_task_info=include_task_info)

    def delete_replica_tasks_execution(self, ctxt, replica_id, execution_id):
        return self._call(
            ctxt, 'delete_replica_tasks_execution', replica_id=replica_id,
            execution_id=execution_id)

    def cancel_replica_tasks_execution(self, ctxt, replica_id, execution_id,
                                       force):
        return self._call(
            ctxt, 'cancel_replica_tasks_execution', replica_id=replica_id,
            execution_id=execution_id, force=force)

    def create_instances_replica(self, ctxt,
                                 replica_scenario,
                                 origin_endpoint_id,
                                 destination_endpoint_id,
                                 origin_minion_pool_id,
                                 destination_minion_pool_id,
                                 instance_osmorphing_minion_pool_mappings,
                                 source_environment, destination_environment,
                                 instances, network_map, storage_mappings,
                                 notes=None, user_scripts=None):
        return self._call(
            ctxt, 'create_instances_replica',
            replica_scenario=replica_scenario,
            origin_endpoint_id=origin_endpoint_id,
            destination_endpoint_id=destination_endpoint_id,
            origin_minion_pool_id=origin_minion_pool_id,
            destination_minion_pool_id=destination_minion_pool_id,
            instance_osmorphing_minion_pool_mappings=(
                instance_osmorphing_minion_pool_mappings),
            destination_environment=destination_environment,
            instances=instances,
            notes=notes,
            network_map=network_map,
            storage_mappings=storage_mappings,
            source_environment=source_environment,
            user_scripts=user_scripts)

    def get_replicas(self, ctxt, include_tasks_executions=False,
                     include_task_info=False):
        return self._call(
            ctxt, 'get_replicas',
            include_tasks_executions=include_tasks_executions,
            include_task_info=include_task_info)

    def get_replica(self, ctxt, replica_id, include_task_info=False):
        return self._call(
            ctxt, 'get_replica', replica_id=replica_id,
            include_task_info=include_task_info)

    def delete_replica(self, ctxt, replica_id):
        self._call(
            ctxt, 'delete_replica', replica_id=replica_id)

    def delete_replica_disks(self, ctxt, replica_id):
        return self._call(
            ctxt, 'delete_replica_disks', replica_id=replica_id)

    def get_deployments(self, ctxt, include_tasks=False,
                        include_task_info=False):
        return self._call(
            ctxt, 'get_deployments', include_tasks=include_tasks,
            include_task_info=include_task_info)

    def get_deployment(self, ctxt, deployment_id, include_task_info=False):
        return self._call(
            ctxt, 'get_deployment', deployment_id=deployment_id,
            include_task_info=include_task_info)

    def deploy_replica_instances(
            self, ctxt, replica_id,
            instance_osmorphing_minion_pool_mappings=None, clone_disks=False,
            force=False, skip_os_morphing=False, user_scripts=None):
        return self._call(
            ctxt, 'deploy_replica_instances', replica_id=replica_id,
            instance_osmorphing_minion_pool_mappings=(
                instance_osmorphing_minion_pool_mappings),
            clone_disks=clone_disks, force=force,
            skip_os_morphing=skip_os_morphing,
            user_scripts=user_scripts)

    def delete_deployment(self, ctxt, deployment_id):
        self._call(
            ctxt, 'delete_deployment', deployment_id=deployment_id)

    def cancel_deployment(self, ctxt, deployment_id, force):
        self._call(
            ctxt, 'cancel_deployment', deployment_id=deployment_id,
            force=force)

    def set_task_host(self, ctxt, task_id, host):
        self._call(
            ctxt, 'set_task_host', task_id=task_id, host=host)

    def set_task_process(self, ctxt, task_id, process_id):
        self._call(
            ctxt, 'set_task_process', task_id=task_id, process_id=process_id)

    def task_completed(self, ctxt, task_id, task_result):
        self._call(
            ctxt, 'task_completed', task_id=task_id, task_result=task_result)

    def confirm_task_cancellation(self, ctxt, task_id, cancellation_details):
        self._call(
            ctxt, 'confirm_task_cancellation', task_id=task_id,
            cancellation_details=cancellation_details)

    def set_task_error(self, ctxt, task_id, exception_details):
        self._call(
            ctxt, 'set_task_error', task_id=task_id,
            exception_details=exception_details)

    def add_task_event(self, ctxt, task_id, level, message):
        self._cast(ctxt, 'add_task_event', task_id=task_id,
                   level=level, message=message)

    def add_task_progress_update(
            self, ctxt, task_id, message, initial_step=0, total_steps=0,
            return_event=False):
        operation = self._cast
        if return_event:
            operation = self._call
        return operation(
            ctxt, 'add_task_progress_update', task_id=task_id,
            message=message, initial_step=initial_step,
            total_steps=total_steps)

    def update_task_progress_update(
            self, ctxt, task_id, progress_update_index, new_current_step,
            new_total_steps=None, new_message=None):
        self._cast(
            ctxt, 'update_task_progress_update', task_id=task_id,
            progress_update_index=progress_update_index,
            new_current_step=new_current_step,
            new_total_steps=new_total_steps, new_message=new_message)

    def create_replica_schedule(self, ctxt, replica_id,
                                schedule, enabled, exp_date,
                                shutdown_instance):
        return self._call(
            ctxt, 'create_replica_schedule',
            replica_id=replica_id,
            schedule=schedule,
            enabled=enabled,
            exp_date=exp_date,
            shutdown_instance=shutdown_instance)

    def update_replica_schedule(self, ctxt, replica_id, schedule_id,
                                updated_values):
        return self._call(
            ctxt, 'update_replica_schedule',
            replica_id=replica_id,
            schedule_id=schedule_id,
            updated_values=updated_values)

    def delete_replica_schedule(self, ctxt, replica_id, schedule_id):
        return self._call(
            ctxt, 'delete_replica_schedule',
            replica_id=replica_id,
            schedule_id=schedule_id)

    def get_replica_schedules(self, ctxt, replica_id=None, expired=True):
        return self._call(
            ctxt, 'get_replica_schedules',
            replica_id=replica_id, expired=expired)

    def get_replica_schedule(self, ctxt, replica_id,
                             schedule_id, expired=True):
        return self._call(
            ctxt, 'get_replica_schedule',
            replica_id=replica_id,
            schedule_id=schedule_id,
            expired=expired)

    def update_replica(self, ctxt, replica_id, updated_properties):
        return self._call(
            ctxt, 'update_replica',
            replica_id=replica_id,
            updated_properties=updated_properties)

    def get_diagnostics(self, ctxt):
        return self._call(ctxt, 'get_diagnostics')

    def get_all_diagnostics(self, ctxt):
        return self._call(ctxt, 'get_all_diagnostics')

    def create_region(
            self, ctxt, region_name, description="", enabled=True):
        return self._call(
            ctxt, 'create_region',
            region_name=region_name,
            description=description,
            enabled=enabled)

    def get_regions(self, ctxt):
        return self._call(ctxt, 'get_regions')

    def get_region(self, ctxt, region_id):
        return self._call(
            ctxt, 'get_region', region_id=region_id)

    def update_region(self, ctxt, region_id, updated_values):
        return self._call(
            ctxt, 'update_region',
            region_id=region_id,
            updated_values=updated_values)

    def delete_region(self, ctxt, region_id):
        return self._call(
            ctxt, 'delete_region', region_id=region_id)

    def register_service(
            self, ctxt, host, binary, topic, enabled, mapped_regions,
            providers=None, specs=None):
        return self._call(
            ctxt, 'register_service', host=host, binary=binary,
            topic=topic, enabled=enabled, mapped_regions=mapped_regions,
            providers=providers, specs=specs)

    def check_service_registered(self, ctxt, host, binary, topic):
        return self._call(
            ctxt, 'check_service_registered', host=host, binary=binary,
            topic=topic)

    def refresh_service_status(self, ctxt, service_id):
        return self._call(
            ctxt, 'refresh_service_status', service_id=service_id)

    def get_services(self, ctxt):
        return self._call(ctxt, 'get_services')

    def get_service(self, ctxt, service_id):
        return self._call(
            ctxt, 'get_service', service_id=service_id)

    def update_service(self, ctxt, service_id, updated_values):
        return self._call(
            ctxt, 'update_service', service_id=service_id,
            updated_values=updated_values)

    def delete_service(self, ctxt, service_id):
        return self._call(
            ctxt, 'delete_service', service_id=service_id)

    def confirm_replica_minions_allocation(
            self, ctxt, replica_id, minion_machine_allocations):
        self._call(
            ctxt, 'confirm_replica_minions_allocation', replica_id=replica_id,
            minion_machine_allocations=minion_machine_allocations)

    def report_replica_minions_allocation_error(
            self, ctxt, replica_id, minion_allocation_error_details):
        self._call(
            ctxt, 'report_replica_minions_allocation_error',
            replica_id=replica_id,
            minion_allocation_error_details=minion_allocation_error_details)

    def confirm_migration_minions_allocation(
            self, ctxt, migration_id, minion_machine_allocations):
        self._call(
            ctxt, 'confirm_migration_minions_allocation',
            migration_id=migration_id,
            minion_machine_allocations=minion_machine_allocations)

    def report_migration_minions_allocation_error(
            self, ctxt, migration_id, minion_allocation_error_details):
        self._call(
            ctxt, 'report_migration_minions_allocation_error',
            migration_id=migration_id,
            minion_allocation_error_details=minion_allocation_error_details)


class ConductorTaskRpcEventHandler(events.BaseEventHandler):
    def __init__(self, ctxt, task_id):
        self._ctxt = ctxt
        self._task_id = task_id
        self._rpc_conductor_client_instance = None

    @property
    def _rpc_conductor_client(self):
        # NOTE(aznashwan): it is unsafe to fork processes with pre-instantiated
        # oslo_messaging clients as the underlying eventlet thread queues will
        # be invalidated.
        if self._rpc_conductor_client_instance is None:
            self._rpc_conductor_client_instance = ConductorClient()
        return self._rpc_conductor_client_instance

    @classmethod
    def get_progress_update_identifier(cls, progress_update):
        return progress_update['index']

    def add_progress_update(
            self, message, initial_step=0, total_steps=0, return_event=False):
        LOG.info(
            "Sending progress update for task '%s' to conductor: %s",
            self._task_id, message)
        return self._rpc_conductor_client.add_task_progress_update(
            self._ctxt, self._task_id, message, initial_step=initial_step,
            total_steps=total_steps, return_event=return_event)

    def update_progress_update(
            self, update_identifier, new_current_step,
            new_total_steps=None, new_message=None):
        LOG.info(
            "Updating progress update '%s' for task '%s' with new step %s",
            update_identifier, self._task_id, new_current_step)
        self._rpc_conductor_client.update_task_progress_update(
            self._ctxt, self._task_id, update_identifier, new_current_step,
            new_total_steps=new_total_steps, new_message=new_message)

    def add_event(self, message, level=constants.TASK_EVENT_INFO):
        self._rpc_conductor_client.add_task_event(
            self._ctxt, self._task_id, level, message)
