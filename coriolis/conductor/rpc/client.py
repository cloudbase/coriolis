# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_config import cfg
import oslo_messaging as messaging

from coriolis import rpc

VERSION = "1.0"

conductor_opts = [
    cfg.IntOpt("conductor_rpc_timeout",
               help="Number of seconds until RPC calls to the "
                    "conductor timeout.")
]

CONF = cfg.CONF
CONF.register_opts(conductor_opts, 'conductor')


class ConductorClient(object):
    def __init__(self, timeout=None):
        target = messaging.Target(topic='coriolis_conductor', version=VERSION)
        if timeout is None:
            timeout = CONF.conductor.conductor_rpc_timeout
        self._client = rpc.get_client(target, timeout=timeout)

    def create_endpoint(self, ctxt, name, endpoint_type, description,
                        connection_info):
        return self._client.call(
            ctxt, 'create_endpoint', name=name, endpoint_type=endpoint_type,
            description=description, connection_info=connection_info)

    def update_endpoint(self, ctxt, endpoint_id, updated_values):
        return self._client.call(
            ctxt, 'update_endpoint',
            endpoint_id=endpoint_id,
            updated_values=updated_values)

    def get_endpoints(self, ctxt):
        return self._client.call(
            ctxt, 'get_endpoints')

    def get_endpoint(self, ctxt, endpoint_id):
        return self._client.call(
            ctxt, 'get_endpoint', endpoint_id=endpoint_id)

    def delete_endpoint(self, ctxt, endpoint_id):
        return self._client.call(
            ctxt, 'delete_endpoint', endpoint_id=endpoint_id)

    def get_endpoint_instances(self, ctxt, endpoint_id, source_environment,
                               marker=None, limit=None,
                               instance_name_pattern=None):
        return self._client.call(
            ctxt, 'get_endpoint_instances',
            endpoint_id=endpoint_id,
            source_environment=source_environment,
            marker=marker,
            limit=limit,
            instance_name_pattern=instance_name_pattern)

    def get_endpoint_instance(
            self, ctxt, endpoint_id, source_environment, instance_name):
        return self._client.call(
            ctxt, 'get_endpoint_instance',
            endpoint_id=endpoint_id,
            source_environment=source_environment,
            instance_name=instance_name)

    def get_endpoint_source_options(
            self, ctxt, endpoint_id, env, option_names):
        return self._client.call(
            ctxt, 'get_endpoint_source_options',
            endpoint_id=endpoint_id,
            env=env, option_names=option_names)

    def get_endpoint_destination_options(
            self, ctxt, endpoint_id, env, option_names):
        return self._client.call(
            ctxt, 'get_endpoint_destination_options',
            endpoint_id=endpoint_id,
            env=env, option_names=option_names)

    def get_endpoint_networks(self, ctxt, endpoint_id, env):
        return self._client.call(
            ctxt, 'get_endpoint_networks',
            endpoint_id=endpoint_id,
            env=env)

    def get_endpoint_storage(self, ctxt, endpoint_id, env):
        return self._client.call(
            ctxt, 'get_endpoint_storage',
            endpoint_id=endpoint_id,
            env=env)

    def validate_endpoint_connection(self, ctxt, endpoint_id):
        return self._client.call(
            ctxt, 'validate_endpoint_connection',
            endpoint_id=endpoint_id)

    def validate_endpoint_target_environment(
            self, ctxt, endpoint_id, target_env):
        return self._client.call(
            ctxt, 'validate_endpoint_target_environment',
            endpoint_id=endpoint_id, target_env=target_env)

    def validate_endpoint_source_environment(
            self, ctxt, endpoint_id, source_env):
        return self._client.call(
            ctxt, 'validate_endpoint_source_environment',
            endpoint_id=endpoint_id, source_env=source_env)

    def get_available_providers(self, ctxt):
        return self._client.call(
            ctxt, 'get_available_providers')

    def get_provider_schemas(self, ctxt, platform_name, provider_type):
        return self._client.call(
            ctxt, 'get_provider_schemas',
            platform_name=platform_name,
            provider_type=provider_type)

    def execute_replica_tasks(self, ctxt, replica_id,
                              shutdown_instances=False, force=False):
        return self._client.call(
            ctxt, 'execute_replica_tasks', replica_id=replica_id,
            shutdown_instances=shutdown_instances, force=force)

    def get_replica_tasks_executions(self, ctxt, replica_id,
                                     include_tasks=False):
        return self._client.call(
            ctxt, 'get_replica_tasks_executions',
            replica_id=replica_id,
            include_tasks=include_tasks)

    def get_replica_tasks_execution(self, ctxt, replica_id, execution_id):
        return self._client.call(
            ctxt, 'get_replica_tasks_execution', replica_id=replica_id,
            execution_id=execution_id)

    def delete_replica_tasks_execution(self, ctxt, replica_id, execution_id):
        return self._client.call(
            ctxt, 'delete_replica_tasks_execution', replica_id=replica_id,
            execution_id=execution_id)

    def cancel_replica_tasks_execution(self, ctxt, replica_id, execution_id,
                                       force):
        return self._client.call(
            ctxt, 'cancel_replica_tasks_execution', replica_id=replica_id,
            execution_id=execution_id, force=force)

    def create_instances_replica(self, ctxt, origin_endpoint_id,
                                 destination_endpoint_id,
                                 source_environment, destination_environment,
                                 instances, network_map, storage_mappings,
                                 notes=None):
        return self._client.call(
            ctxt, 'create_instances_replica',
            origin_endpoint_id=origin_endpoint_id,
            destination_endpoint_id=destination_endpoint_id,
            destination_environment=destination_environment,
            instances=instances,
            notes=notes,
            network_map=network_map,
            storage_mappings=storage_mappings,
            source_environment=source_environment)

    def get_replicas(self, ctxt, include_tasks_executions=False):
        return self._client.call(
            ctxt, 'get_replicas',
            include_tasks_executions=include_tasks_executions)

    def get_replica(self, ctxt, replica_id):
        return self._client.call(
            ctxt, 'get_replica', replica_id=replica_id)

    def delete_replica(self, ctxt, replica_id):
        self._client.call(
            ctxt, 'delete_replica', replica_id=replica_id)

    def delete_replica_disks(self, ctxt, replica_id, force):
        return self._client.call(
            ctxt, 'delete_replica_disks', replica_id=replica_id, force=force)

    def get_migrations(self, ctxt, include_tasks=False):
        return self._client.call(ctxt, 'get_migrations',
                                 include_tasks=include_tasks)

    def get_migration(self, ctxt, migration_id):
        return self._client.call(
            ctxt, 'get_migration', migration_id=migration_id)

    def migrate_instances(self, ctxt, origin_endpoint_id,
                          destination_endpoint_id, source_environment,
                          destination_environment, instances, network_map,
                          storage_mappings, notes=None,
                          skip_os_morphing=False):
        return self._client.call(
            ctxt, 'migrate_instances',
            origin_endpoint_id=origin_endpoint_id,
            destination_endpoint_id=destination_endpoint_id,
            destination_environment=destination_environment,
            instances=instances,
            notes=notes,
            skip_os_morphing=skip_os_morphing,
            network_map=network_map,
            storage_mappings=storage_mappings,
            source_environment=source_environment)

    def deploy_replica_instances(self, ctxt, replica_id, clone_disks=False,
                                 force=False, skip_os_morphing=False):
        return self._client.call(
            ctxt, 'deploy_replica_instances', replica_id=replica_id,
            clone_disks=clone_disks, force=force,
            skip_os_morphing=skip_os_morphing)

    def delete_migration(self, ctxt, migration_id):
        self._client.call(
            ctxt, 'delete_migration', migration_id=migration_id)

    def cancel_migration(self, ctxt, migration_id, force):
        self._client.call(
            ctxt, 'cancel_migration', migration_id=migration_id, force=force)

    def set_task_host(self, ctxt, task_id, host, process_id):
        self._client.call(
            ctxt, 'set_task_host', task_id=task_id, host=host,
            process_id=process_id)

    def task_completed(self, ctxt, task_id, task_info):
        self._client.call(
            ctxt, 'task_completed', task_id=task_id, task_info=task_info)

    def set_task_error(self, ctxt, task_id, exception_details):
        self._client.call(ctxt, 'set_task_error', task_id=task_id,
                          exception_details=exception_details)

    def task_event(self, ctxt, task_id, level, message):
        self._client.cast(ctxt, 'task_event', task_id=task_id, level=level,
                          message=message)

    def task_progress_update(self, ctxt, task_id, current_step, total_steps,
                             message):
        self._client.cast(ctxt, 'task_progress_update', task_id=task_id,
                          current_step=current_step, total_steps=total_steps,
                          message=message)

    def create_replica_schedule(self, ctxt, replica_id,
                                schedule, enabled, exp_date,
                                shutdown_instance):
        return self._client.call(
            ctxt, 'create_replica_schedule',
            replica_id=replica_id,
            schedule=schedule,
            enabled=enabled,
            exp_date=exp_date,
            shutdown_instance=shutdown_instance)

    def update_replica_schedule(self, ctxt, replica_id, schedule_id,
                                updated_values):
        return self._client.call(
            ctxt, 'update_replica_schedule',
            replica_id=replica_id,
            schedule_id=schedule_id,
            updated_values=updated_values)

    def delete_replica_schedule(self, ctxt, replica_id, schedule_id):
        return self._client.call(
            ctxt, 'delete_replica_schedule',
            replica_id=replica_id,
            schedule_id=schedule_id)

    def get_replica_schedules(self, ctxt, replica_id=None, expired=True):
        return self._client.call(
            ctxt, 'get_replica_schedules',
            replica_id=replica_id, expired=expired)

    def get_replica_schedule(self, ctxt, replica_id,
                             schedule_id, expired=True):
        return self._client.call(
            ctxt, 'get_replica_schedule',
            replica_id=replica_id,
            schedule_id=schedule_id,
            expired=expired)

    def update_replica(self, ctxt, replica_id, properties, force):
        return self._client.call(
            ctxt, 'update_replica',
            replica_id=replica_id,
            properties=properties,
            force=force)
