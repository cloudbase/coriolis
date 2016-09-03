# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import oslo_messaging as messaging

from coriolis import rpc

VERSION = "1.0"


class ConductorClient(object):
    def __init__(self):
        target = messaging.Target(topic='coriolis_conductor', version=VERSION)
        self._client = rpc.get_client(target)

    def execute_replica_tasks(self, ctxt, replica_id,
                              shutdown_instances=False):
        return self._client.call(
            ctxt, 'execute_replica_tasks', replica_id=replica_id,
            shutdown_instances=shutdown_instances)

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

    def create_instances_replica(self, ctxt, origin, destination, instances):
        return self._client.call(
            ctxt, 'create_instances_replica', origin=origin,
            destination=destination, instances=instances)

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

    def delete_replica_disks(self, ctxt, replica_id):
        return self._client.call(
            ctxt, 'delete_replica_disks', replica_id=replica_id)

    def get_migrations(self, ctxt, include_tasks=False):
        return self._client.call(ctxt, 'get_migrations',
                                 include_tasks=include_tasks)

    def get_migration(self, ctxt, migration_id):
        return self._client.call(
            ctxt, 'get_migration', migration_id=migration_id)

    def migrate_instances(self, ctxt, origin, destination, instances):
        return self._client.call(
            ctxt, 'migrate_instances', origin=origin, destination=destination,
            instances=instances)

    def deploy_replica_instances(self, ctxt, replica_id, clone_disks=False,
                                 force=False):
        return self._client.call(
            ctxt, 'deploy_replica_instances', replica_id=replica_id,
            clone_disks=clone_disks, force=force)

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
