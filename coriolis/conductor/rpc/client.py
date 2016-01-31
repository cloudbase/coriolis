import oslo_messaging as messaging

from coriolis import rpc

VERSION = "1.0"


class ConductorClient(object):
    def __init__(self):
        target = messaging.Target(topic='coriolis_conductor', version=VERSION)
        self._client = rpc.get_client(target)

    def get_migrations(self, ctxt):
        return self._client.call(ctxt, 'get_migrations')

    def get_migration(self, ctxt, migration_id):
        return self._client.call(
            ctxt, 'get_migration', migration_id=migration_id)

    def begin_migrate_instances(self, ctxt, origin, destination, instances):
        return self._client.call(
            ctxt, 'migrate_instances', origin=origin, destination=destination,
            instances=instances)

    def stop_instances_migration(self, ctxt, migration_id):
        self._client.call(
            ctxt, 'stop_instances_migration', migration_id=migration_id)

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

    def task_progress_update(self, ctxt, task_id, current_step, total_steps,
                             message):
        self._client.cast(ctxt, 'task_progress_update', task_id=task_id,
                          current_step=current_step, total_steps=total_steps,
                          message=message)
