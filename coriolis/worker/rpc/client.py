import oslo_messaging as messaging

from coriolis import rpc

VERSION = "1.0"


class WorkerClient(object):
    def __init__(self):
        target = messaging.Target(topic='coriolis_worker', version=VERSION)
        self._client = rpc.get_client(target)

    def begin_task(self, ctxt, server, task_id, task_type, origin, destination,
                   instance, task_info):
        cctxt = self._client.prepare(server=server)
        cctxt.cast(
            ctxt, 'exec_task', task_id=task_id, task_type=task_type,
            origin=origin, destination=destination, instance=instance,
            task_info=task_info)

    def cancel_task(self, ctxt, server, process_id):
        # Needs to be executed on the same server
        cctxt = self._client.prepare(server=server,
                                     topic="coriolis_worker_delete_task")
        cctxt.call(ctxt, 'cancel_task', process_id=process_id)

    def update_migration_status(self, ctxt, task_id, status):
        self._client.call(ctxt, "update_migration_status", status=status)
