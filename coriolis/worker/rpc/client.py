import oslo_messaging as messaging

from coriolis import rpc

VERSION = "1.0"


class WorkerClient(object):
    def __init__(self):
        target = messaging.Target(topic='coriolis_worker', version=VERSION)
        self._client = rpc.get_client(target)

    def begin_export_instance(self, ctxt, task_id, origin, instance):
        self._client.cast(
            ctxt, 'export_instance', task_id=task_id, origin=origin,
            instance=instance)

    def stop_task(self, ctxt, server, process_id):
        # Needs to be executed on the same server
        cctxt = self._client.prepare(server=server)
        cctxt.call(ctxt, 'stop_task', process_id=process_id)

    def begin_import_instance(self, ctxt, server, task_id, destination,
                              instance, export_info):
        # Needs to be executed on the same server
        cctxt = self._client.prepare(server=server)
        cctxt.cast(
            ctxt, 'import_instance', task_id=task_id,
            destination=destination, instance=instance,
            export_info=export_info)

    def update_migration_status(self, ctxt, task_id, status):
        self._client.call(ctxt, "update_migration_status", status=status)
