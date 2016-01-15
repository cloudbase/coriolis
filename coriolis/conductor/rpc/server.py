import uuid

import json

import oslo_messaging as messaging

from coriolis import constants
from coriolis.db import api as db_api
from coriolis.db.sqlalchemy import models
from coriolis.worker.rpc import client as rpc_worker_client

VERSION = "1.0"


class ConductorServerEndpoint(object):
    def __init__(self):
        self._rpc_worker_client = rpc_worker_client.WorkerClient()

    def get_migrations(self, ctxt):
        # TODO: fix context
        from coriolis import context
        ctxt = context.CoriolisContext()

        return db_api.get_migrations(ctxt)

    def get_migration(self, ctxt, migration_id):
        # TODO: fix context
        from coriolis import context
        ctxt = context.CoriolisContext()

        return db_api.get_migration(ctxt, migration_id)

    def migrate_instances(self, ctxt, origin, destination, instances):
        # TODO: fix context
        from coriolis import context
        ctxt = context.CoriolisContext()

        migration = models.Migration()
        migration.user_id = "todo"
        migration.status = constants.MIGRATION_STATUS_STARTED
        migration.origin = json.dumps(origin)
        migration.destination = json.dumps(destination)

        for instance in instances:
            op = models.Operation()
            op.id = str(uuid.uuid4())
            op.migration = migration
            op.instance = instance
            op.status = constants.OPERATION_STATUS_STARTED
            op.operation_type = constants.OPERATION_TYPE_EXPORT

        db_api.add(ctxt, migration)

        for op in migration.operations:
            self._rpc_worker_client.begin_export_instance(
                ctxt.to_dict(), op.id, origin, instance)

    def set_operation_host(self, ctxt, operation_id, host):
        # TODO: fix context
        from coriolis import context
        ctxt = context.CoriolisContext()
        db_api.set_operation_host(ctxt, operation_id, host)

    def export_completed(self, ctxt, operation_id, export_info):
        # TODO: fix context
        from coriolis import context
        ctxt = context.CoriolisContext()

        db_api.update_operation_status(
            ctxt, operation_id, constants.OPERATION_STATUS_COMPLETE)
        op_export = db_api.get_operation(ctxt, operation_id)

        op_import = models.Operation()
        op_import.id = str(uuid.uuid4())
        op_import.migration = op_export.migration
        op_import.instance = op_export.instance
        op_import.status = constants.OPERATION_STATUS_STARTED
        op_import.operation_type = constants.OPERATION_TYPE_IMPORT

        db_api.add(ctxt, op_import)

        self._rpc_worker_client.begin_import_instance(
            ctxt.to_dict(), op_export.host, op_import.id,
            json.loads(op_import.migration.destination),
            op_import.instance,
            export_info)

    def import_completed(self, ctxt, operation_id):
        # TODO: fix context
        from coriolis import context
        ctxt = context.CoriolisContext()

        db_api.update_operation_status(
            ctxt, operation_id, constants.OPERATION_STATUS_COMPLETE)
