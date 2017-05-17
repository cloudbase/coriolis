# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from webob import exc

from oslo_log import log as logging

from coriolis.api import wsgi as api_wsgi
from coriolis.api.v1.views import migration_view
from coriolis import exception
from coriolis.migrations import api

LOG = logging.getLogger(__name__)


class MigrationController(api_wsgi.Controller):
    def __init__(self):
        self._migration_api = api.API()
        super(MigrationController, self).__init__()

    def show(self, req, id):
        migration = self._migration_api.get_migration(
            req.environ["coriolis.context"], id)
        if not migration:
            raise exc.HTTPNotFound()

        return migration_view.single(req, migration)

    def index(self, req):
        return migration_view.collection(
            req, self._migration_api.get_migrations(
                req.environ['coriolis.context'], include_tasks=False))

    def detail(self, req):
        return migration_view.collection(
            req, self._migration_api.get_migrations(
                req.environ['coriolis.context'], include_tasks=True))

    def _validate_migration_input(self, migration):
        try:
            origin_endpoint_id = migration["origin_endpoint_id"]
            destination_endpoint_id = migration["destination_endpoint_id"]
            destination_environment = migration.get("destination_environment")
            instances = migration["instances"]
            notes = migration.get("notes")

            return (origin_endpoint_id, destination_endpoint_id,
                    destination_environment, instances, notes)
        except Exception as ex:
            LOG.exception(ex)
            if hasattr(ex, "message"):
                msg = ex.message
            else:
                msg = str(ex)
            raise exception.InvalidInput(msg)

    def create(self, req, body):
        # TODO: validate body
        migration_body = body.get("migration", {})
        context = req.environ['coriolis.context']

        replica_id = migration_body.get("replica_id")
        if replica_id:
            clone_disks = migration_body.get("clone_disks", True)
            force = migration_body.get("force", False)

            migration = self._migration_api.deploy_replica_instances(
                context, replica_id, clone_disks, force)
        else:
            (origin_endpoint_id,
             destination_endpoint_id,
             destination_environment,
             instances,
             notes) = self._validate_migration_input(
                migration_body)
            migration = self._migration_api.migrate_instances(
                context, origin_endpoint_id, destination_endpoint_id,
                destination_environment, instances, notes)

        return migration_view.single(req, migration)

    def delete(self, req, id):
        try:
            self._migration_api.delete(req.environ['coriolis.context'], id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(MigrationController())
