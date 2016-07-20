from webob import exc

from coriolis.api import wsgi as api_wsgi
from coriolis.api.v1.views import migration_view
from coriolis import constants
from coriolis import exception
from coriolis.migrations import api
from coriolis.providers import factory


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

    def _validate_create_body(self, body):
        migration = body["migration"]

        origin = migration["origin"]
        destination = migration["destination"]

        export_provider = factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_EXPORT, None)
        if not export_provider.validate_connection_info(
                origin.get("connection_info", {})):
            # TODO: use a decent exception
            raise exception.CoriolisException("Invalid connection info")

        import_provider = factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT, None)
        if not import_provider.validate_connection_info(
                destination.get("connection_info", {})):
            # TODO: use a decent exception
            raise exception.CoriolisException("Invalid connection info")

        if not import_provider.validate_target_environment(
                destination.get("target_environment", {})):
            raise exception.CoriolisException("Invalid target environment")

        return origin, destination, migration["instances"]

    def create(self, req, body):
        origin, destination, instances = self._validate_create_body(body)
        return migration_view.single(req, self._migration_api.start(
            req.environ['coriolis.context'], origin, destination, instances))

    def delete(self, req, id):
        try:
            self._migration_api.delete(req.environ['coriolis.context'], id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(MigrationController())
