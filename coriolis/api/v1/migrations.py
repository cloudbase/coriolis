from coriolis.api import wsgi as api_wsgi
from coriolis.api.v1.views import migration_view
from coriolis import constants
from coriolis.migrations import api
from coriolis.providers import factory


class MigrationController(object):
    def __init__(self):
        self._migration_api = api.API()
        super(MigrationController, self).__init__()

    def show(self, req, id):
        return migration_view.format_migration(
            req, self._migration_api.get_migration(id))

    def index(self, req):
        return migration_view.collection(
            req, self._migration_api.get_migrations())

    def detail(self, req):
        return migration_view.collection(
            req, self._migration_api.get_migrations())

    def _validate_create_body(self, body):
        migration = body["migration"]

        origin = migration["origin"]
        destination = migration["destination"]

        export_provider = factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_EXPORT)
        if not export_provider.validate_connection_info(
                origin["connection_info"]):
            # TODO: use a decent exception
            raise Exception("Invalid connection info")

        import_provider = factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT)
        if not import_provider.validate_connection_info(
                destination["connection_info"]):
            # TODO: use a decent exception
            raise Exception("Invalid connection info")

        return origin, destination, migration["instances"]

    def create(self, req, body):
        origin, destination, instances = self._validate_create_body(body)
        self._migration_api.start(origin, destination, instances)

    def delete(self, req, id):
        self._migration_api.stop(id)


def create_resource():
    return api_wsgi.Resource(MigrationController())
