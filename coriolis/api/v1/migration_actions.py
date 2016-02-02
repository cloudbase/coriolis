from webob import exc

from coriolis.api import wsgi as api_wsgi
from coriolis import exception
from coriolis.migrations import api


class MigrationActionsController(api_wsgi.Controller):
    def __init__(self):
        self._migration_api = api.API()
        super(MigrationActionsController, self).__init__()

    @api_wsgi.action('cancel')
    def _cancel(self, req, id, body):
        try:
            self._migration_api.cancel(req.environ['coriolis.context'], id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(MigrationActionsController())
