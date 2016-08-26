# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

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
            force = (body["cancel"] or {}).get("force", False)

            self._migration_api.cancel(
                req.environ['coriolis.context'], id, force)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(MigrationActionsController())
