# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from webob import exc

from coriolis.api import wsgi as api_wsgi
from coriolis import exception
from coriolis.endpoints import api


class EndpointActionsController(api_wsgi.Controller):
    def __init__(self):
        self._endpoint_api = api.API()
        super(EndpointActionsController, self).__init__()

    @api_wsgi.action('validate-connection')
    def _cancel(self, req, id, body):
        try:
            is_valid, message = self._endpoint_api.validate_connection(
                req.environ['coriolis.context'], id)
            return {
                "validate-connection":
                    {"valid": is_valid, "message": message}
            }
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(EndpointActionsController())
