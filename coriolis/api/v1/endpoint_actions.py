# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api import wsgi as api_wsgi
from coriolis.endpoints import api
from coriolis import exception
from coriolis.policies import endpoints as endpoint_policies

from webob import exc


class EndpointActionsController(api_wsgi.Controller):
    def __init__(self):
        self._endpoint_api = api.API()
        super(EndpointActionsController, self).__init__()

    @api_wsgi.action('validate-connection')
    def _validate_connection(self, req, id, body):
        context = req.environ['coriolis.context']
        context.can("%s:validate_connection" % (
            endpoint_policies.ENDPOINTS_POLICY_PREFIX))
        try:
            is_valid, message = self._endpoint_api.validate_connection(
                context, id)
            return {
                "validate-connection":
                    {"valid": is_valid, "message": message}
            }
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)

    @api_wsgi.action('invalidate')
    def _invalidate(self, req, id, body):
        context = req.environ['coriolis.context']
        context.can(
            "%s:invalidate" % endpoint_policies.ENDPOINTS_POLICY_PREFIX)
        try:
            self._endpoint_api.invalidate_cache(context, id)
            return {
                "invalidate": {
                    "success": True,
                    "message": "Instance list cache invalidation triggered."
                }
            }
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPBadRequest(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(EndpointActionsController())
