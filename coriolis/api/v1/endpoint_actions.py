# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from webob import exc

from coriolis import exception
from coriolis.api import wsgi as api_wsgi
from coriolis.api.v1 import utils as api_utils
from coriolis.endpoints import api
from coriolis.policies import endpoints as endpoint_policies


class EndpointActionsController(api_wsgi.Controller):
    def __init__(self):
        self._endpoint_api = api.API()
        super(EndpointActionsController, self).__init__()

    @api_utils.format_keyerror_message(resource='endpoint', method='validate')
    def _validate_connection_body(self, body):
        validate_connection = body["validate-connection"]
        if not isinstance(validate_connection, dict):
            raise exception.InvalidInput(
                'The "validate-connection" body must be an object containing '
                'the "platform" and "connection_info" of the endpoint'
            )
        platform = validate_connection["platform"]
        connection_info = validate_connection["connection_info"]
        mapped_regions = validate_connection.get("mapped_regions", [])
        return (platform, connection_info, mapped_regions)

    @api_wsgi.action('validate-connection')
    def _validate_endpoint(self, req, body):
        context = req.environ['coriolis.context']
        context.can(
            "%s:validate_connection" % (endpoint_policies.ENDPOINTS_POLICY_PREFIX)
        )
        platform, connection_info, mapped_regions = self._validate_connection_body(body)
        try:
            is_valid, message = self._endpoint_api.validate_connection(
                context, platform, connection_info, mapped_regions
            )
            return {"validate-connection": {"valid": is_valid, "message": message}}
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(EndpointActionsController())
