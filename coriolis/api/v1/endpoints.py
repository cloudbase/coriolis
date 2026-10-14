# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from typing import NoReturn

from oslo_log import log as logging
from webob import exc

from coriolis import exception
from coriolis.api import wsgi as api_wsgi
from coriolis.api.v1 import utils as api_utils
from coriolis.api.v1.views import endpoint_view
from coriolis.endpoints import api
from coriolis.policies import endpoints as endpoint_policies

LOG = logging.getLogger(__name__)


class EndpointController(api_wsgi.Controller):
    def __init__(self):
        self._endpoint_api = api.API()
        super(EndpointController, self).__init__()

    def show(self, req: api_wsgi.Request, id: str) -> dict[str, dict]:
        context = req.environ["coriolis.context"]
        context.can(endpoint_policies.get_endpoints_policy_label("show"))
        endpoint = self._endpoint_api.get_endpoint(context, id)
        if not endpoint:
            raise exc.HTTPNotFound()

        return endpoint_view.single(endpoint)

    def index(self, req: api_wsgi.Request) -> dict[str, list]:
        context = req.environ["coriolis.context"]
        context.can(endpoint_policies.get_endpoints_policy_label("list"))
        return endpoint_view.collection(self._endpoint_api.get_endpoints(context))

    @api_utils.format_keyerror_message(resource='endpoint', method='create')
    def _validate_create_body(
        self, body: dict[str, dict]
    ) -> tuple[str, str, str, dict, list]:
        endpoint = body["endpoint"]
        name = endpoint["name"]
        description = endpoint.get("description")
        endpoint_type = endpoint["type"]
        connection_info = endpoint["connection_info"]
        mapped_regions = endpoint.get("mapped_regions", [])
        return (name, endpoint_type, description, connection_info, mapped_regions)

    def create(self, req: api_wsgi.Request, body: dict[str, dict]) -> dict[str, dict]:
        context = req.environ["coriolis.context"]
        context.can(endpoint_policies.get_endpoints_policy_label("create"))
        (name, endpoint_type, description, connection_info, mapped_regions) = (
            self._validate_create_body(body)
        )
        return endpoint_view.single(
            self._endpoint_api.create(
                context,
                name,
                endpoint_type,
                description,
                connection_info,
                mapped_regions,
            )
        )

    @api_utils.format_keyerror_message(resource='endpoint', method='update')
    def _validate_update_body(self, body: dict[str, dict]) -> dict | tuple:
        endpoint = body["endpoint"]
        return {
            k: endpoint[k]
            for k in endpoint.keys()
            & {"name", "description", "connection_info", "mapped_regions"}
        }

    def update(
        self, req: api_wsgi.Request, id: str, body: dict[str, dict]
    ) -> dict[str, dict]:
        context = req.environ["coriolis.context"]
        context.can(endpoint_policies.get_endpoints_policy_label("update"))
        updated_values = self._validate_update_body(body)
        return endpoint_view.single(
            self._endpoint_api.update(
                req.environ['coriolis.context'], id, updated_values
            )
        )

    def delete(self, req: api_wsgi.Request, id: str) -> NoReturn:
        context = req.environ["coriolis.context"]
        context.can(endpoint_policies.get_endpoints_policy_label("delete"))
        try:
            self._endpoint_api.delete(req.environ['coriolis.context'], id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource() -> api_wsgi.Resource:
    return api_wsgi.Resource(EndpointController())
