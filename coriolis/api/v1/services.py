# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from typing import NoReturn

from oslo_log import log as logging
from webob import exc

from coriolis import exception
from coriolis.api import wsgi as api_wsgi
from coriolis.api.v1 import utils as api_utils
from coriolis.api.v1.views import service_view
from coriolis.policies import services as service_policies
from coriolis.services import api

LOG = logging.getLogger(__name__)


class ServiceController(api_wsgi.Controller):
    def __init__(self):
        self._service_api = api.API()
        super(ServiceController, self).__init__()

    def show(self, req: api_wsgi.Request, id: str) -> dict:
        context = req.environ["coriolis.context"]
        context.can(service_policies.get_services_policy_label("show"))
        service = self._service_api.get_service(context, id)
        if not service:
            raise exc.HTTPNotFound()

        return service_view.single(service)

    def index(self, req: api_wsgi.Request) -> dict[str, list]:
        context = req.environ["coriolis.context"]
        context.can(service_policies.get_services_policy_label("list"))
        return service_view.collection(self._service_api.get_services(context))

    @api_utils.format_keyerror_message(resource='service', method='create')
    def _validate_create_body(self, body: dict[str, dict]) -> dict | tuple:
        service = body["service"]
        host = service["host"]
        binary = service["binary"]
        topic = service["topic"]
        mapped_regions = service.get('mapped_regions', [])
        enabled = service.get("enabled", True)
        return host, binary, topic, mapped_regions, enabled

    def create(self, req: api_wsgi.Request, body: dict[str, dict]) -> dict:
        context = req.environ["coriolis.context"]
        context.can(service_policies.get_services_policy_label("create"))
        (host, binary, topic, mapped_regions, enabled) = self._validate_create_body(
            body
        )
        return service_view.single(
            self._service_api.create(
                context,
                host=host,
                binary=binary,
                topic=topic,
                mapped_regions=mapped_regions,
                enabled=enabled,
            )
        )

    @api_utils.format_keyerror_message(resource='service', method='update')
    def _validate_update_body(self, body: dict[str, dict]) -> dict:
        service = body["service"]
        return {k: service[k] for k in service.keys() & {"enabled", "mapped_regions"}}

    def update(
        self, req: api_wsgi.Request, id: str, body: dict[str, dict]
    ) -> dict[str, dict]:
        context = req.environ["coriolis.context"]
        context.can(service_policies.get_services_policy_label("update"))
        updated_values = self._validate_update_body(body)
        return service_view.single(
            self._service_api.update(
                req.environ['coriolis.context'], id, updated_values
            )
        )

    def delete(self, req: api_wsgi.Request, id: str) -> NoReturn:
        context = req.environ["coriolis.context"]
        context.can(service_policies.get_services_policy_label("delete"))
        try:
            self._service_api.delete(req.environ['coriolis.context'], id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource() -> api_wsgi.Resource:
    return api_wsgi.Resource(ServiceController())
