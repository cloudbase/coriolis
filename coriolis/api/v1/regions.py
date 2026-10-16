# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from typing import NoReturn

from oslo_log import log as logging
from webob import exc

from coriolis import exception
from coriolis.api import wsgi as api_wsgi
from coriolis.api.v1 import utils as api_utils
from coriolis.api.v1.views import region_view
from coriolis.policies import regions as region_policies
from coriolis.regions import api

LOG = logging.getLogger(__name__)


class RegionController(api_wsgi.Controller):
    def __init__(self):
        self._region_api = api.API()
        super(RegionController, self).__init__()

    def show(self, req: api_wsgi.Request, id: str) -> dict[str, dict]:
        context = req.environ["coriolis.context"]
        context.can(region_policies.get_regions_policy_label("show"))
        region = self._region_api.get_region(context, id)
        if not region:
            raise exc.HTTPNotFound()

        return region_view.single(region)

    def index(self, req: api_wsgi.Request) -> dict[str, list]:
        context = req.environ["coriolis.context"]
        context.can(region_policies.get_regions_policy_label("list"))
        return region_view.collection(self._region_api.get_regions(context))

    @api_utils.format_keyerror_message(resource='region', method='create')
    def _validate_create_body(self, body: dict[str, dict]) -> tuple[str, str, bool]:
        region = body["region"]
        name = region["name"]
        description = region.get("description", "")
        enabled = region.get("enabled", True)
        return name, description, enabled

    def create(self, req: api_wsgi.Request, body: dict[str, dict]) -> dict[str, dict]:
        context = req.environ["coriolis.context"]
        context.can(region_policies.get_regions_policy_label("create"))
        (name, description, enabled) = self._validate_create_body(body)
        return region_view.single(
            self._region_api.create(
                context, region_name=name, description=description, enabled=enabled
            )
        )

    @api_utils.format_keyerror_message(resource='region', method='update')
    def _validate_update_body(self, body: dict[str, dict]) -> dict[str, str]:
        region = body["region"]
        return {
            k: region[k] for k in region.keys() & {"name", "description", "enabled"}
        }

    def update(
        self, req: api_wsgi.Request, id: str, body: dict[str, dict]
    ) -> dict[str, dict]:
        context = req.environ["coriolis.context"]
        context.can(region_policies.get_regions_policy_label("update"))
        updated_values = self._validate_update_body(body)
        return region_view.single(
            self._region_api.update(req.environ['coriolis.context'], id, updated_values)
        )

    def delete(self, req: api_wsgi.Request, id: str) -> NoReturn:
        context = req.environ["coriolis.context"]
        context.can(region_policies.get_regions_policy_label("delete"))
        try:
            self._region_api.delete(req.environ['coriolis.context'], id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource() -> api_wsgi.Resource:
    return api_wsgi.Resource(RegionController())
