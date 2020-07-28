# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging
from webob import exc

from coriolis import exception
from coriolis.api.v1.views import region_view
from coriolis.api import wsgi as api_wsgi
from coriolis.policies import regions as region_policies
from coriolis.regions import api

LOG = logging.getLogger(__name__)


class RegionController(api_wsgi.Controller):
    def __init__(self):
        self._region_api = api.API()
        super(RegionController, self).__init__()

    def show(self, req, id):
        context = req.environ["coriolis.context"]
        context.can(region_policies.get_regions_policy_label("show"))
        region = self._region_api.get_region(context, id)
        if not region:
            raise exc.HTTPNotFound()

        return region_view.single(req, region)

    def index(self, req):
        context = req.environ["coriolis.context"]
        context.can(region_policies.get_regions_policy_label("list"))
        return region_view.collection(
            req, self._region_api.get_regions(context))

    def _validate_create_body(self, body):
        try:
            region = body["region"]
            name = region["name"]
            description = region.get("description", "")
            enabled = region.get("enabled", True)
            return name, description, enabled
        except Exception as ex:
            LOG.exception(ex)
            if hasattr(ex, "message"):
                msg = ex.message
            else:
                msg = str(ex)
            raise exception.InvalidInput(msg)

    def create(self, req, body):
        context = req.environ["coriolis.context"]
        context.can(region_policies.get_regions_policy_label("create"))
        (name, description, enabled) = self._validate_create_body(body)
        return region_view.single(req, self._region_api.create(
            context, region_name=name, description=description,
            enabled=enabled))

    def _validate_update_body(self, body):
        try:
            region = body["region"]
            return {k: region[k] for k in region.keys() &
                    {"name", "description", "enabled"}}
        except Exception as ex:
            LOG.exception(ex)
            if hasattr(ex, "message"):
                msg = ex.message
            else:
                msg = str(ex)
            raise exception.InvalidInput(msg)

    def update(self, req, id, body):
        context = req.environ["coriolis.context"]
        context.can(region_policies.get_regions_policy_label("update"))
        updated_values = self._validate_update_body(body)
        return region_view.single(req, self._region_api.update(
            req.environ['coriolis.context'], id, updated_values))

    def delete(self, req, id):
        context = req.environ["coriolis.context"]
        context.can(region_policies.get_regions_policy_label("delete"))
        try:
            self._region_api.delete(req.environ['coriolis.context'], id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(RegionController())
