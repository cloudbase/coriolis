# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis.api import wsgi as api_wsgi
from coriolis.providers import api

LOG = logging.getLogger(__name__)


class ProviderSchemasController(api_wsgi.Controller):
    def __init__(self):
        self._provider_api = api.API()
        super(ProviderSchemasController, self).__init__()

    def index(self, req, platform_name, provider_type):
        return {"schemas": self._provider_api.get_provider_schemas(
            req.environ["coriolis.context"], platform_name, provider_type)}


def create_resource():
    return api_wsgi.Resource(ProviderSchemasController())
