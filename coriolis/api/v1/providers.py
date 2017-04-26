# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis.api import wsgi as api_wsgi
from coriolis.providers import api

LOG = logging.getLogger(__name__)


class ProviderController(api_wsgi.Controller):
    def __init__(self):
        self._provider_api = api.API()
        super(ProviderController, self).__init__()

    def index(self, req):
        return {"providers": self._provider_api.get_available_providers(
            req.environ['coriolis.context'])}


def create_resource():
    return api_wsgi.Resource(ProviderController())
