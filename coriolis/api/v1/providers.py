# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis.api import wsgi as api_wsgi
from coriolis.providers import api
from coriolis.policies import general as general_policies

LOG = logging.getLogger(__name__)


class ProviderController(api_wsgi.Controller):
    def __init__(self):
        self._provider_api = api.API()
        super(ProviderController, self).__init__()

    def index(self, req):
        context = req.environ['coriolis.context']
        context.can(general_policies.get_providers_policy_label('list'))
        return {
            "providers": self._provider_api.get_available_providers(context)}


def create_resource():
    return api_wsgi.Resource(ProviderController())
