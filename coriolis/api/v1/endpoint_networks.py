# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import endpoint_resources_view
from coriolis.api import wsgi as api_wsgi
from coriolis.endpoint_resources import api
from coriolis.policies import endpoints as endpoint_policies
from coriolis import utils

from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class EndpointNetworkController(api_wsgi.Controller):
    def __init__(self):
        self._network_api = api.API()
        super(EndpointNetworkController, self).__init__()

    def index(self, req, endpoint_id):
        context = req.environ['coriolis.context']
        context.can("%s:list_networks" % (
            endpoint_policies.ENDPOINTS_POLICY_PREFIX))
        env = req.GET.get("env")
        if env is not None:
            env = utils.decode_base64_param(env, is_json=True)
        else:
            env = {}

        return endpoint_resources_view.networks_collection(
            req, self._network_api.get_endpoint_networks(
                context, endpoint_id, env))


def create_resource():
    return api_wsgi.Resource(EndpointNetworkController())
