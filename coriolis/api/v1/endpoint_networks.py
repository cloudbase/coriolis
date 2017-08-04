# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis.api import wsgi as api_wsgi
from coriolis.api.v1.views import endpoint_network_view
from coriolis.endpoint_networks import api
from coriolis import utils

LOG = logging.getLogger(__name__)


class EndpointNetworkController(api_wsgi.Controller):
    def __init__(self):
        self._network_api = api.API()
        super(EndpointNetworkController, self).__init__()

    def index(self, req, endpoint_id):
        env = req.GET.get("env")
        if env is not None:
            env = utils.decode_base64_param(env, is_json=True)

        return endpoint_network_view.collection(
            req, self._network_api.get_endpoint_networks(
                req.environ['coriolis.context'], endpoint_id, env))


def create_resource():
    return api_wsgi.Resource(EndpointNetworkController())
