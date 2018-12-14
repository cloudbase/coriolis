# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis import utils
from coriolis.api.v1.views import endpoint_storage_view
from coriolis.api import wsgi as api_wsgi
from coriolis.endpoint_storage import api
from coriolis.policies import endpoints as endpoint_policies

LOG = logging.getLogger(__name__)


class EndpointStorageController(api_wsgi.Controller):
    def __init__(self):
        self._storage_api = api.API()
        super(EndpointStorageController, self).__init__()

    def index(self, req, endpoint_id):
        context = req.environ['coriolis.context']
        context.can("%s:list_storage" % (
            endpoint_policies.ENDPOINTS_POLICY_PREFIX))
        env = req.GET.get("env")
        if env is not None:
            env = utils.decode_base64_param(env, is_json=True)
        else:
            env = {}

        return endpoint_storage_view.collection(
            req, self._storage_api.get_endpoint_storage(
                context, endpoint_id, env))


def create_resource():
    return api_wsgi.Resource(EndpointStorageController())
