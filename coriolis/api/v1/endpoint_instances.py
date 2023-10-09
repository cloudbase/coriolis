# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api import common
from coriolis.api.v1.views import endpoint_resources_view
from coriolis.api import wsgi as api_wsgi
from coriolis.endpoint_resources import api
from coriolis.policies import endpoints as endpoint_policies
from coriolis import utils

from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class EndpointInstanceController(api_wsgi.Controller):
    def __init__(self):
        self._instance_api = api.API()
        super(EndpointInstanceController, self).__init__()

    def index(self, req, endpoint_id):
        context = req.environ['coriolis.context']
        context.can("%s:list_instances" % (
            endpoint_policies.ENDPOINTS_POLICY_PREFIX))
        marker, limit = common.get_paging_params(req)
        instance_name_pattern = req.GET.get("name")

        env = req.GET.get("env")
        if env is not None:
            env = utils.decode_base64_param(env, is_json=True)
        else:
            env = {}

        return endpoint_resources_view.instances_collection(
            req, self._instance_api.get_endpoint_instances(
                context, endpoint_id, env, marker, limit,
                instance_name_pattern))

    def show(self, req, endpoint_id, id):
        context = req.environ['coriolis.context']
        context.can("%s:get_instance" % (
            endpoint_policies.ENDPOINTS_POLICY_PREFIX))

        # WSGI does not allow encoded / chars (%2F) in the url
        # See e.g.: https://github.com/pallets/flask/issues/900
        id = utils.decode_base64_param(id)

        env = req.GET.get("env")
        if env is not None:
            env = utils.decode_base64_param(env, is_json=True)
        else:
            env = {}

        return endpoint_resources_view.instance_single(
            req, self._instance_api.get_endpoint_instance(
                req.environ['coriolis.context'], endpoint_id, env, id))


def create_resource():
    return api_wsgi.Resource(EndpointInstanceController())
