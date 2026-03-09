# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

import webob

from coriolis.api import wsgi as api_wsgi
from coriolis.endpoint_resources import api
from coriolis.policies import endpoints as endpoint_policies
from coriolis import utils

from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class EndpointInventoryController(api_wsgi.Controller):
    """Returns a VM inventory CSV for endpoints that support it."""

    def __init__(self):
        self._endpoint_resources_api = api.API()
        super(EndpointInventoryController, self).__init__()

    def index(self, req, endpoint_id):
        context = req.environ['coriolis.context']
        context.can("%s:export_inventory" % (
            endpoint_policies.ENDPOINTS_POLICY_PREFIX))

        env = req.GET.get("env")
        if env is not None:
            env = utils.decode_base64_param(env, is_json=True)
        else:
            env = {}

        csv_content = self._endpoint_resources_api.get_endpoint_inventory_csv(
            context, endpoint_id, env)

        response = webob.Response()
        response.status_int = 200
        response.content_type = 'text/csv'
        response.charset = 'utf-8'
        response.headers['Content-Disposition'] = (
            'attachment; filename="vm_inventory_%s.csv"' % endpoint_id)
        response.body = csv_content.encode('utf-8')
        return response


def create_resource():
    return api_wsgi.Resource(EndpointInventoryController())
