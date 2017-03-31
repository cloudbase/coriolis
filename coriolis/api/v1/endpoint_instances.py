# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis.api import common
from coriolis.api import wsgi as api_wsgi
from coriolis.api.v1.views import endpoint_instance_view
from coriolis.endpoint_instances import api

LOG = logging.getLogger(__name__)


class EndpointInstanceController(api_wsgi.Controller):
    def __init__(self):
        self._instance_api = api.API()
        super(EndpointInstanceController, self).__init__()

    def index(self, req, endpoint_id):
        marker, limit = common.get_paging_params(req)

        return endpoint_instance_view.collection(
            req, self._instance_api.get_endpoint_instances(
                req.environ['coriolis.context'], endpoint_id, marker, limit))


def create_resource():
    return api_wsgi.Resource(EndpointInstanceController())
