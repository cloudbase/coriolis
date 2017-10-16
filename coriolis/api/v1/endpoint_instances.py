# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis.api import common
from coriolis.api.v1.views import endpoint_instance_view
from coriolis.api import wsgi as api_wsgi
from coriolis.endpoint_instances import api
from coriolis import utils

LOG = logging.getLogger(__name__)


class EndpointInstanceController(api_wsgi.Controller):
    def __init__(self):
        self._instance_api = api.API()
        super(EndpointInstanceController, self).__init__()

    def index(self, req, endpoint_id):
        marker, limit = common.get_paging_params(req)
        instance_name_pattern = req.GET.get("name")

        return endpoint_instance_view.collection(
            req, self._instance_api.get_endpoint_instances(
                req.environ['coriolis.context'], endpoint_id, marker, limit,
                instance_name_pattern))

    def show(self, req, endpoint_id, id):
        # WSGI does not allow encoded / chars (%2F) in the url
        # See e.g.: https://github.com/pallets/flask/issues/900
        id = utils.decode_base64_param(id)

        return endpoint_instance_view.single(
            req, self._instance_api.get_endpoint_instance(
                req.environ['coriolis.context'], endpoint_id, id))


def create_resource():
    return api_wsgi.Resource(EndpointInstanceController())
