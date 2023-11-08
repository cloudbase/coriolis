# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import endpoint_options_view
from coriolis.api import wsgi as api_wsgi
from coriolis.endpoint_options import api
from coriolis.policies import endpoints as endpoint_policies
from coriolis import utils

from oslo_log import log as logging


LOG = logging.getLogger(__name__)


class EndpointSourceMinionPoolOptionsController(api_wsgi.Controller):
    def __init__(self):
        self._minion_pool_options_api = api.API()
        super(EndpointSourceMinionPoolOptionsController, self).__init__()

    def index(self, req, endpoint_id):
        context = req.environ['coriolis.context']
        context.can("%s:list_source_minion_pool_options" % (
            endpoint_policies.ENDPOINTS_POLICY_PREFIX))

        env = req.GET.get("env")
        if env is not None:
            env = utils.decode_base64_param(env, is_json=True)
        else:
            env = {}

        options = req.GET.get("options")
        if options is not None:
            options = utils.decode_base64_param(options, is_json=True)
        else:
            options = {}

        return endpoint_options_view.source_minion_pool_options_collection(
            (self._minion_pool_options_api.
             get_endpoint_source_minion_pool_options)(
                context, endpoint_id, env=env, option_names=options))


def create_resource():
    return api_wsgi.Resource(EndpointSourceMinionPoolOptionsController())
