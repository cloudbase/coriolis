# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import logging

from coriolis import exception
from coriolis.api import wsgi as api_wsgi
from coriolis.api.v1.views import diagnostic_view
from coriolis.diagnostics import api
from coriolis.policies import diagnostics


LOG = logging.getLogger(__name__)


class DiagnosticsController(api_wsgi.Controller):
    def __init__(self):
        self._diag_api = api.API()
        super(DiagnosticsController, self).__init__()

    def index(self, req):
        context = req.environ['coriolis.context']
        context.can(
            diagnostics.get_diagnostics_policy_label("get"))

        return diagnostic_view.collection(
            req, self._diag_api.get(context))


def create_resource():
    return api_wsgi.Resource(DiagnosticsController())
