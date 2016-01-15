from oslo_log import log as logging
from oslo_service import wsgi
import routes

import webob.dec
import webob.exc

from coriolis import api
from coriolis.i18n import _LI, _LE
from coriolis.api.v1 import migrations

LOG = logging.getLogger(__name__)


class ExtensionManager(object):
    def get_resources(self):
        return []

    def get_controller_extensions(self):
        return []


class APIRouter(api.APIRouter):
    ExtensionManager = ExtensionManager

    def _setup_routes(self, mapper, ext_mgr):
        mapper.redirect("", "/")

        self.resources['migrations'] = migrations.create_resource()
        mapper.resource('migration', 'migrations',
                        controller=self.resources['migrations'],
                        collection={'detail': 'GET'},
                        member={'action': 'POST'})
