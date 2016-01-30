import webob

from oslo_log import log as logging
from oslo_middleware import request_id
from oslo_serialization import jsonutils

from coriolis.api import wsgi
from coriolis import context
from coriolis.i18n import _

LOG = logging.getLogger(__name__)


class CoriolisKeystoneContext(wsgi.Middleware):
    @webob.dec.wsgify(RequestClass=wsgi.Request)
    def __call__(self, req):
        user = req.headers.get('X_USER')
        user = req.headers.get('X_USER_ID', user)
        if user is None:
            LOG.debug("Neither X_USER_ID nor X_USER found in request")
            return webob.exc.HTTPUnauthorized()

        # get the roles
        roles = [r.strip() for r in req.headers.get('X_ROLE', '').split(',')]
        if 'X_TENANT_ID' in req.headers:
            # This is the new header since Keystone went to ID/Name
            tenant = req.headers['X_TENANT_ID']
        else:
            # This is for legacy compatibility
            tenant = req.headers['X_TENANT']

        project_name = req.headers.get('X_TENANT_NAME')

        req_id = req.environ.get(request_id.ENV_REQUEST_ID)

        # Get the auth token
        auth_token = req.headers.get('X_AUTH_TOKEN',
                                     req.headers.get('X_STORAGE_TOKEN'))

        # Build a context, including the auth_token...
        remote_address = req.remote_addr

        service_catalog = None
        if req.headers.get('X_SERVICE_CATALOG') is not None:
            try:
                catalog_header = req.headers.get('X_SERVICE_CATALOG')
                service_catalog = jsonutils.loads(catalog_header)
            except ValueError:
                raise webob.exc.HTTPInternalServerError(
                    explanation=_('Invalid service catalog json.'))

        ctx = context.RequestContext(user,
                                     tenant,
                                     project_name=project_name,
                                     roles=roles,
                                     auth_token=auth_token,
                                     remote_address=remote_address,
                                     service_catalog=service_catalog,
                                     request_id=req_id)

        req.environ['coriolis.context'] = ctx
        return self.application
