# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging
from oslo_middleware import request_id
from oslo_serialization import jsonutils
import webob

from coriolis.api import wsgi
from coriolis import context
from coriolis.i18n import _

LOG = logging.getLogger(__name__)


class CoriolisKeystoneContext(wsgi.Middleware):
    def _get_project_id(self, req):
        if 'X_TENANT_ID' in req.headers:
            # This is the new header since Keystone went to ID/Name
            return req.headers['X_TENANT_ID']
        elif 'X_TENANT' in req.headers:
            # This is for legacy compatibility
            return req.headers['X_TENANT']
        else:
            raise webob.exc.HTTPBadRequest(
                explanation=_("No 'X_TENANT_ID' or 'X_TENANT' passed."))

    def _get_user(self, req):
        user = req.headers.get('X_USER')
        user = req.headers.get('X_USER_ID', user)
        if user is None:
            raise webob.exc.HTTPUnauthorized(
                explanation=_("Neither X_USER_ID nor X_USER found in request"))
        return user

    @webob.dec.wsgify(RequestClass=wsgi.Request)
    def __call__(self, req):
        user = self._get_user(req)

        project_id = self._get_project_id(req)

        # get the roles
        roles = [r.strip() for r in req.headers.get('X_ROLE', '').split(',')]

        project_name = req.headers.get('X_TENANT_NAME')
        project_domain_name = req.headers.get('X-Project-Domain-Name')
        user_domain_name = req.headers.get('X-User-Domain-Name')

        req_id = req.environ.get(request_id.ENV_REQUEST_ID)
        # TODO(alexpilotti): Check why it's not str
        if isinstance(req_id, bytes):
            req_id = req_id.decode()

        # Get the auth token
        auth_token = req.headers.get('X_AUTH_TOKEN')

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
                                     project_id,
                                     project_name=project_name,
                                     project_domain_name=project_domain_name,
                                     user_domain_name=user_domain_name,
                                     roles=roles,
                                     auth_token=auth_token,
                                     remote_address=remote_address,
                                     service_catalog=service_catalog,
                                     request_id=req_id)

        req.environ['coriolis.context'] = ctx
        return self.application
