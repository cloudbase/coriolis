# Copyright 2022 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from oslo_middleware import request_id
import webob

from coriolis.api.middleware import auth
from coriolis.api import wsgi
from coriolis import context
from coriolis.tests import test_base


class CoriolisKeystoneContextTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis api middleware auth."""

    def setUp(self):
        super(CoriolisKeystoneContextTestCase, self).setUp()
        self.auth = auth.CoriolisKeystoneContext(wsgi.Middleware)

    @mock.patch.object(context, "RequestContext")
    @mock.patch.object(auth.CoriolisKeystoneContext, "_get_project_id")
    @mock.patch.object(auth.CoriolisKeystoneContext, "_get_user")
    def test__call__(
        self,
        mock_get_user,
        mock__get_project_id,
        mock_request_context
    ):
        req_mock = mock.Mock()

        expected_roles = ['1', '2', '3']
        expected_project_name = 'mock_project_name'
        expected_project_domain_name = 'mock_project_domain_name'
        expected_user_domain_name = 'mock_user_domain_name'
        expected_auth_token = 'mock_token123'
        expected_remote_address = 'mock_addr'
        expected_service_catalog = {"catalog1": ["service1", "service2"]}
        expected_req_id = 'mock_req_id'

        req_mock.remote_addr = expected_remote_address
        req_mock.environ = {
            request_id.ENV_REQUEST_ID: expected_req_id
        }

        req_mock.headers = {
            'X_ROLE': '1,2,3',
            'X_TENANT_NAME': expected_project_name,
            'X-Project-Domain-Name': expected_project_domain_name,
            'X-User-Domain-Name': expected_user_domain_name,
            'X_AUTH_TOKEN': expected_auth_token,
            'X_SERVICE_CATALOG':
                str(expected_service_catalog).replace("'", '"'),
        }

        result = self.auth(req_mock)

        self.assertEqual(
            self.auth.application,
            result
        )

        mock_get_user.assert_called_once_with(req_mock)
        mock__get_project_id.assert_called_once_with(req_mock)
        mock_request_context.assert_called_once_with(
            mock_get_user.return_value,
            mock__get_project_id.return_value,
            project_name=expected_project_name,
            project_domain_name=expected_project_domain_name,
            user_domain_name=expected_user_domain_name,
            roles=expected_roles,
            auth_token=expected_auth_token,
            remote_address=expected_remote_address,
            service_catalog=expected_service_catalog,
            request_id=expected_req_id
        )

        self.assertEqual(
            req_mock.environ['coriolis.context'],
            mock_request_context.return_value
        )

    @mock.patch.object(context, "RequestContext")
    @mock.patch.object(auth.CoriolisKeystoneContext, "_get_project_id")
    @mock.patch.object(auth.CoriolisKeystoneContext, "_get_user")
    def test__call__req_id_is_bytes(
        self,
        mock_get_user,
        mock__get_project_id,
        mock_request_context
    ):
        req_mock = mock.Mock()

        expected_roles = ['1', '2', '3']
        expected_project_name = 'mock_project_name'
        expected_project_domain_name = 'mock_project_domain_name'
        expected_user_domain_name = 'mock_user_domain_name'
        expected_auth_token = 'mock_token123'
        expected_remote_address = 'mock_addr'
        expected_service_catalog = {"catalog1": ["service1", "service2"]}
        expected_req_id = 'mock_req_id'

        req_mock.remote_addr = expected_remote_address
        req_mock.environ = {
            request_id.ENV_REQUEST_ID: expected_req_id.encode()
        }

        req_mock.headers = {
            'X_ROLE': '1,2,3',
            'X_TENANT_NAME': expected_project_name,
            'X-Project-Domain-Name': expected_project_domain_name,
            'X-User-Domain-Name': expected_user_domain_name,
            'X_AUTH_TOKEN': expected_auth_token,
            'X_SERVICE_CATALOG':
                str(expected_service_catalog).replace("'", '"'),
        }

        result = self.auth(req_mock)

        self.assertEqual(
            self.auth.application,
            result
        )

        mock_get_user.assert_called_once_with(req_mock)
        mock__get_project_id.assert_called_once_with(req_mock)
        mock_request_context.assert_called_once_with(
            mock_get_user.return_value,
            mock__get_project_id.return_value,
            project_name=expected_project_name,
            project_domain_name=expected_project_domain_name,
            user_domain_name=expected_user_domain_name,
            roles=expected_roles,
            auth_token=expected_auth_token,
            remote_address=expected_remote_address,
            service_catalog=expected_service_catalog,
            request_id=expected_req_id
        )

        self.assertEqual(
            req_mock.environ['coriolis.context'],
            mock_request_context.return_value
        )

    @mock.patch.object(auth.CoriolisKeystoneContext, "_get_project_id")
    @mock.patch.object(auth.CoriolisKeystoneContext, "_get_user")
    def test__call__invalid_service_catalog(
        self,
        mock_get_user,
        mock__get_project_id,
    ):
        req_mock = mock.Mock()

        invalid_service_catalog = "mock_invalid_service_catalog"

        req_mock.headers = {
            'X_SERVICE_CATALOG': invalid_service_catalog,
        }

        self.assertRaises(
            webob.exc.HTTPInternalServerError,
            self.auth,
            req_mock,
        )

    @mock.patch.object(auth.CoriolisKeystoneContext, "_get_user")
    def test__call__no_headers(
        self,
        mock_get_user,
    ):
        req_mock = mock.Mock()
        mock_get_user.side_effect = webob.exc.HTTPUnauthorized()

        req_mock.headers = {}

        self.assertRaises(
            webob.exc.HTTPUnauthorized,
            self.auth,
            req_mock
        )

    def test_get_project_id_tenant_id(self):
        req_mock = mock.Mock()

        expected_result = 'mock_tenant'

        req_mock.headers = {
            'X_TENANT_ID': expected_result,
        }

        result = self.auth._get_project_id(req_mock)

        self.assertEqual(
            expected_result,
            result
        )

    def test_get_project_id_tenant(self):
        req_mock = mock.Mock()

        expected_result = 'mock_tenant'

        req_mock.headers = {
            'X_TENANT': expected_result,
        }

        result = self.auth._get_project_id(req_mock)

        self.assertEqual(
            expected_result,
            result
        )

    def test_get_project_id_no_tenant(self):
        req_mock = mock.Mock()

        req_mock.headers = {}

        self.assertRaises(
            webob.exc.HTTPBadRequest,
            self.auth._get_project_id,
            req_mock
        )

    def test_get_user(self):
        req_mock = mock.Mock()

        mock_user = 'mock_user'

        expected_result = 'mock_user_id'

        req_mock.headers = {
            'X_USER': mock_user,
            'X_USER_ID': expected_result,
        }

        result = self.auth._get_user(req_mock)

        self.assertEqual(
            expected_result,
            result
        )

    def test_get_user_has_user(self):
        req_mock = mock.Mock()

        expected_result = 'mock_user'

        req_mock.headers = {
            'X_USER': expected_result,
        }

        result = self.auth._get_user(req_mock)

        self.assertEqual(
            expected_result,
            result
        )

    def test_get_user_has_user_id(self):
        req_mock = mock.Mock()

        expected_result = 'mock_user_id'

        req_mock.headers = {
            'X_USER_ID': expected_result,
        }

        result = self.auth._get_user(req_mock)

        self.assertEqual(
            expected_result,
            result
        )

    def test_get_user_none(self):
        req_mock = mock.Mock()

        req_mock.headers = {}

        self.assertRaises(
            webob.exc.HTTPUnauthorized,
            self.auth._get_user,
            req_mock
        )
