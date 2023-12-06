# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from webob import exc

from coriolis.api.v1 import endpoints
from coriolis.api.v1.views import endpoint_view
from coriolis.endpoints import api
from coriolis import exception
from coriolis.tests import test_base
from coriolis.tests import testutils


class EndpointControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Endpoint v1 api"""

    def setUp(self):
        super(EndpointControllerTestCase, self).setUp()
        self.endpoint_api = endpoints.EndpointController()

    @mock.patch.object(endpoint_view, 'single')
    @mock.patch.object(api.API, 'get_endpoint')
    def test_show(
        self,
        mock_get_endpoint,
        mock_single,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id

        result = self.endpoint_api.show(mock_req, id)

        mock_context.can.assert_called_once_with('migration:endpoints:show')
        mock_get_endpoint.assert_called_once_with(mock_context, id)
        mock_single.assert_called_once_with(
            mock_get_endpoint.return_value
        )
        self.assertEqual(
            mock_single.return_value,
            result
        )

    @mock.patch.object(api.API, 'get_endpoint')
    def test_show_no_endpoint(
        self,
        mock_get_endpoint,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_get_endpoint.return_value = None

        self.assertRaises(
            exc.HTTPNotFound,
            self.endpoint_api.show,
            mock_req,
            id
        )
        mock_context.can.assert_called_once_with('migration:endpoints:show')
        mock_get_endpoint.assert_called_once_with(mock_context, id)

    @mock.patch.object(endpoint_view, 'collection')
    @mock.patch.object(api.API, 'get_endpoints')
    def test_index(
        self,
        mock_get_endpoints,
        mock_collection,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}

        result = self.endpoint_api.index(mock_req)

        mock_context.can.assert_called_once_with('migration:endpoints:list')
        mock_get_endpoints.assert_called_once_with(mock_context)
        mock_collection.assert_called_once_with(
            mock_get_endpoints.return_value
        )
        self.assertEqual(
            mock_collection.return_value,
            result
        )

    def test__validate_create_body(self):
        mock_body = {
            'endpoint':
                {
                    'name': 'mock_name',
                    'type': 'mock_type',
                    'connection_info': 'mock_connection_info',
                }
        }
        endpoint = testutils.get_wrapped_function(
            self.endpoint_api._validate_create_body)(
                self.endpoint_api,
                body=mock_body,  # type: ignore
        )
        self.assertEqual(
            ('mock_name', 'mock_type', None, 'mock_connection_info', []),
            endpoint
        )

    def test__validate_create_body_all_keys(self):
        mock_body = {
            'endpoint':
                {
                    'name': 'mock_name',
                    'type': 'mock_type',
                    'description': 'mock_description',
                    'connection_info': 'mock_connection_info',
                    'mapped_regions': ['mapped_region_1', 'mapped_region_1'],
                }
        }
        endpoint = testutils.get_wrapped_function(
            self.endpoint_api._validate_create_body)(
                self.endpoint_api,
                body=mock_body,  # type: ignore
        )
        self.assertEqual(
            ('mock_name', 'mock_type', 'mock_description',
             'mock_connection_info', ['mapped_region_1', 'mapped_region_1']),
            endpoint
        )

    def test__validate_create_body_no_endpoint(self):
        mock_body = {}
        self.assertRaises(
            KeyError,
            testutils.get_wrapped_function(
                self.endpoint_api._validate_create_body),
            self.endpoint_api,
            body=mock_body,  # type: ignore
        )

    @mock.patch.object(endpoint_view, 'single')
    @mock.patch.object(api.API, 'create')
    @mock.patch.object(endpoints.EndpointController, '_validate_create_body')
    def test_create(
        self,
        mock__validate_create_body,
        mock_create,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        body = mock.sentinel.body
        mock__validate_create_body.return_value = (
            mock.sentinel.name,
            mock.sentinel.endpoint_type,
            mock.sentinel.description,
            mock.sentinel.connection_info,
            mock.sentinel.mapped_regions
        )

        result = self.endpoint_api.create(mock_req, body)

        mock_context.can.assert_called_once_with('migration:endpoints:create')
        mock_create.assert_called_once_with(
            mock_context,
            mock.sentinel.name,
            mock.sentinel.endpoint_type,
            mock.sentinel.description,
            mock.sentinel.connection_info,
            mock.sentinel.mapped_regions
        )
        self.assertEqual(
            mock_single.return_value,
            result
        )

    def test__validate_update_body(self):
        mock_body = {
            'endpoint':
                {
                    'name': 'mock_name',
                    'description': 'mock_description',
                    'connection_info': 'mock_connection_info',
                    'mapped_regions': ['mapped_region_1', 'mapped_region_1'],
                }
        }
        endpoint = testutils.get_wrapped_function(
            self.endpoint_api._validate_update_body)(
                self.endpoint_api,
                body=mock_body,  # type: ignore
        )
        self.assertEqual(
            mock_body['endpoint'],
            endpoint
        )

    def test__validate_update_body_no_keys(self):
        mock_body = {
            'endpoint': {}
        }
        endpoint = testutils.get_wrapped_function(
            self.endpoint_api._validate_update_body)(
                self.endpoint_api,
                body=mock_body,  # type: ignore
        )
        self.assertEqual(
            {},
            endpoint
        )

    def test__validate_update_body_no_endpoint(self):
        mock_body = {}
        self.assertRaises(
            KeyError,
            testutils.get_wrapped_function(
                self.endpoint_api._validate_update_body),
            self.endpoint_api,
            body=mock_body,  # type: ignore
        )

    @mock.patch.object(endpoint_view, 'single')
    @mock.patch.object(api.API, 'update')
    @mock.patch.object(endpoints.EndpointController, '_validate_update_body')
    def test_update(
        self,
        mock__validate_update_body,
        mock_update,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        body = mock.sentinel.body

        result = self.endpoint_api.update(mock_req, id, body)

        mock_context.can.assert_called_once_with('migration:endpoints:update')
        mock__validate_update_body.assert_called_once_with(body)
        mock_update.assert_called_once_with(
            mock_context, id,
            mock__validate_update_body.return_value
        )
        self.assertEqual(
            mock_single.return_value,
            result
        )

    @mock.patch.object(api.API, 'delete')
    def test_delete(
        self,
        mock_delete
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id

        self.assertRaises(
            exc.HTTPNoContent,
            self.endpoint_api.delete,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with('migration:endpoints:delete')
        mock_delete.assert_called_once_with(mock_context, id)

    @mock.patch.object(api.API, 'delete')
    def test_delete_not_found(
        self,
        mock_delete
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_delete.side_effect = exception.NotFound()

        self.assertRaises(
            exc.HTTPNotFound,
            self.endpoint_api.delete,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with('migration:endpoints:delete')
        mock_delete.assert_called_once_with(mock_context, id)
