# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.api.v1 import endpoint_storage as endpoint
from coriolis.api.v1.views import endpoint_resources_view
from coriolis.endpoint_resources import api
from coriolis.tests import test_base
from coriolis import utils


class EndpointStorageControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Endpoint Storage api v1 API"""

    def setUp(self):
        super(EndpointStorageControllerTestCase, self).setUp()
        self.endpoint_api = endpoint.EndpointStorageController()

    @mock.patch.object(utils, 'decode_base64_param')
    @mock.patch.object(endpoint_resources_view, 'storage_collection')
    @mock.patch.object(api.API, 'get_endpoint_storage')
    def test_index(
        self,
        mock_storage_collection,
        mock_get_endpoint_storage,
        mock_decode_base64_param,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        endpoint_id = mock.sentinel.endpoint_id
        mock_req.environ = {'coriolis.context': mock_context}
        env = mock.sentinel.env
        mock_req.GET = {
            'env': env
        }

        result = self.endpoint_api.index(mock_req, endpoint_id)

        mock_context.can.assert_called_once_with(
            'migration:endpoints:list_storage')
        mock_decode_base64_param.assert_called_once_with(env, is_json=True)
        mock_storage_collection.assert_called_once_with(
            mock_context, endpoint_id,
            mock_decode_base64_param.return_value)
        mock_get_endpoint_storage.assert_called_once_with(
            mock_storage_collection.return_value)
        self.assertEqual(
            mock_get_endpoint_storage.return_value,
            result
        )

    @mock.patch.object(utils, 'decode_base64_param')
    @mock.patch.object(endpoint_resources_view, 'storage_collection')
    @mock.patch.object(api.API, 'get_endpoint_storage')
    def test_index_no_env(
        self,
        mock_storage_collection,
        mock_get_endpoint_storage,
        mock_decode_base64_param,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        endpoint_id = mock.sentinel.endpoint_id
        mock_req.environ = {'coriolis.context': mock_context}
        mock_req.GET = {}

        result = self.endpoint_api.index(mock_req, endpoint_id)

        mock_decode_base64_param.assert_not_called()
        mock_storage_collection.assert_called_once_with(
            mock_context, endpoint_id, {})
        mock_get_endpoint_storage.assert_called_once_with(
            mock_storage_collection.return_value)
        self.assertEqual(
            mock_get_endpoint_storage.return_value,
            result
        )
