# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.api.v1 import endpoint_inventory as endpoint
from coriolis.endpoint_resources import api
from coriolis.tests import test_base
from coriolis import utils


class EndpointInventoryControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Endpoint Inventory v1 API"""

    def setUp(self):
        super(EndpointInventoryControllerTestCase, self).setUp()
        self.endpoint_api = endpoint.EndpointInventoryController()

    @mock.patch.object(utils, 'decode_base64_param')
    @mock.patch.object(api.API, 'get_endpoint_inventory_csv')
    def test_index(
        self,
        mock_get_endpoint_inventory_csv,
        mock_decode_base64_param,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        endpoint_id = mock.sentinel.endpoint_id
        mock_req.environ = {'coriolis.context': mock_context}
        env = mock.sentinel.env
        mock_req.GET = {'env': env}
        mock_get_endpoint_inventory_csv.return_value = 'vm_id,vm_name\n'

        response = self.endpoint_api.index(mock_req, endpoint_id)

        mock_context.can.assert_called_once_with(
            'migration:endpoints:export_inventory')
        mock_decode_base64_param.assert_called_once_with(env, is_json=True)
        mock_get_endpoint_inventory_csv.assert_called_once_with(
            mock_context, endpoint_id,
            mock_decode_base64_param.return_value)
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.content_type, 'text/csv')
        self.assertIn('attachment', response.headers['Content-Disposition'])
        self.assertIn(
            str(endpoint_id), response.headers['Content-Disposition'])

    @mock.patch.object(utils, 'decode_base64_param')
    @mock.patch.object(api.API, 'get_endpoint_inventory_csv')
    def test_index_no_env(
        self,
        mock_get_endpoint_inventory_csv,
        mock_decode_base64_param,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        endpoint_id = mock.sentinel.endpoint_id
        mock_req.environ = {'coriolis.context': mock_context}
        mock_req.GET = {}
        mock_get_endpoint_inventory_csv.return_value = 'vm_id,vm_name\n'

        response = self.endpoint_api.index(mock_req, endpoint_id)

        mock_decode_base64_param.assert_not_called()
        mock_get_endpoint_inventory_csv.assert_called_once_with(
            mock_context, endpoint_id, {})
        self.assertEqual(response.status_int, 200)
        self.assertEqual(response.content_type, 'text/csv')
