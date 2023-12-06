# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.api import common
from coriolis.api.v1 import endpoint_instances as endpoint
from coriolis.api.v1.views import endpoint_resources_view
from coriolis.endpoint_resources import api
from coriolis.tests import test_base
from coriolis import utils


class EndpointInstanceControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Endpoint Instance v1 API"""

    def setUp(self):
        super(EndpointInstanceControllerTestCase, self).setUp()
        self.endpoint_api = endpoint.EndpointInstanceController()

    @mock.patch.object(common, 'get_paging_params')
    @mock.patch.object(utils, 'decode_base64_param')
    @mock.patch.object(endpoint_resources_view, 'instances_collection')
    @mock.patch.object(api.API, 'get_endpoint_instances')
    def test_index(
        self,
        mock_get_endpoint_instances,
        mock_instances_collection,
        mock_decode_base64_param,
        mock_get_paging_params,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        endpoint_id = mock.sentinel.endpoint_id
        mock_req.environ = {'coriolis.context': mock_context}
        env = mock.sentinel.env
        instance_name_pattern = mock.sentinel.instance_name_pattern
        mock_req.GET = {
            'env': env,
            'name': instance_name_pattern
        }
        marker = 'mock_marker'
        limit = 'mock_limit'
        mock_get_paging_params.return_value = (marker, limit)

        result = self.endpoint_api.index(mock_req, endpoint_id)

        mock_context.can.assert_called_once_with(
            'migration:endpoints:list_instances')
        mock_get_paging_params.assert_called_once_with(mock_req)
        mock_decode_base64_param.assert_called_once_with(env, is_json=True)
        mock_get_endpoint_instances.assert_called_once_with(
            mock_context, endpoint_id,
            mock_decode_base64_param.return_value,
            marker, limit, instance_name_pattern)
        mock_instances_collection.assert_called_once_with(
            mock_get_endpoint_instances.return_value)
        self.assertEqual(
            mock_instances_collection.return_value,
            result
        )

    @mock.patch.object(common, 'get_paging_params')
    @mock.patch.object(utils, 'decode_base64_param')
    @mock.patch.object(endpoint_resources_view, 'instances_collection')
    @mock.patch.object(api.API, 'get_endpoint_instances')
    def test_index_no_env_and_options(
        self,
        mock_get_endpoint_instances,
        mock_instances_collection,
        mock_decode_base64_param,
        mock_get_paging_params,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        endpoint_id = mock.sentinel.endpoint_id
        mock_req.environ = {'coriolis.context': mock_context}
        mock_get_paging_params.return_value = (None, None)
        mock_req.GET = {}

        result = self.endpoint_api.index(mock_req, endpoint_id)

        mock_get_paging_params.assert_called_once_with(mock_req)
        mock_decode_base64_param.assert_not_called()
        mock_get_endpoint_instances.assert_called_once_with(
            mock_context, endpoint_id,
            {}, None, None, None)
        mock_instances_collection.assert_called_once_with(
            mock_get_endpoint_instances.return_value)
        self.assertEqual(
            mock_instances_collection.return_value,
            result
        )

    @mock.patch.object(utils, 'decode_base64_param')
    @mock.patch.object(endpoint_resources_view, 'instance_single')
    @mock.patch.object(api.API, 'get_endpoint_instance')
    def test_show(
        self,
        mock_get_endpoint_instance,
        mock_instance_single,
        mock_decode_base64_param,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        endpoint_id = mock.sentinel.endpoint_id
        id = mock.sentinel.id
        mock_req.environ = {'coriolis.context': mock_context}
        env = mock.sentinel.env
        mock_req.GET = {
            'env': env,
        }
        mock_decode_base64_param.side_effect = [id, env]

        expected_calls = [
            mock.call.mock_decode_base64_param(id),
            mock.call.mock_decode_base64_param(env, is_json=True)]

        result = self.endpoint_api.show(mock_req, endpoint_id, id)

        mock_context.can.assert_called_once_with(
            'migration:endpoints:get_instance')
        mock_decode_base64_param.has_calls(expected_calls)
        mock_get_endpoint_instance.assert_called_once_with(
            mock_context, endpoint_id,
            env,
            id)
        mock_instance_single.assert_called_once_with(
            mock_get_endpoint_instance.return_value)
        self.assertEqual(
            mock_instance_single.return_value,
            result
        )

    @mock.patch.object(utils, 'decode_base64_param')
    @mock.patch.object(endpoint_resources_view, 'instance_single')
    @mock.patch.object(api.API, 'get_endpoint_instance')
    def test_show_no_env(
        self,
        mock_get_endpoint_instance,
        mock_instance_single,
        mock_decode_base64_param,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        endpoint_id = mock.sentinel.endpoint_id
        id = mock.sentinel.id
        mock_req.environ = {'coriolis.context': mock_context}
        mock_req.GET = {}

        result = self.endpoint_api.show(mock_req, endpoint_id, id)

        mock_decode_base64_param.assert_called_once_with(id)
        mock_get_endpoint_instance.assert_called_once_with(
            mock_context, endpoint_id, {},
            mock_decode_base64_param.return_value)
        mock_instance_single.assert_called_once_with(
            mock_get_endpoint_instance.return_value)
        self.assertEqual(
            mock_instance_single.return_value,
            result
        )
