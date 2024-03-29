# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.endpoint_resources import api
from coriolis.tests import test_base

ARGS = {
    "ctxt": "mock_ctxt",
    "endpoint_id": "mock_endpoint_id"
}


class EndpointResourcesAPITestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Endpoint Resources API."""

    def setUp(self):
        super(EndpointResourcesAPITestCase, self).setUp()
        self.endpoint_resources_api = api.API()
        self.endpoint_resources_api._rpc_client = mock.Mock()

    def test_get_endpoint_instances(self):
        result = self.endpoint_resources_api.get_endpoint_instances(
            **ARGS,
            source_environment=mock.sentinel.source_environment,
            marker=mock.sentinel.marker,
            limit=mock.sentinel.limit,
            instance_name_pattern=mock.sentinel.instance_name_pattern
        )
        (self.endpoint_resources_api._rpc_client.
            get_endpoint_instances.assert_called_once_with)(
            *(ARGS.values()),
            mock.sentinel.source_environment,
            mock.sentinel.marker,
            mock.sentinel.limit,
            mock.sentinel.instance_name_pattern
        )
        self.assertEqual(
            (self.endpoint_resources_api._rpc_client.
                get_endpoint_instances.return_value),
            result
        )

    def test_get_endpoint_instance(self):
        result = self.endpoint_resources_api.get_endpoint_instance(
            **ARGS,
            source_environment=mock.sentinel.source_environment,
            instance_name=mock.sentinel.instance_name
        )
        (self.endpoint_resources_api._rpc_client.
            get_endpoint_instance.assert_called_once_with)(
            *(ARGS.values()),
            mock.sentinel.source_environment,
            mock.sentinel.instance_name
        )
        self.assertEqual(
            (self.endpoint_resources_api._rpc_client.
                get_endpoint_instance.return_value),
            result
        )

    def test_get_endpoint_networks(self):
        result = self.endpoint_resources_api.get_endpoint_networks(
            **ARGS,
            env=mock.sentinel.env
        )
        (self.endpoint_resources_api._rpc_client.
            get_endpoint_networks.assert_called_once_with)(
            *(ARGS.values()),
            mock.sentinel.env
        )
        self.assertEqual(
            (self.endpoint_resources_api._rpc_client.
                get_endpoint_networks.return_value),
            result
        )

    def test_get_endpoint_storage(self):
        result = self.endpoint_resources_api.get_endpoint_storage(
            **ARGS,
            env=mock.sentinel.env
        )
        (self.endpoint_resources_api._rpc_client.
            get_endpoint_storage.assert_called_once_with)(
            *(ARGS.values()),
            mock.sentinel.env
        )
        self.assertEqual(
            (self.endpoint_resources_api._rpc_client.
                get_endpoint_storage.return_value),
            result
        )
