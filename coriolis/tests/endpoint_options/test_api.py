# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.endpoint_options import api
from coriolis.tests import test_base

ARGS = {
    "ctxt": "mock_ctxt",
    "endpoint_id": "mock_endpoint_id",
    "env": "mock_env",
    "option_names": "mock_option_names"
}


class EndpointOptionsAPITestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Endpoint Options API."""

    def setUp(self):
        super(EndpointOptionsAPITestCase, self).setUp()
        self.endpoint_options_api = api.API()
        self.endpoint_options_api._rpc_minion_manager_client = mock.Mock()
        self.endpoint_options_api._rpc_conductor_client = mock.Mock()

    def test_get_endpoint_source_options(self):
        result = self.endpoint_options_api.get_endpoint_source_options(**ARGS)
        (self.endpoint_options_api._rpc_conductor_client.
            get_endpoint_source_options.assert_called_once_with)(
            *(ARGS.values())
        )
        self.assertEqual(
            (self.endpoint_options_api._rpc_conductor_client.
                get_endpoint_source_options.return_value),
            result
        )

    def test_get_endpoint_destination_options(self):
        result = self.endpoint_options_api.get_endpoint_destination_options(
            **ARGS)
        (self.endpoint_options_api._rpc_conductor_client.
            get_endpoint_destination_options.assert_called_once_with)(
            *(ARGS.values())
        )
        self.assertEqual(
            (self.endpoint_options_api._rpc_conductor_client.
                get_endpoint_destination_options.return_value),
            result
        )

    def test_get_endpoint_source_minion_pool_options(self):
        result = (self.endpoint_options_api.
                  get_endpoint_source_minion_pool_options)(**ARGS)
        (self.endpoint_options_api._rpc_minion_manager_client.
            get_endpoint_source_minion_pool_options.assert_called_once_with)(
            *(ARGS.values())
        )
        self.assertEqual(
            (self.endpoint_options_api._rpc_minion_manager_client.
                get_endpoint_source_minion_pool_options.return_value),
            result
        )

    def test_get_endpoint_destination_minion_pool_options(self):
        result = (self.endpoint_options_api.
                  get_endpoint_destination_minion_pool_options)(**ARGS)
        (self.endpoint_options_api._rpc_minion_manager_client.
            get_endpoint_destination_minion_pool_options.
            assert_called_once_with)(
            *(ARGS.values())
        )
        self.assertEqual(
            (self.endpoint_options_api._rpc_minion_manager_client.
                get_endpoint_destination_minion_pool_options.return_value),
            result
        )
