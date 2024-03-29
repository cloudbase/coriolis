# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.endpoints import api
from coriolis.tests import test_base
from coriolis.tests import testutils


class EndpointsAPITestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Endpoints API."""

    def setUp(self):
        super(EndpointsAPITestCase, self).setUp()
        self.endpoints_api = api.API()
        self.endpoints_api._rpc_conductor_client = mock.Mock()
        self.endpoints_api._rpc_minion_manager_client = mock.Mock()

    def test_create(self):
        args = {
            "ctxt": mock.sentinel.ctxt,
            "name": mock.sentinel.name,
            "endpoint_type": mock.sentinel.endpoint_type,
            "description": mock.sentinel.description,
            "connection_info": mock.sentinel.connection_info,
            "mapped_regions": mock.sentinel.mapped_regions
        }
        result = self.endpoints_api.create(
            **args
        )
        (self.endpoints_api._rpc_conductor_client.
            create_endpoint.assert_called_once_with)(
            *(args.values())
        )
        self.assertEqual(
            (self.endpoints_api._rpc_conductor_client.
                create_endpoint.return_value),
            result
        )

    def test_update(self):
        args = {
            "ctxt": mock.sentinel.ctxt,
            "endpoint_id": mock.sentinel.endpoint_id,
            "properties": mock.sentinel.properties,
        }
        result = self.endpoints_api.update(
            **args
        )
        (self.endpoints_api._rpc_conductor_client.
            update_endpoint.assert_called_once_with)(
            *(args.values())
        )
        self.assertEqual(
            (self.endpoints_api._rpc_conductor_client.
                update_endpoint.return_value),
            result
        )

    def test_delete(self):
        args = {
            "ctxt": mock.sentinel.ctxt,
            "endpoint_id": mock.sentinel.endpoint_id
        }
        self.endpoints_api.delete(
            **args
        )
        (self.endpoints_api._rpc_conductor_client.
            delete_endpoint.assert_called_once_with)(
            *(args.values())
        )

    def test_get_endpoints(self):
        args = {
            "ctxt": mock.sentinel.ctxt
        }
        result = self.endpoints_api.get_endpoints(
            **args
        )
        (self.endpoints_api._rpc_conductor_client.
            get_endpoints.assert_called_once_with)(
            *(args.values())
        )
        self.assertEqual(
            (self.endpoints_api._rpc_conductor_client.
                get_endpoints.return_value),
            result
        )

    def test_get_endpoint(self):
        args = {
            "ctxt": mock.sentinel.ctxt,
            "endpoint_id": mock.sentinel.endpoint_id
        }
        result = self.endpoints_api.get_endpoint(
            **args
        )
        (self.endpoints_api._rpc_conductor_client.
            get_endpoint.assert_called_once_with)(
            *(args.values())
        )
        self.assertEqual(
            (self.endpoints_api._rpc_conductor_client.
                get_endpoint.return_value),
            result
        )

    def test_validate_connection(self):
        args = {
            "ctxt": mock.sentinel.ctxt,
            "endpoint_id": mock.sentinel.endpoint_id
        }
        result = self.endpoints_api.validate_connection(
            **args
        )
        (self.endpoints_api._rpc_conductor_client.
            validate_endpoint_connection.assert_called_once_with)(
            *(args.values())
        )
        self.assertEqual(
            (self.endpoints_api._rpc_conductor_client.
                validate_endpoint_connection.return_value),
            result
        )

    def test_validate_target_environment(self):
        args = {
            "ctxt": mock.sentinel.ctxt,
            "endpoint_id": mock.sentinel.endpoint_id,
            "target_env": mock.sentinel.target_env
        }
        result = testutils.get_wrapped_function(
            self.endpoints_api.validate_target_environment)(
            self.endpoints_api,
            **args
        )
        (self.endpoints_api._rpc_conductor_client.
            validate_endpoint_target_environment.assert_called_once_with)(
            *(args.values())
        )
        self.assertEqual(
            (self.endpoints_api._rpc_conductor_client.
                validate_endpoint_target_environment.return_value),
            result
        )

    def test_validate_source_environment(self):
        args = {
            "ctxt": mock.sentinel.ctxt,
            "endpoint_id": mock.sentinel.endpoint_id,
            "source_env": mock.sentinel.source_env
        }
        result = testutils.get_wrapped_function(
            self.endpoints_api.validate_source_environment)(
            self.endpoints_api,
            **args
        )
        (self.endpoints_api._rpc_conductor_client.
            validate_endpoint_source_environment.assert_called_once_with)(
            *(args.values())
        )
        self.assertEqual(
            (self.endpoints_api._rpc_conductor_client.
                validate_endpoint_source_environment.return_value),
            result
        )

    def test_validate_endpoint_source_minion_pool_options(self):
        args = {
            "ctxt": mock.sentinel.ctxt,
            "endpoint_id": mock.sentinel.endpoint_id,
            "pool_environment": mock.sentinel.pool_environment
        }
        result = testutils.get_wrapped_function(
            self.endpoints_api.validate_endpoint_source_minion_pool_options)(
            self.endpoints_api,
            **args
        )
        (self.endpoints_api._rpc_minion_manager_client.
            validate_endpoint_source_minion_pool_options.
            assert_called_once_with)(
            *(args.values())
        )
        self.assertEqual(
            (self.endpoints_api._rpc_minion_manager_client.
                validate_endpoint_source_minion_pool_options.return_value),
            result
        )

    def test_validate_endpoint_destination_minion_pool_options(self):
        args = {
            "ctxt": mock.sentinel.ctxt,
            "endpoint_id": mock.sentinel.endpoint_id,
            "pool_environment": mock.sentinel.pool_environment
        }
        result = testutils.get_wrapped_function(
            self.endpoints_api.
            validate_endpoint_destination_minion_pool_options)(
            self.endpoints_api,
            **args
        )
        (self.endpoints_api._rpc_minion_manager_client.
            validate_endpoint_destination_minion_pool_options.
            assert_called_once_with)(
            *(args.values())
        )
        self.assertEqual(
            (self.endpoints_api._rpc_minion_manager_client.
                validate_endpoint_destination_minion_pool_options.
                return_value),
            result
        )
