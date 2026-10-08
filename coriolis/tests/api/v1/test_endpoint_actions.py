# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from webob import exc

from coriolis import exception
from coriolis.api.v1 import endpoint_actions
from coriolis.endpoints import api
from coriolis.tests import test_base, testutils


class EndpointActionsControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Endpoint Actions v1 API"""

    def setUp(self):
        super(EndpointActionsControllerTestCase, self).setUp()
        self.endpoint_api = endpoint_actions.EndpointActionsController()

    def test__validate_connection_body(self):
        body = {
            "validate-connection": {
                "platform": "mock_platform",
                "connection_info": "mock_connection_info",
                "mapped_regions": ["mock_region"],
            }
        }

        result = testutils.get_wrapped_function(
            self.endpoint_api._validate_connection_body
        )(
            self.endpoint_api,
            body,  # type: ignore
        )

        self.assertEqual(
            ("mock_platform", "mock_connection_info", ["mock_region"]), result
        )

    def test__validate_connection_body_no_mapped_regions(self):
        body = {
            "validate-connection": {
                "platform": "mock_platform",
                "connection_info": "mock_connection_info",
            }
        }

        result = testutils.get_wrapped_function(
            self.endpoint_api._validate_connection_body
        )(
            self.endpoint_api,
            body,  # type: ignore
        )

        self.assertEqual(("mock_platform", "mock_connection_info", []), result)

    def test__validate_connection_body_not_dict(self):
        body = {"validate-connection": None}

        self.assertRaises(
            exception.InvalidInput,
            testutils.get_wrapped_function(self.endpoint_api._validate_connection_body),
            self.endpoint_api,
            body,
        )

    def test__validate_connection_body_missing_platform(self):
        body = {"validate-connection": {"connection_info": "mock_connection_info"}}

        self.assertRaises(
            KeyError,
            testutils.get_wrapped_function(self.endpoint_api._validate_connection_body),
            self.endpoint_api,
            body,
        )

    def test__validate_connection_body_missing_connection_info(self):
        body = {"validate-connection": {"platform": "mock_platform"}}

        self.assertRaises(
            KeyError,
            testutils.get_wrapped_function(self.endpoint_api._validate_connection_body),
            self.endpoint_api,
            body,
        )

    @mock.patch.object(api.API, 'validate_connection')
    @mock.patch.object(
        endpoint_actions.EndpointActionsController, '_validate_connection_body'
    )
    def test_validate_endpoint(
        self, mock__validate_connection_body, mock_validate_connection
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        body = mock.sentinel.body
        mock__validate_connection_body.return_value = (
            mock.sentinel.platform,
            mock.sentinel.connection_info,
            mock.sentinel.mapped_regions,
        )
        is_valid = True
        message = 'mock_message'
        mock_validate_connection.return_value = (is_valid, message)

        expected_result = {
            "validate-connection": {"valid": is_valid, "message": message}
        }
        result = testutils.get_wrapped_function(self.endpoint_api._validate_endpoint)(
            mock_req,
            body,  # type: ignore
        )

        mock_context.can.assert_called_once_with(
            'migration:endpoints:validate_connection'
        )
        mock__validate_connection_body.assert_called_once_with(body)
        mock_validate_connection.assert_called_once_with(
            mock_context,
            mock.sentinel.platform,
            mock.sentinel.connection_info,
            mock.sentinel.mapped_regions,
        )
        self.assertEqual(expected_result, result)

    @mock.patch.object(api.API, 'validate_connection')
    @mock.patch.object(
        endpoint_actions.EndpointActionsController, '_validate_connection_body'
    )
    def test_validate_endpoint_except_not_found(
        self, mock__validate_connection_body, mock_validate_connection
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        body = mock.sentinel.body
        mock__validate_connection_body.return_value = (
            mock.sentinel.platform,
            mock.sentinel.connection_info,
            mock.sentinel.mapped_regions,
        )
        mock_validate_connection.side_effect = exception.NotFound

        self.assertRaises(
            exc.HTTPNotFound,
            testutils.get_wrapped_function(self.endpoint_api._validate_endpoint),
            mock_req,
            body,
        )
        mock_validate_connection.assert_called_once_with(
            mock_context,
            mock.sentinel.platform,
            mock.sentinel.connection_info,
            mock.sentinel.mapped_regions,
        )

    @mock.patch.object(api.API, 'validate_connection')
    @mock.patch.object(
        endpoint_actions.EndpointActionsController, '_validate_connection_body'
    )
    def test_validate_endpoint_except_invalid_parameter_value(
        self, mock__validate_connection_body, mock_validate_connection
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        body = mock.sentinel.body
        mock__validate_connection_body.return_value = (
            mock.sentinel.platform,
            mock.sentinel.connection_info,
            mock.sentinel.mapped_regions,
        )
        mock_validate_connection.side_effect = exception.InvalidParameterValue(
            "mock_err"
        )

        self.assertRaises(
            exc.HTTPNotFound,
            testutils.get_wrapped_function(self.endpoint_api._validate_endpoint),
            mock_req,
            body,
        )
        mock_validate_connection.assert_called_once_with(
            mock_context,
            mock.sentinel.platform,
            mock.sentinel.connection_info,
            mock.sentinel.mapped_regions,
        )
