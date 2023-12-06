# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from webob import exc

from coriolis.api.v1 import endpoint_actions
from coriolis.endpoints import api
from coriolis import exception
from coriolis.tests import test_base
from coriolis.tests import testutils


class EndpointActionsControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Endpoint Actions v1 API"""

    def setUp(self):
        super(EndpointActionsControllerTestCase, self).setUp()
        self.endpoint_api = endpoint_actions.EndpointActionsController()

    @mock.patch.object(api.API, 'validate_connection')
    def test_validate_connection(
        self,
        mock_validate_connection
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        body = mock.sentinel.body
        is_valid = True
        message = 'mock_message'
        mock_validate_connection.return_value = (is_valid, message)

        expected_result = {
            "validate-connection":
                {"valid": is_valid, "message": message}
        }
        result = testutils.get_wrapped_function(
            self.endpoint_api._validate_connection)(
                mock_req,
                id,
                body  # type: ignore
        )

        mock_context.can.assert_called_once_with(
            'migration:endpoints:validate_connection')
        mock_validate_connection.assert_called_once_with(mock_context, id)
        self.assertEqual(
            expected_result,
            result
        )

    @mock.patch.object(api.API, 'validate_connection')
    def test_validate_connection_except_not_found(
        self,
        mock_validate_connection
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        body = mock.sentinel.body
        mock_validate_connection.side_effect = exception.NotFound

        self.assertRaises(
            exc.HTTPNotFound,
            testutils.get_wrapped_function(
                self.endpoint_api._validate_connection),
            mock_req,
            id,
            body
        )
        mock_validate_connection.assert_called_once_with(mock_context, id)

    @mock.patch.object(api.API, 'validate_connection')
    def test_validate_connection_except_invalid_parameter_value(
        self,
        mock_validate_connection
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        body = mock.sentinel.body
        mock_validate_connection.side_effect = exception.InvalidParameterValue(
            "mock_err"
        )

        self.assertRaises(
            exc.HTTPNotFound,
            testutils.get_wrapped_function(
                self.endpoint_api._validate_connection),
            mock_req,
            id,
            body
        )
        mock_validate_connection.assert_called_once_with(mock_context, id)
