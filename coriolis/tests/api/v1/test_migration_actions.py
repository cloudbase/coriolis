# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from webob import exc

from coriolis.api.v1 import migration_actions
from coriolis import exception
from coriolis.migrations import api
from coriolis.tests import test_base
from coriolis.tests import testutils


class MigrationActionsControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Migration Actions v1 API"""

    def setUp(self):
        super(MigrationActionsControllerTestCase, self).setUp()
        self.migration_actions = migration_actions.MigrationActionsController()

    @mock.patch.object(api.API, 'cancel')
    def test__cancel(
        self,
        mock_cancel,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_body = {
            'cancel': {
                'force': False
            }
        }

        self.assertRaises(
            exc.HTTPNoContent,
            testutils.get_wrapped_function(self.migration_actions._cancel),
            mock_req,
            id,
            mock_body
        )

        mock_context.can.assert_called_once_with("migration:migrations:cancel")
        mock_cancel.assert_called_once_with(mock_context, id, False)

    @mock.patch.object(api.API, 'cancel')
    def test__cancel_empty(
        self,
        mock_cancel,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_body = {'cancel': {}}

        self.assertRaises(
            exc.HTTPNoContent,
            testutils.get_wrapped_function(self.migration_actions._cancel),
            mock_req,
            id,
            mock_body
        )

        mock_context.can.assert_called_once_with("migration:migrations:cancel")
        mock_cancel.assert_called_once_with(mock_context, id, False)

    @mock.patch.object(api.API, 'cancel')
    def test__cancel_not_found(
        self,
        mock_cancel,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_body = {'cancel': {}}
        mock_cancel.side_effect = exception.NotFound()

        self.assertRaises(
            exc.HTTPNotFound,
            testutils.get_wrapped_function(self.migration_actions._cancel),
            mock_req,
            id,
            mock_body
        )

        mock_context.can.assert_called_once_with("migration:migrations:cancel")
        mock_cancel.assert_called_once_with(mock_context, id, False)

    @mock.patch.object(api.API, 'cancel')
    def test__cancel_invalid_parameter_value(
        self,
        mock_cancel,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_body = {'cancel': {}}
        mock_cancel.side_effect = exception.InvalidParameterValue("err")

        self.assertRaises(
            exc.HTTPNotFound,
            testutils.get_wrapped_function(self.migration_actions._cancel),
            mock_req,
            id,
            mock_body
        )

        mock_context.can.assert_called_once_with("migration:migrations:cancel")
        mock_cancel.assert_called_once_with(mock_context, id, False)
