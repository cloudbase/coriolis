# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from webob import exc

from coriolis.api.v1 import transfer_actions
from coriolis.api.v1.views import transfer_tasks_execution_view
from coriolis import exception
from coriolis.tests import test_base
from coriolis.tests import testutils
from coriolis.transfers import api


class TransferActionsControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Transfer Actions v1 API"""

    def setUp(self):
        super(TransferActionsControllerTestCase, self).setUp()
        self.transfer_actions = transfer_actions.TransferActionsController()

    @mock.patch.object(transfer_tasks_execution_view, 'single')
    @mock.patch.object(api.API, 'delete_disks')
    def test_delete_disks(
        self,
        mock_delete_disks,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        body = mock.sentinel.body

        result = testutils.get_wrapped_function(
            self.transfer_actions._delete_disks)(
                mock_req,
                id,
                body
        )

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:transfers:delete_disks")
        mock_delete_disks.assert_called_once_with(mock_context, id)
        mock_single.assert_called_once_with(mock_delete_disks.return_value)

    @mock.patch.object(transfer_tasks_execution_view, 'single')
    @mock.patch.object(api.API, 'delete_disks')
    def test_delete_disks_not_found(
        self,
        mock_delete_disks,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        body = mock.sentinel.body
        mock_delete_disks.side_effect = exception.NotFound()

        self.assertRaises(
            exc.HTTPNotFound,
            testutils.get_wrapped_function(
                self.transfer_actions._delete_disks),
            req=mock_req,
            id=id,
            body=body
        )

        mock_context.can.assert_called_once_with(
            "migration:transfers:delete_disks")
        mock_delete_disks.assert_called_once_with(mock_context, id)
        mock_single.assert_not_called()

    @mock.patch.object(transfer_tasks_execution_view, 'single')
    @mock.patch.object(api.API, 'delete_disks')
    def test_delete_disks_invalid_parameter_value(
        self,
        mock_delete_disks,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        body = mock.sentinel.body
        mock_delete_disks.side_effect = exception.InvalidParameterValue("err")

        self.assertRaises(
            exc.HTTPNotFound,
            testutils.get_wrapped_function(
                self.transfer_actions._delete_disks),
            req=mock_req,
            id=id,
            body=body
        )

        mock_context.can.assert_called_once_with(
            "migration:transfers:delete_disks")
        mock_delete_disks.assert_called_once_with(mock_context, id)
        mock_single.assert_not_called()
