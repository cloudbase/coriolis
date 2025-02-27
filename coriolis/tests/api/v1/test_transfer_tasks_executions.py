# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from webob import exc

from coriolis.api.v1 import transfer_tasks_executions as transfer_api
from coriolis.api.v1.views import transfer_tasks_execution_view
from coriolis import exception
from coriolis.tests import test_base
from coriolis.transfer_tasks_executions import api


class TransferTasksExecutionControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Transfer Tasks Execution v1 API"""

    def setUp(self):
        super(TransferTasksExecutionControllerTestCase, self).setUp()
        self.transfer_api = transfer_api.TransferTasksExecutionController()

    @mock.patch.object(transfer_tasks_execution_view, 'single')
    @mock.patch.object(api.API, 'get_execution')
    def test_show(
        self,
        mock_get_execution,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        transfer_id = mock.sentinel.transfer_id
        id = mock.sentinel.id

        result = self.transfer_api.show(mock_req, transfer_id, id)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:transfer_executions:show")
        mock_get_execution.assert_called_once_with(
            mock_context, transfer_id, id)
        mock_single.assert_called_once_with(mock_get_execution.return_value)

    @mock.patch.object(transfer_tasks_execution_view, 'single')
    @mock.patch.object(api.API, 'get_execution')
    def test_show_not_found(
        self,
        mock_get_execution,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        transfer_id = mock.sentinel.transfer_id
        id = mock.sentinel.id
        mock_get_execution.return_value = None

        self.assertRaises(
            exc.HTTPNotFound,
            self.transfer_api.show,
            mock_req,
            transfer_id,
            id
        )

        mock_context.can.assert_called_once_with(
            "migration:transfer_executions:show")
        mock_get_execution.assert_called_once_with(
            mock_context, transfer_id, id)
        mock_single.assert_not_called()

    @mock.patch.object(transfer_tasks_execution_view, 'collection')
    @mock.patch.object(api.API, 'get_executions')
    def test_index(
        self,
        mock_get_executions,
        mock_collection
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        transfer_id = mock.sentinel.transfer_id

        result = self.transfer_api.index(mock_req, transfer_id)

        self.assertEqual(
            mock_collection.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:transfer_executions:list")
        mock_get_executions.assert_called_once_with(
            mock_context, transfer_id, include_tasks=False)
        mock_collection.assert_called_once_with(
            mock_get_executions.return_value)

    @mock.patch.object(transfer_tasks_execution_view, 'collection')
    @mock.patch.object(api.API, 'get_executions')
    def test_detail(
        self,
        mock_get_executions,
        mock_collection
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        transfer_id = mock.sentinel.transfer_id

        result = self.transfer_api.detail(mock_req, transfer_id)

        self.assertEqual(
            mock_collection.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:transfer_executions:show")
        mock_get_executions.assert_called_once_with(
            mock_context, transfer_id, include_tasks=True)
        mock_collection.assert_called_once_with(
            mock_get_executions.return_value)

    @mock.patch.object(transfer_tasks_execution_view, 'single')
    @mock.patch.object(api.API, 'create')
    def test_create(
        self,
        mock_create,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        transfer_id = mock.sentinel.transfer_id
        execution = {"shutdown_instances": True, "auto_deploy": True}
        mock_body = {"execution": execution}

        result = self.transfer_api.create(mock_req, transfer_id, mock_body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:transfer_executions:create")
        mock_create.assert_called_once_with(
            mock_context, transfer_id, True, True)
        mock_single.assert_called_once_with(mock_create.return_value)

    @mock.patch.object(transfer_tasks_execution_view, 'single')
    @mock.patch.object(api.API, 'create')
    def test_create_no_executions(
        self,
        mock_create,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        transfer_id = mock.sentinel.transfer_id
        mock_body = {}

        result = self.transfer_api.create(mock_req, transfer_id, mock_body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:transfer_executions:create")
        mock_create.assert_called_once_with(
            mock_context, transfer_id, False, False)
        mock_single.assert_called_once_with(mock_create.return_value)

    @mock.patch.object(api.API, 'delete')
    def test_delete(
        self,
        mock_delete
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        transfer_id = mock.sentinel.transfer_id
        id = mock.sentinel.id

        self.assertRaises(
            exc.HTTPNoContent,
            self.transfer_api.delete,
            mock_req,
            transfer_id,
            id
        )

        mock_context.can.assert_called_once_with(
            "migration:transfer_executions:delete")
        mock_delete.assert_called_once_with(mock_context, transfer_id, id)

    @mock.patch.object(api.API, 'delete')
    def test_delete_not_found(
        self,
        mock_delete
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        transfer_id = mock.sentinel.transfer_id
        id = mock.sentinel.id
        mock_delete.side_effect = exception.NotFound()

        self.assertRaises(
            exc.HTTPNotFound,
            self.transfer_api.delete,
            mock_req,
            transfer_id,
            id
        )

        mock_context.can.assert_called_once_with(
            "migration:transfer_executions:delete")
        mock_delete.assert_called_once_with(mock_context, transfer_id, id)
