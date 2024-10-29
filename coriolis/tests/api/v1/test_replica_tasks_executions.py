# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from webob import exc

from coriolis.api.v1 import replica_tasks_executions as replica_api
from coriolis.api.v1.views import replica_tasks_execution_view
from coriolis import exception
from coriolis.replica_tasks_executions import api
from coriolis.tests import test_base


class ReplicaTasksExecutionControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Replica Tasks Execution v1 API"""

    def setUp(self):
        super(ReplicaTasksExecutionControllerTestCase, self).setUp()
        self.replica_api = replica_api.ReplicaTasksExecutionController()

    @mock.patch.object(replica_tasks_execution_view, 'single')
    @mock.patch.object(api.API, 'get_execution')
    def test_show(
        self,
        mock_get_execution,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        replica_id = mock.sentinel.transfer_id
        id = mock.sentinel.id

        result = self.replica_api.show(mock_req, replica_id, id)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_executions:show")
        mock_get_execution.assert_called_once_with(
            mock_context, replica_id, id)
        mock_single.assert_called_once_with(mock_get_execution.return_value)

    @mock.patch.object(replica_tasks_execution_view, 'single')
    @mock.patch.object(api.API, 'get_execution')
    def test_show_not_found(
        self,
        mock_get_execution,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        replica_id = mock.sentinel.transfer_id
        id = mock.sentinel.id
        mock_get_execution.return_value = None

        self.assertRaises(
            exc.HTTPNotFound,
            self.replica_api.show,
            mock_req,
            replica_id,
            id
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_executions:show")
        mock_get_execution.assert_called_once_with(
            mock_context, replica_id, id)
        mock_single.assert_not_called()

    @mock.patch.object(replica_tasks_execution_view, 'collection')
    @mock.patch.object(api.API, 'get_executions')
    def test_index(
        self,
        mock_get_executions,
        mock_collection
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        replica_id = mock.sentinel.transfer_id

        result = self.replica_api.index(mock_req, replica_id)

        self.assertEqual(
            mock_collection.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_executions:list")
        mock_get_executions.assert_called_once_with(
            mock_context, replica_id, include_tasks=False)
        mock_collection.assert_called_once_with(
            mock_get_executions.return_value)

    @mock.patch.object(replica_tasks_execution_view, 'collection')
    @mock.patch.object(api.API, 'get_executions')
    def test_detail(
        self,
        mock_get_executions,
        mock_collection
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        replica_id = mock.sentinel.transfer_id

        result = self.replica_api.detail(mock_req, replica_id)

        self.assertEqual(
            mock_collection.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_executions:show")
        mock_get_executions.assert_called_once_with(
            mock_context, replica_id, include_tasks=True)
        mock_collection.assert_called_once_with(
            mock_get_executions.return_value)

    @mock.patch.object(replica_tasks_execution_view, 'single')
    @mock.patch.object(api.API, 'create')
    def test_create(
        self,
        mock_create,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        replica_id = mock.sentinel.transfer_id
        execution = {"shutdown_instances": True}
        mock_body = {"execution": execution}

        result = self.replica_api.create(mock_req, replica_id, mock_body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_executions:create")
        mock_create.assert_called_once_with(
            mock_context, replica_id, True)
        mock_single.assert_called_once_with(mock_create.return_value)

    @mock.patch.object(replica_tasks_execution_view, 'single')
    @mock.patch.object(api.API, 'create')
    def test_create_no_executions(
        self,
        mock_create,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        replica_id = mock.sentinel.transfer_id
        mock_body = {}

        result = self.replica_api.create(mock_req, replica_id, mock_body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_executions:create")
        mock_create.assert_called_once_with(
            mock_context, replica_id, False)
        mock_single.assert_called_once_with(mock_create.return_value)

    @mock.patch.object(api.API, 'delete')
    def test_delete(
        self,
        mock_delete
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        replica_id = mock.sentinel.transfer_id
        id = mock.sentinel.id

        self.assertRaises(
            exc.HTTPNoContent,
            self.replica_api.delete,
            mock_req,
            replica_id,
            id
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_executions:delete")
        mock_delete.assert_called_once_with(mock_context, replica_id, id)

    @mock.patch.object(api.API, 'delete')
    def test_delete_not_found(
        self,
        mock_delete
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        replica_id = mock.sentinel.transfer_id
        id = mock.sentinel.id
        mock_delete.side_effect = exception.NotFound()

        self.assertRaises(
            exc.HTTPNotFound,
            self.replica_api.delete,
            mock_req,
            replica_id,
            id
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_executions:delete")
        mock_delete.assert_called_once_with(mock_context, replica_id, id)
