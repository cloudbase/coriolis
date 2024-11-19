# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.api.v1.views import transfer_tasks_execution_view as view
from coriolis.api.v1.views import utils as view_utils
from coriolis import constants
from coriolis.tests import test_base


class TransferTaskExecutionViewTestCase(test_base.CoriolisApiViewsTestCase):
    """Test suite for the Coriolis api v1 views."""

    @mock.patch.object(view, '_sort_tasks')
    @mock.patch.object(view_utils, 'format_opt')
    def test_format_transfer_tasks_execution(
        self,
        mock_format_opt,
        mock_sort_tasks
    ):
        mock_tasks = ['mock_task1', 'mock_task2']
        mock_execution = {
            'tasks': mock_tasks,
            'mock_key': 'mock_value'
        }
        mock_sort_tasks.return_value = mock_execution

        keys = mock.sentinel.keys
        result = view.format_transfer_tasks_execution(mock_execution, keys)

        mock_sort_tasks.assert_called_once_with(mock_tasks)
        mock_format_opt.assert_called_once_with(mock_execution["tasks"], keys)
        self.assertEqual(
            mock_format_opt.return_value,
            result
        )

    @mock.patch.object(view, '_sort_tasks')
    @mock.patch.object(view_utils, 'format_opt')
    def test_format_transfer_tasks_execution_no_tasks(
        self,
        mock_format_opt,
        mock_sort_tasks
    ):
        mock_execution = {
            'mock_key': 'mock_value'
        }

        keys = mock.sentinel.keys
        result = view.format_transfer_tasks_execution(mock_execution, keys)

        mock_sort_tasks.assert_not_called()
        mock_format_opt.assert_called_once_with(mock_execution, keys)
        self.assertEqual(
            mock_format_opt.return_value,
            result
        )

    def test_sort_tasks(self):
        mock_tasks = [
            {'index': 2, 'status': 'mock_status1'},
            {'status': 'mock_status3'},
            {'index': 3, 'status': constants.TASK_STATUS_ON_ERROR_ONLY},
            {'index': 1, 'status': 'mock_status2'},
        ]
        expected_result = [
            {'status': 'mock_status3'},
            {'index': 1, 'status': 'mock_status2'},
            {'index': 2, 'status': 'mock_status1'},
        ]

        result = view._sort_tasks(mock_tasks)

        self.assertEqual(
            expected_result,
            result
        )

    def test_sort_tasks_no_filter(self):
        mock_tasks = [
            {'index': 2, 'status': 'mock_status1'},
            {'status': 'mock_status3'},
            {'index': 3, 'status': constants.TASK_STATUS_ON_ERROR_ONLY},
            {'index': 1, 'status': 'mock_status2'},
        ]
        expected_result = [
            {'status': 'mock_status3'},
            {'index': 1, 'status': 'mock_status2'},
            {'index': 2, 'status': 'mock_status1'},
            {'index': 3, 'status': constants.TASK_STATUS_ON_ERROR_ONLY},
        ]

        result = view._sort_tasks(mock_tasks, False)

        self.assertEqual(
            expected_result,
            result
        )

    def test_sort_tasks_no_tasks(self):
        expected_result = []

        result = view._sort_tasks(expected_result)

        self.assertEqual(
            expected_result,
            result
        )

    def test_single(self):
        fun = getattr(view, 'single')
        self._single_view_test(fun, 'execution')

    def test_collection(self):
        fun = getattr(view, 'collection')
        self._collection_view_test(fun, 'executions')
