# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.api.v1.views import replica_tasks_execution_view as view
from coriolis.api.v1.views import replica_view
from coriolis.api.v1.views import utils as view_utils
from coriolis.tests import test_base


class ReplicaViewTestCase(test_base.CoriolisApiViewsTestCase):
    """Test suite for the Coriolis api v1 views."""

    def setUp(self):
        super(ReplicaViewTestCase, self).setUp()
        self._format_fun = replica_view._format_replica

    @mock.patch.object(view, 'format_replica_tasks_execution')
    @mock.patch.object(view_utils, 'format_opt')
    def test_format_replica(self, mock_format_opt,
                            mock_format_replica_tasks_execution):
            mock_format_opt.return_value = {
                "executions": [{'id': 'mock_id1'}, {'id': 'mock_id2'}],
                "mock_key": "mock_value"
            }

            expected_calls = [
                mock.call.mock_format_replica_tasks_execution(
                    {'id': 'mock_id1'}),
                mock.call.mock_format_replica_tasks_execution(
                    {'id': 'mock_id2'})]
            expected_result = {
                "executions":
                    [mock_format_replica_tasks_execution.return_value,
                     mock_format_replica_tasks_execution.return_value],
                'mock_key': 'mock_value'
            }

            replica = mock.sentinel.replica
            keys = mock.sentinel.keys
            result = replica_view._format_replica(replica, keys)

            mock_format_opt.assert_called_once_with(replica, keys)
            mock_format_replica_tasks_execution.assert_has_calls(
                expected_calls
            )
            self.assertEqual(
                expected_result,
                result
            )

    @mock.patch.object(view, 'format_replica_tasks_execution')
    @mock.patch.object(view_utils, 'format_opt')
    def test_format_replica_no_keys(self, mock_format_opt,
                                    mock_format_replica_tasks_execution):
            mock_format_opt.return_value = {
                "executions": [{'id': 'mock_id1'}, {'id': 'mock_id2'}],
            }

            expected_calls = [
                mock.call.mock_format_replica_tasks_execution(
                    {'id': 'mock_id1'}),
                mock.call.mock_format_replica_tasks_execution(
                    {'id': 'mock_id2'})]
            expected_result = {
                "executions":
                    [mock_format_replica_tasks_execution.return_value,
                     mock_format_replica_tasks_execution.return_value],
            }

            replica = mock.sentinel.replica
            keys = mock.sentinel.keys
            result = replica_view._format_replica(replica, keys)

            mock_format_opt.assert_called_once_with(replica, keys)
            mock_format_replica_tasks_execution.assert_has_calls(
                expected_calls
            )
            self.assertEqual(
                expected_result,
                result
            )

    @mock.patch.object(view_utils, 'format_opt')
    def test_format_replica_no_executions(self, mock_format_opt):
            mock_format_opt.return_value = {
                "mock_key": "mock_value"
            }

            expected_result = {
                'executions': [],
                'mock_key': 'mock_value'
            }

            replica = mock.sentinel.replica
            keys = mock.sentinel.keys
            result = replica_view._format_replica(replica, keys)

            mock_format_opt.assert_called_once_with(replica, keys)
            self.assertEqual(
                expected_result,
                result
            )

    def test_single(self):
        fun = getattr(replica_view, 'single')
        self._single_view_test(fun, 'replica')

    def test_collection(self):
        fun = getattr(replica_view, 'collection')
        self._collection_view_test(fun, 'replicas')
