# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.api.v1.views import migration_view
from coriolis.api.v1.views import replica_tasks_execution_view as view
from coriolis.api.v1.views import utils as view_utils
from coriolis.tests import test_base


class MigrationViewTestCase(test_base.CoriolisApiViewsTestCase):
    """Test suite for the Coriolis api v1 views."""

    @mock.patch.object(view, 'format_replica_tasks_execution')
    @mock.patch.object(view_utils, 'format_opt')
    def test_format_migration(
        self,
        mock_format_opt,
        mock_format_replica_tasks_execution
    ):
        mock_execution = {'tasks': 'mock_id1'}
        mock_format_opt.return_value = {
            "executions": [mock_execution],
            'tasks': 'mock_id2',
            'mock_key': 'mock_value'
        }
        mock_format_replica_tasks_execution.return_value = mock_execution

        expected_result = {
            'tasks': 'mock_id1',
            'mock_key': 'mock_value'
        }

        endpoint = mock.sentinel.endpoint
        keys = mock.sentinel.keys
        result = migration_view._format_migration(endpoint, keys)

        mock_format_replica_tasks_execution.assert_called_once_with(
            mock_execution, keys
        )
        mock_format_opt.assert_called_once_with(endpoint, keys)

        self.assertEqual(
            expected_result,
            result
        )

    @mock.patch.object(view_utils, 'format_opt')
    def test_format_migration_no_tasks(
        self,
        mock_format_opt,
    ):
        mock_format_opt.return_value = {
            'mock_key': 'mock_value'
        }

        endpoint = mock.sentinel.endpoint
        keys = mock.sentinel.keys
        result = migration_view._format_migration(endpoint, keys)

        mock_format_opt.assert_called_once_with(endpoint, keys)

        self.assertEqual(
            mock_format_opt.return_value,
            result
        )

    @mock.patch.object(view_utils, 'format_opt')
    def test_format_migration_migration_dict_has_tasks(
        self,
        mock_format_opt,
    ):
        mock_format_opt.return_value = {
            'tasks': 'mock_id1',
            'mock_key': 'mock_value'
        }

        endpoint = mock.sentinel.endpoint
        keys = mock.sentinel.keys
        result = migration_view._format_migration(endpoint, keys)

        mock_format_opt.assert_called_once_with(endpoint, keys)

        self.assertEqual(
            mock_format_opt.return_value,
            result
        )

    def test_single(self):
        fun = getattr(migration_view, 'single')
        self._single_view_test(fun, 'migration')

    def test_collection(self):
        fun = getattr(migration_view, 'collection')
        self._collection_view_test(fun, 'migrations')
