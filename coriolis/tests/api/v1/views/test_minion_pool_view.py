# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.api.v1.views import minion_pool_view as view
from coriolis.api.v1.views import utils as view_utils
from coriolis.tests import test_base


class MinionPoolViewTestCase(test_base.CoriolisApiViewsTestCase):
    """Test suite for the Coriolis api v1 views."""

    @mock.patch.object(view_utils, 'format_opt')
    def test_format_minion_pool(
        self,
        mock_format_opt,
    ):
        mock_minion_pool_dict = {
            'minion_machines': [{
                'connection_info': {
                    'pkey': 'mock_key',
                    'password': 'mock_key',
                    'certificates': {'key': 'mock_key'}
                },
                'backup_writer_connection_info': {
                    'connection_details': {
                        'pkey': 'mock_key',
                        'password': 'mock_key',
                        'certificates': {'key': 'mock_key'}
                    },
                }
            }],
        }
        expected_result = {
            'minion_machines': [{
                'connection_info': {
                    'pkey': '***',
                    'password': '***',
                    'certificates': {'key': '***'}
                },
                'backup_writer_connection_info': {
                    'connection_details': {
                        'pkey': '***',
                        'password': '***',
                        'certificates': {'key': '***'}
                    },
                }
            }],
        }
        mock_format_opt.return_value = mock_minion_pool_dict

        endpoint = mock.sentinel.endpoint
        keys = mock.sentinel.keys
        result = view._format_minion_pool(endpoint, keys)

        mock_format_opt.assert_called_once_with(endpoint, keys)
        self.assertEqual(
            expected_result,
            result
        )

    @mock.patch.object(view_utils, 'format_opt')
    def test_format_minion_pool_connection_none(
        self,
        mock_format_opt,
    ):
        mock_minion_pool_dict = {
            'minion_machines': [{
                'connection_info': None,
                'backup_writer_connection_info': {
                    'connection_details': None,
                }
            }],
        }
        expected_result = {
            'minion_machines': [{
                'connection_info': None,
                'backup_writer_connection_info': {
                    'connection_details': None,
                }
            }],
        }
        mock_format_opt.return_value = mock_minion_pool_dict

        endpoint = mock.sentinel.endpoint
        keys = mock.sentinel.keys
        result = view._format_minion_pool(endpoint, keys)

        mock_format_opt.assert_called_once_with(endpoint, keys)
        self.assertEqual(
            expected_result,
            result
        )

    @mock.patch.object(view_utils, 'format_opt')
    def test_format_minion_pool_no_minion_machines(
        self,
        mock_format_opt,
    ):
        expected_result = {}
        mock_format_opt.return_value = {}

        endpoint = mock.sentinel.endpoint
        keys = mock.sentinel.keys
        result = view._format_minion_pool(endpoint, keys)

        mock_format_opt.assert_called_once_with(endpoint, keys)
        self.assertEqual(
            expected_result,
            result
        )

    def test_single(self):
        fun = getattr(view, 'single')
        self._single_view_test(fun, 'minion_pool')

    def test_collection(self):
        fun = getattr(view, 'collection')
        self._collection_view_test(fun, 'minion_pools')
