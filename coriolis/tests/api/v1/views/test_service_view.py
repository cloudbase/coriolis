# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.api.v1.views import service_view
from coriolis.api.v1.views import utils as view_utils
from coriolis.tests import test_base


class ServiceViewTestCase(test_base.CoriolisApiViewsTestCase):
    """Test suite for the Coriolis api v1 views."""

    def setUp(self):
        super(ServiceViewTestCase, self).setUp()
        self._format_fun = service_view._format_service

    @mock.patch.object(view_utils, 'format_opt')
    def test_format_service(self, mock_format_opt):
            mock_format_opt.return_value = {
                "mapped_regions": [{'id': 'mock_id1'}, {'id': 'mock_id2'}],
                "mock_key": "mock_value"
            }

            expected_result = {
                "mapped_regions": ['mock_id1', 'mock_id2'],
                'mock_key': 'mock_value'
            }

            service = mock.sentinel.service
            keys = mock.sentinel.keys
            result = service_view._format_service(service, keys)

            mock_format_opt.assert_called_once_with(service, keys)
            self.assertEqual(
                expected_result,
                result
            )

    @mock.patch.object(view_utils, 'format_opt')
    def test_format_service_no_keys(self, mock_format_opt):
            mock_format_opt.return_value = {
                "mapped_regions": [{'id': 'mock_id1'}, {'id': 'mock_id2'}],
            }

            expected_result = {
                "mapped_regions": ['mock_id1', 'mock_id2'],
            }

            service = mock.sentinel.service
            keys = mock.sentinel.keys
            result = service_view._format_service(service, keys)

            mock_format_opt.assert_called_once_with(service, keys)
            self.assertEqual(
                expected_result,
                result
            )

    @mock.patch.object(view_utils, 'format_opt')
    def test_format_service_no_mapped_regions(self, mock_format_opt):
            mock_format_opt.return_value = {
                "mock_key": "mock_value"
            }

            expected_result = {
                'mapped_regions': [],
                'mock_key': 'mock_value'
            }

            service = mock.sentinel.service
            keys = mock.sentinel.keys
            result = service_view._format_service(service, keys)

            mock_format_opt.assert_called_once_with(service, keys)
            self.assertEqual(
                expected_result,
                result
            )

    def test_single(self):
        fun = getattr(service_view, 'single')
        self._single_view_test(fun, 'service')

    def test_collection(self):
        fun = getattr(service_view, 'collection')
        self._collection_view_test(fun, 'services')
