# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.api.v1.views import region_view
from coriolis.api.v1.views import utils as view_utils
from coriolis.tests import test_base


class RegionViewTestCase(test_base.CoriolisApiViewsTestCase):
    """Test suite for the Coriolis api v1 views."""

    def setUp(self):
        super(RegionViewTestCase, self).setUp()
        self._format_fun = region_view._format_region

    @mock.patch.object(view_utils, 'format_opt')
    def test_format_region(self, mock_format_opt):
            mock_format_opt.return_value = {
                "mapped_endpoints": [{'id': 'endpoint_1'},
                                     {'id': 'endpoint_2'}],
                "mapped_services": [{'id': 'service_1'},
                                    {'id': 'service_2'}],
                "mock_key": "mock_value"
            }

            expected_result = {
                'mapped_endpoints': ['endpoint_1', 'endpoint_2'],
                "mapped_services": ['service_1', 'service_2'],
                'mock_key': 'mock_value'
            }

            region = mock.sentinel.region
            keys = mock.sentinel.keys
            result = region_view._format_region(region, keys)

            mock_format_opt.assert_called_once_with(region, keys)
            self.assertEqual(
                expected_result,
                result
            )

    @mock.patch.object(view_utils, 'format_opt')
    def test_format_region_no_keys(self, mock_format_opt):
            mock_format_opt.return_value = {
                'mapped_endpoints': [{'id': 'endpoint_1'},
                                     {'id': 'endpoint_2'}],
                'mapped_services': [{'id': 'service_1'},
                                    {'id': 'service_2'}],
            }

            expected_result = {
                'mapped_endpoints': ['endpoint_1', 'endpoint_2'],
                'mapped_services': ['service_1', 'service_2'],
            }

            region = mock.sentinel.region
            keys = mock.sentinel.keys
            result = region_view._format_region(region, keys)

            mock_format_opt.assert_called_once_with(region, keys)
            self.assertEqual(
                expected_result,
                result
            )

    @mock.patch.object(view_utils, 'format_opt')
    def test_format_region_no_mapped_regions(self, mock_format_opt):
            mock_format_opt.return_value = {
                "mock_key": "mock_value"
            }

            expected_result = {
                'mapped_endpoints': [],
                'mapped_services': [],
                'mock_key': 'mock_value'
            }

            region = mock.sentinel.region
            keys = mock.sentinel.keys
            result = region_view._format_region(region, keys)

            mock_format_opt.assert_called_once_with(region, keys)
            self.assertEqual(
                expected_result,
                result
            )

    def test_single(self):
        fun = getattr(region_view, 'single')
        self._single_view_test(fun, 'region')

    def test_collection(self):
        fun = getattr(region_view, 'collection')
        self._collection_view_test(fun, 'regions')
