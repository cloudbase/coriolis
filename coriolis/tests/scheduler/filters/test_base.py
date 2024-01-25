# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.scheduler.filters import base
from coriolis.tests import test_base


class BaseServiceFilterTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseServiceFilter class."""

    @mock.patch.object(base.BaseServiceFilter, '__abstractmethods__', set())
    def setUp(self):
        super(BaseServiceFilterTestCase, self).setUp()
        self.service_filter = base.BaseServiceFilter()

    def test_is_service_acceptable(self):
        self.service_filter.rate_service = mock.Mock()
        self.service_filter.rate_service.return_value = 50

        result = self.service_filter.is_service_acceptable(
            mock.sentinel.service)

        self.service_filter.rate_service.assert_called_once_with(
            mock.sentinel.service
        )
        self.assertTrue(result)

    def test_is_service_acceptable_false(self):
        self.service_filter.rate_service = mock.Mock()
        self.service_filter.rate_service.return_value = 0

        result = self.service_filter.is_service_acceptable(
            mock.sentinel.service)

        self.service_filter.rate_service.assert_called_once_with(
            mock.sentinel.service
        )
        self.assertFalse(result)

    def test_filter_services(self):
        self.service_filter.is_service_acceptable = mock.Mock()
        self.service_filter.is_service_acceptable.side_effect = [
            True, False, True]

        result = self.service_filter.filter_services([mock.sentinel.service1,
                                                      mock.sentinel.service2,
                                                      mock.sentinel.service3])

        self.assertEqual(result, [mock.sentinel.service1,
                                  mock.sentinel.service3])
