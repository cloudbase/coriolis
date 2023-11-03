# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock
from coriolis import exception
from coriolis import cache
from coriolis.tests import test_base


class CacheTestCase(test_base.CoriolisBaseTestCase):
    """Collection of tests for the Coriolis cache package."""

    @mock.patch.object(cache.cache, 'get_memoization_decorator')
    def test_get_cache_decorator(self, mock_get_memoization_decorator):
        provider = 'ValidProviderName'
        result = cache.get_cache_decorator(provider)

        self.assertEqual(result, mock_get_memoization_decorator.return_value)
        mock_get_memoization_decorator.assert_called_once_with(
            cache.CONF,
            cache.cache_region,
            provider
        )

    def test_get_cache_decorator_invalid_provider_name(self):
        invalid_providers = [123, '', None]

        for provider in invalid_providers:
            self.assertRaises(exception.CoriolisException,
                              cache.get_cache_decorator,
                              provider)
