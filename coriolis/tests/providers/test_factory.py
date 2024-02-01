# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import exception
from coriolis.providers import factory
from coriolis.tests import test_base


class FactoryTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis factory module."""

    @mock.patch.object(factory.utils, 'load_class')
    @mock.patch.object(factory, 'CONF')
    def test_get_available_providers(self, mock_conf, mock_load_class):
        mock_conf.providers = [mock.MagicMock(), mock.MagicMock()]

        # Create a provider class for testing purposes
        class ProviderClass:
            platform = mock.sentinel.platform
            type = mock.sentinel.type

        mock_load_class.return_value = ProviderClass

        # We mocked the PROVIDER_TYPE_MAP to avoid having to import all the
        # providers
        with mock.patch.dict('coriolis.providers.factory.PROVIDER_TYPE_MAP',
                             {'type1': ProviderClass,
                              'type2': ProviderClass}):
            expected_result = {
                mock.sentinel.platform: {
                    'types': ['type1', 'type2']
                }
            }

            result = factory.get_available_providers()

            mock_load_class.assert_has_calls([
                mock.call(mock_conf.providers[0]),
                mock.call(mock_conf.providers[1])]
            )
            self.assertEqual(result, expected_result)

    @mock.patch.object(factory.utils, 'load_class')
    @mock.patch.object(factory, 'CONF')
    def test_get_provider(self, mock_conf, mock_load_class):
        mock_conf.providers = [mock.MagicMock(), mock.MagicMock()]

        mock_class = mock.MagicMock()
        mock_class.platform = mock.sentinel.platform
        mock_load_class.return_value = mock_class

        result = factory.get_provider(
            mock.sentinel.platform, mock.sentinel.provider_type,
            mock.sentinel.event_handler, raise_if_not_found=False)

        mock_load_class.assert_has_calls([
            mock.call(mock_conf.providers[0]),
            mock.call(mock_conf.providers[1])]
        )

        self.assertEqual(result, None)

    @mock.patch.object(factory.utils, 'load_class')
    @mock.patch.object(factory, 'CONF')
    def test_get_provider_found(self, mock_conf, mock_load_class):
        mock_conf.providers = [mock.MagicMock(), mock.MagicMock()]

        class BaseProviderType:
            pass

        class ProviderClass(BaseProviderType):
            platform = mock.sentinel.platform
            type = mock.sentinel.provider_type

            def __init__(self, event_handler=None):
                self.event_handler = event_handler

        mock_load_class.return_value = ProviderClass

        with mock.patch.dict('coriolis.providers.factory.PROVIDER_TYPE_MAP',
                             {mock.sentinel.provider_type: BaseProviderType}):
            result = factory.get_provider(
                mock.sentinel.platform, mock.sentinel.provider_type,
                mock.sentinel.event_handler, raise_if_not_found=False)

            mock_load_class.assert_has_calls([
                mock.call(mock_conf.providers[0])])

            self.assertIsInstance(result, ProviderClass)

    @mock.patch.dict('coriolis.providers.factory.PROVIDER_TYPE_MAP', {})
    def test_get_provider_with_exception(self):
        self.assertRaises(
            exception.NotFound, factory.get_provider, mock.sentinel.platform,
            mock.sentinel.provider_type, mock.sentinel.event_handler,
            raise_if_not_found=True)
