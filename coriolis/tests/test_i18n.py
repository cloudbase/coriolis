# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import i18n
from coriolis.tests import test_base


class I18nTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis i18n module."""

    @mock.patch('oslo_i18n.enable_lazy')
    def test_enable_lazy(self, mock_enable_lazy):
        result = i18n.enable_lazy(True)

        mock_enable_lazy.assert_called_once_with(True)
        self.assertEqual(result, mock_enable_lazy.return_value)

    @mock.patch('oslo_i18n.translate')
    def test_translate(self, mock_translate):
        result = i18n.translate('test_value')

        mock_translate.assert_called_once_with('test_value', None)
        self.assertEqual(result, mock_translate.return_value)

    @mock.patch('oslo_i18n.get_available_languages')
    def test_get_available_languages(self, mock_get_available_languages):
        result = i18n.get_available_languages()

        mock_get_available_languages.assert_called_once_with(i18n.DOMAIN)
        self.assertEqual(result, mock_get_available_languages.return_value)
