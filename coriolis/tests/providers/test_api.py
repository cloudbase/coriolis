# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.providers import api as providers_module
from coriolis.tests import test_base


class APITestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis API class."""

    def setUp(self):
        super(APITestCase, self).setUp()
        self.api = providers_module.API()
        self.rpc_client = mock.MagicMock()
        self.ctxt = mock.sentinel.ctxt
        self.api._rpc_client = self.rpc_client

    def test_get_available_providers(self):
        result = self.api.get_available_providers(self.ctxt)

        self.rpc_client.get_available_providers.assert_called_once_with(
            self.ctxt)
        self.assertEqual(result,
                         self.rpc_client.get_available_providers.return_value)

    def test_get_provider_schemas(self):
        result = self.api.get_provider_schemas(
            self.ctxt, mock.sentinel.platform_name, '1')

        self.rpc_client.get_provider_schemas.assert_called_once_with(
            self.ctxt, mock.sentinel.platform_name, 1)
        self.assertEqual(
            result, self.rpc_client.get_provider_schemas.return_value)
