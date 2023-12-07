# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.api.v1 import provider_schemas
from coriolis.providers import api
from coriolis.tests import test_base


class ProviderSchemasControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Provider Schemas v1 API"""

    def setUp(self):
        super(ProviderSchemasControllerTestCase, self).setUp()
        self.provider_schemas = provider_schemas.ProviderSchemasController()

    @mock.patch.object(api.API, 'get_provider_schemas')
    def test_index(
        self,
        get_provider_schemas
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        mock_platform_name = "mock_platform_name"
        mock_provider_type = "mock_provider_type"
        expected_result = {
            'schemas': get_provider_schemas.return_value
        }

        result = self.provider_schemas.index(
            mock_req, mock_platform_name, mock_provider_type)

        self.assertEqual(
            expected_result,
            result
        )

        get_provider_schemas.assert_called_once_with(
            mock_context, mock_platform_name, mock_provider_type)
