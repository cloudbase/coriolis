# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.api.v1 import providers
from coriolis.providers import api
from coriolis.tests import test_base


class ProviderControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Providers v1 API"""

    def setUp(self):
        super(ProviderControllerTestCase, self).setUp()
        self.providers = providers.ProviderController()

    @mock.patch.object(api.API, 'get_available_providers')
    def test_index(
        self,
        mock_get_available_providers
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        expected_result = {
            'providers': mock_get_available_providers.return_value
        }

        result = self.providers.index(mock_req)

        self.assertEqual(
            expected_result,
            result
        )

        mock_context.can.assert_called_once_with("migration:providers:list")
        mock_get_available_providers.assert_called_once_with(mock_context)
