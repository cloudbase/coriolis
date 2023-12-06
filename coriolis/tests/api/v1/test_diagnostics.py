# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.api.v1 import diagnostics as diag
from coriolis.api.v1.views import diagnostic_view
from coriolis.diagnostics import api
from coriolis.tests import test_base


class DiagnosticsControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Diagnostics v1 API"""

    def setUp(self):
        super(DiagnosticsControllerTestCase, self).setUp()
        self.diag_api = diag.DiagnosticsController()

    @mock.patch.object(api.API, 'get')
    @mock.patch.object(diagnostic_view, 'collection')
    def test_index(
        self,
        mock_collection,
        mock_get
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}

        result = self.diag_api.index(mock_req)

        mock_context.can.assert_called_once_with("migration:diagnostics:get")
        mock_get.assert_called_once_with(mock_context)
        mock_collection.assert_called_once_with(mock_get.return_value)
        self.assertEqual(result, mock_collection.return_value)
