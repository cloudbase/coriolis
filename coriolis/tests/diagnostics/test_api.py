# Copyright 2019 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.diagnostics import api
from coriolis.tests import test_base
from coriolis import utils


class DiagnosticsAPITestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Diagnostics API."""

    def setUp(self):
        super(DiagnosticsAPITestCase, self).setUp()
        self.diagnostics_api = api.API()
        self.diagnostics_api._conductor_cli = mock.Mock()

    @mock.patch.object(utils, "get_diagnostics_info")
    def test_get(
        self,
        mock_get_diagnostics_info
    ):
        (self.diagnostics_api._conductor_cli.get_all_diagnostics.
            return_value) = [mock.sentinel.all_diag]
        mock_get_diagnostics_info.return_value = mock.sentinel.diag_info
        expected_result = [
            mock.sentinel.all_diag,
            mock.sentinel.diag_info
        ]

        result = self.diagnostics_api.get(mock.sentinel.context)

        self.assertEqual(
            expected_result,
            result
        )
        (self.diagnostics_api._conductor_cli.get_all_diagnostics.
            assert_called_once_with)(mock.sentinel.context)
        mock_get_diagnostics_info.assert_called_once()
