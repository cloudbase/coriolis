# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import diagnostic_view
from coriolis.tests import test_base


class DiagnosticViewTestCase(test_base.CoriolisBaseTestCase):
    "Test suite for the Coriolis api v1 views."""

    def test_single(self):
        mock_single = "mock_single"
        expected_result = {'diagnostic': mock_single}

        result = diagnostic_view.single(mock_single)

        self.assertEqual(
            expected_result,
            result
        )

    def test_single_none(self):
        mock_single = None
        expected_result = {'diagnostic': mock_single}

        result = diagnostic_view.single(mock_single)

        self.assertEqual(
            expected_result,
            result
        )

    def test_collection(self):
        mock_collection = {"mock_key_1": "value_1", "mock_key_2": "value_2"}
        expected_result = {'diagnostics': mock_collection}

        result = diagnostic_view.collection(mock_collection)

        self.assertEqual(
            expected_result,
            result
        )

    def test_collection_none(self):
        mock_collection = {}
        expected_result = {'diagnostics': mock_collection}

        result = diagnostic_view.collection(mock_collection)

        self.assertEqual(
            expected_result,
            result
        )
