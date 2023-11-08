# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import utils as view_utils
from coriolis.tests import test_base


class ViewUtilsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis api v1 views."""

    def test_format_opt(self):
        mock_option = {"mock_key_1": "value_1", "mock_key_2": "value_2"}
        mock_keys = {"mock_key_1"}

        expected_result = {"mock_key_1": "value_1"}

        result = view_utils.format_opt(mock_option, mock_keys)

        self.assertEqual(
            expected_result,
            result
        )

    def test_format_opt_key_not_in_options(self):
        mock_option = {"mock_key_1": "value_1", "mock_key_2": "value_2"}
        mock_keys = {"mock_key_3"}

        expected_result = {}

        result = view_utils.format_opt(mock_option, mock_keys)

        self.assertEqual(
            expected_result,
            result
        )

    def test_format_opt_keys_none(self):
        mock_option = {"mock_key_1": "value_1", "mock_key_2": "value_2"}
        mock_keys = None

        expected_result = mock_option

        result = view_utils.format_opt(mock_option, mock_keys)

        self.assertEqual(
            expected_result,
            result
        )
