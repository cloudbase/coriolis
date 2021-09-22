# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.db import api
from coriolis import exception
from coriolis.tests import test_base


def get_wrapped_function(function):
    """Get the method at the bottom of a stack of decorators."""
    if not hasattr(function, '__closure__') or not function.__closure__:
        return function

    def _get_wrapped_function(function):
        if not hasattr(function, '__closure__') or not function.__closure__:
            return None

        for closure in function.__closure__:
            func = closure.cell_contents

            deeper_func = _get_wrapped_function(func)
            if deeper_func:
                return deeper_func
            elif hasattr(closure.cell_contents, '__call__'):
                return closure.cell_contents

        return function

    return _get_wrapped_function(function)


class DBAPITestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis DB API."""

    @mock.patch.object(api, 'get_endpoint')
    def test_update_endpoint_not_found(self, mock_get_endpoint):
        mock_get_endpoint.return_value = None

        # We only need to test the unwrapped functions. Without this,
        # when calling a coriolis.db.api function, it will try to
        # establish an SQL connection.
        update_endpoint = get_wrapped_function(api.update_endpoint)

        self.assertRaises(exception.NotFound, update_endpoint,
                          mock.sentinel.context, mock.sentinel.endpoint_id,
                          mock.sentinel.updated_values)

        mock_get_endpoint.assert_called_once_with(mock.sentinel.context,
                                                  mock.sentinel.endpoint_id)
