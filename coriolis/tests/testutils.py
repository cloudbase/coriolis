"""Defines general utilities for all tests."""

from unittest import mock


def identity_dec(item, *args, **kwargs):
    """A decorator which adds nothing to the decorated item."""
    return item


def make_identity_decorator_mock():
    """Returns a MagicMock with identity_dec as a side-effect."""
    return mock.MagicMock(side_effect=identity_dec)


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
