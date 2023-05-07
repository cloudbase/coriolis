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


class DictToObject:
    """Converts a dictionary to an object with attributes.

    This is useful for mocking objects that are used as configuration
    objects.
    """

    def __init__(self, dictionary, skip_attrs=None):
        if skip_attrs is None:
            skip_attrs = []

        for key, value in dictionary.items():
            if key in skip_attrs:
                setattr(self, key, value)
            elif isinstance(value, dict):
                setattr(self, key, DictToObject(value, skip_attrs=skip_attrs))
            elif isinstance(value, list):
                setattr(
                    self, key,
                    [DictToObject(item, skip_attrs=skip_attrs) if isinstance(
                        item, dict) else item for item in value])
            else:
                setattr(self, key, value)

    def __getattr__(self, item):
        return None

    def __repr__(self):
        attrs = [f"{k}={v!r}" for k, v in self.__dict__.items()]
        attrs_str = ', '.join(attrs)
        return f"{self.__class__.__name__}({attrs_str})"
