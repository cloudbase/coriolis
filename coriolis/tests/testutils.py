"""Defines general utilities for all tests."""

from unittest import mock


def identity_dec(item, *args, **kwargs):
    """A decorator which adds nothing to the decorated item."""
    return item


def make_identity_decorator_mock():
    """Returns a MagicMock with identity_dec as a side-effect."""
    return mock.MagicMock(side_effect=identity_dec)
