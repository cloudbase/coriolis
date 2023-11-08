# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

"""Defines base class for all tests."""

from unittest import mock

from oslotest import base

from coriolis.api.v1.views import utils as views_utils


class CoriolisBaseTestCase(base.BaseTestCase):

    def setUp(self):
        super(CoriolisBaseTestCase, self).setUp()


class CoriolisApiViewsTestCase(CoriolisBaseTestCase):

    def setUp(self):
        super(CoriolisApiViewsTestCase, self).setUp()
        self._single_response = {"key1": "value1", "key2": "value2"}
        self._collection_response = [
            self._single_response, self._single_response]
        self._format_fun = views_utils.format_opt

    def _single_view_test(self, fun, expected_result_key, keys=None):
        format_fun = '%s.%s' % (
            self._format_fun.__module__, self._format_fun.__name__)
        with mock.patch(format_fun) as format_mock:
            result = fun(self._single_response, keys)
            format_mock.assert_called_once_with(self._single_response, keys)
            expected_result = {expected_result_key: format_mock.return_value}
            self.assertEqual(expected_result, result)

    def _collection_view_test(
            self, fun, expected_result_key, keys=None):
        format_fun = '%s.%s' % (
            self._format_fun.__module__, self._format_fun.__name__)
        with mock.patch(format_fun) as format_mock:
            f_opts = []
            result = fun(self._collection_response, keys)
            for c in self._collection_response:
                format_mock.assert_has_calls([mock.call(c, keys)])
                f_opts.append(format_mock.return_value)
            expected_result = {expected_result_key: f_opts}
            self.assertEqual(expected_result, result)
