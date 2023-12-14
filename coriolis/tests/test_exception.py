# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import exception
from coriolis.tests import test_base


class ConvertedExceptionTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis ConvertedException class."""

    def test__init__(self):
        result = exception.ConvertedException(
            code=403, title='Forbidden', explanation='test')

        self.assertEqual(result.code, 403)
        self.assertEqual(result.title, 'Forbidden')
        self.assertEqual(result.explanation, 'test')

    def test__init__without_title(self):
        result = exception.ConvertedException(code=403, explanation='test')

        self.assertEqual(result.code, 403)
        self.assertEqual(result.title, 'Forbidden')
        self.assertEqual(result.explanation, 'test')

    def test__init__with_code_not_in_status_reasons(self):
        result = exception.ConvertedException(code=450, explanation='test')

        self.assertEqual(result.code, 450)
        self.assertEqual(result.title, 'Unknown Client Error')
        self.assertEqual(result.explanation, 'test')


class CoriolisTestException(exception.CoriolisException):
    message = "Test message with missing placeholder: %(missing_key)s"


class CoriolisExceptionTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis CoriolisException class."""

    def test__init__(self):
        result = exception.CoriolisException()

        self.assertEqual(result.message, 'An unknown exception occurred.')
        self.assertEqual(result.code, 500)
        self.assertEqual(result.headers, {})
        self.assertEqual(result.safe, False)

    def test__init__with_exception_in_kwargs_items(self):
        kwargs = {'test_key': 'test_value', 'code': 500}
        kwargs['exc'] = ValueError('Test exception message')

        result = exception.CoriolisException(**kwargs)

        kwargs['exc'] = str(kwargs['exc'])

        self.assertEqual(result.msg, 'An unknown exception occurred.')
        self.assertEqual(result.kwargs, kwargs)

    def test__init__custom_message_and_kwargs(self):
        custom_message = 'test-message'
        kwargs = {'test-key': 'test-value'}
        result = exception.CoriolisException(custom_message, **kwargs)

        expected_message = custom_message % kwargs
        expected_kwargs = {'test-key': 'test-value', 'code': 500}

        self.assertEqual(result.msg, expected_message)
        self.assertEqual(result.kwargs, expected_kwargs)

    @mock.patch.object(exception, 'LOG')
    @mock.patch.object(exception, 'CONF')
    def test__init__with_exception_in_string_format(self, mock_conf, mock_log):
        mock_conf.fatal_exception_format_errors = False
        kwargs = {'some_key': 'some_value'}

        result = CoriolisTestException(**kwargs)

        self.assertEqual(result.msg, CoriolisTestException.message)
        mock_log.exception.assert_called_once_with(
            'Exception in string format operation')
        expected_calls = [
            mock.call("%(name)s: %(value)s",
                      {'name': 'some_key', 'value': 'some_value'}),
            mock.call("%(name)s: %(value)s", {'name': 'code', 'value': 500})
        ]
        mock_log.error.assert_has_calls(expected_calls)
        self.assertEqual(mock_log.error.call_count, 2)

    @mock.patch.object(exception, 'CONF')
    def test__init__with_fatal_exception_format_errors(self, mock_conf):
        mock_conf.fatal_exception_format_errors = True
        kwargs = {'some_key': 'some_value'}

        self.assertRaises(KeyError, CoriolisTestException, **kwargs)

    def test__init__with_exception_message(self):
        message = ValueError('Test exception message')
        result = exception.CoriolisException(message)
        self.assertEqual(str(result), 'Test exception message')

    @mock.patch.object(exception, 'CONF')
    def test__unicode__(self, mock_conf):
        mock_conf.fatal_exception_format_errors = False
        kwargs = {'missing_key': 'some_value'}

        exception_instance = CoriolisTestException(**kwargs)
        result = exception_instance.__unicode__()
        self.assertEqual(
            result, "Test message with missing placeholder: some_value")


class APIExceptionTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis APIException class."""

    def test__init__(self):
        result = exception.APIException(service='test_service')
        self.assertEqual(result.kwargs['service'], 'test_service')

    def test__init__no_service(self):
        result = exception.APIException()
        self.assertEqual(result.kwargs['service'], 'unknown')
