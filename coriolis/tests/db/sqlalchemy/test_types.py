# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock
import zlib

from coriolis.db.sqlalchemy import types
from coriolis.tests import test_base


class DatabaseSqlalchemyLongTextTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy types LongText."""

    def test_load_dialect_impl(self):
        long_text = types.LongText()
        mock_dialect = mock.Mock()
        mock_dialect.name = 'mysql'

        result = long_text.load_dialect_impl(mock_dialect)

        self.assertEqual(
            mock_dialect.type_descriptor.return_value,
            result
        )

        mock_dialect.name = 'sqlite'

        result = long_text.load_dialect_impl(mock_dialect)

        self.assertEqual(
            long_text.impl,
            result
        )


class DatabaseSqlalchemyBlobTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy types Blob."""

    def test_load_dialect_impl(self):
        blob = types.Blob()
        mock_dialect = mock.Mock()
        mock_dialect.name = 'mysql'

        result = blob.load_dialect_impl(mock_dialect)

        self.assertEqual(
            mock_dialect.type_descriptor.return_value,
            result
        )

        mock_dialect.name = 'sqlite'

        result = blob.load_dialect_impl(mock_dialect)

        self.assertEqual(
            blob.impl,
            result
        )


class DatabaseSqlalchemyJsonTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy types Json."""

    def setUp(self):
        super(DatabaseSqlalchemyJsonTestCase, self).setUp()
        self.type = types.Json()

    def test_process_bind_param(self):
        mock_dialect = mock.Mock()

        result = self.type.process_bind_param(
            {"mock_key": "mock_value"}, mock_dialect)

        self.assertEqual(
            '{"mock_key": "mock_value"}',
            result
        )

    def test_process_result_value(self):
        mock_dialect = mock.Mock()

        result = self.type.process_result_value(
            '{"mock_key": "mock_value"}', mock_dialect)

        self.assertEqual(
            {"mock_key": "mock_value"},
            result
        )

        result = self.type.process_result_value(None, mock_dialect)

        self.assertEqual(
            None,
            result
        )


class DatabaseSqlalchemyBsonTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy types Bson."""

    def setUp(self):
        super(DatabaseSqlalchemyBsonTestCase, self).setUp()
        self.type = types.Bson()

    def test_process_bind_param(self):
        mock_dialect = mock.Mock()

        result = self.type.process_bind_param(
            {"mock_key": "mock_value"}, mock_dialect)

        self.assertEqual(
            '{"mock_key": "mock_value"}',
            zlib.decompress(result).decode('utf-8')
        )

    def test_process_result_value(self):
        mock_dialect = mock.Mock()

        result = self.type.process_result_value(
            zlib.compress('{"mock_key": "mock_value"}'.encode('utf-8')),
            mock_dialect
        )

        self.assertEqual(
            {"mock_key": "mock_value"},
            result
        )

        result = self.type.process_result_value(
            '{"mock_key": "mock_value"}', mock_dialect)

        self.assertEqual(
            {"mock_key": "mock_value"},
            result
        )

        result = self.type.process_result_value(None, mock_dialect)

        self.assertEqual(
            None,
            result
        )


class DatabaseSqlalchemyListTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy types List."""

    def setUp(self):
        super(DatabaseSqlalchemyListTestCase, self).setUp()
        self.type = types.List()

    def test_load_dialect_impl(self):
        mock_dialect = mock.Mock()
        mock_dialect.name = 'mysql'

        result = self.type.load_dialect_impl(mock_dialect)

        self.assertEqual(
            mock_dialect.type_descriptor.return_value,
            result
        )

        mock_dialect.name = 'sqlite'

        result = self.type.load_dialect_impl(mock_dialect)

        self.assertEqual(
            self.type.impl,
            result
        )

    def test_process_bind_param(self):
        mock_dialect = mock.Mock()

        result = self.type.process_bind_param(
            {"mock_key": "mock_value"}, mock_dialect)

        self.assertEqual(
            '{"mock_key": "mock_value"}',
            result
        )

    def test_process_result_value(self):
        mock_dialect = mock.Mock()

        result = self.type.process_result_value(
            '{"mock_key": "mock_value"}', mock_dialect)

        self.assertEqual(
            {"mock_key": "mock_value"},
            result
        )

        result = self.type.process_result_value(None, mock_dialect)

        self.assertEqual(
            None,
            result
        )
