# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import os
from unittest import mock

from oslo_db.sqlalchemy import migration as oslo_migration

from coriolis.db.sqlalchemy import migration
from coriolis.tests import test_base


class DatabaseSqlalchemyMigrationTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy migration."""

    @mock.patch.object(os.path, 'abspath')
    @mock.patch.object(oslo_migration, 'db_sync')
    def test_db_sync(self, mock_db_sync, mock_abspath):
        mock_abspath.return_value = "/abspath"

        result = migration.db_sync(mock.sentinel.engine, mock.sentinel.version)

        self.assertEqual(
            mock_db_sync.return_value,
            result
        )
        mock_db_sync.assert_called_once_with(
            mock.sentinel.engine,
            "/abspath/migrate_repo",
            mock.sentinel.version,
            init_version=0
        )

    @mock.patch.object(os.path, 'abspath')
    @mock.patch.object(oslo_migration, 'db_version')
    def test_db_version(self, mock_db_version, mock_abspath):
        mock_abspath.return_value = "/abspath"

        result = migration.db_version(mock.sentinel.engine)

        self.assertEqual(mock_db_version.return_value, result)
        mock_db_version.assert_called_once_with(
            mock.sentinel.engine,
            "/abspath/migrate_repo",
            0
        )

    @mock.patch.object(os.path, 'abspath')
    @mock.patch.object(oslo_migration, 'db_version_control')
    def test_db_version_control(self, mock_db_version_control, mock_abspath):
        mock_abspath.return_value = "/abspath"

        result = migration.db_version_control(
            mock.sentinel.engine, mock.sentinel.version)

        self.assertEqual(mock_db_version_control.return_value, result)
        mock_db_version_control.assert_called_once_with(
            mock.sentinel.engine,
            "/abspath/migrate_repo",
            mock.sentinel.version
        )
