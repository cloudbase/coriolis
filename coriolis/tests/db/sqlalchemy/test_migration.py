# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import exception
from coriolis.db.sqlalchemy import migration
from coriolis.tests import test_base


class DatabaseSqlalchemyMigrationTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy migration."""

    @mock.patch.object(migration, "sqlalchemy")
    def test_stamp_legacy_database_if_needed_no_table(self, mock_sqlalchemy):
        mock_sqlalchemy.inspect.return_value.get_table_names.return_value = ["foo"]
        mock_engine = mock.MagicMock()
        mock_config = mock.MagicMock()

        migration._stamp_legacy_database_if_needed(mock_engine, mock_config)

        mock_engine.connect.assert_not_called()

    @mock.patch.object(migration, "sqlalchemy")
    def test_stamp_legacy_database_if_needed_raises(self, mock_sqlalchemy):
        mock_sqlalchemy.inspect.return_value.get_table_names.return_value = [
            migration.LEGACY_VERSION_TABLE
        ]
        mock_engine = mock.MagicMock()
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.scalar.return_value = (
            migration.LEGACY_FINAL_VERSION + 1
        )
        mock_config = mock.MagicMock()

        self.assertRaises(
            exception.CoriolisException,
            migration._stamp_legacy_database_if_needed,
            mock_engine,
            mock_config,
        )

    @mock.patch.object(migration, "command")
    @mock.patch.object(migration, "sqlalchemy")
    def test_stamp_legacy_database_if_needed(
        self,
        mock_sqlalchemy,
        mock_command,
    ):
        mock_sqlalchemy.inspect.return_value.get_table_names.return_value = [
            migration.LEGACY_VERSION_TABLE
        ]
        mock_engine = mock.MagicMock()
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.scalar.return_value = 10
        mock_config = mock.MagicMock()

        migration._stamp_legacy_database_if_needed(mock_engine, mock_config)

        mock_command.stamp.assert_called_once_with(mock_config, "010")

    @mock.patch.object(migration, "command")
    @mock.patch.object(migration, "sqlalchemy")
    def test_stamp_legacy_database_if_needed_final_version(
        self,
        mock_sqlalchemy,
        mock_command,
    ):
        mock_sqlalchemy.inspect.return_value.get_table_names.return_value = [
            migration.LEGACY_VERSION_TABLE
        ]
        mock_engine = mock.MagicMock()
        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_conn.execute.return_value.scalar.return_value = (
            migration.LEGACY_FINAL_VERSION
        )
        mock_config = mock.MagicMock()

        migration._stamp_legacy_database_if_needed(mock_engine, mock_config)

        mock_command.stamp.assert_called_once_with(
            mock_config, "%03d" % migration.LEGACY_FINAL_VERSION
        )

    @mock.patch.object(migration, "_stamp_legacy_database_if_needed")
    @mock.patch.object(migration, "command")
    @mock.patch.object(migration, "_get_alembic_config")
    def test_db_sync(
        self,
        mock_get_config,
        mock_command,
        mock_stamp_legacy,
    ):
        mock_engine = mock.MagicMock()

        result = migration.db_sync(mock_engine, mock.sentinel.version)

        self.assertEqual(mock_command.upgrade.return_value, result)
        mock_get_config.assert_called_once_with()
        mock_stamp_legacy.assert_called_once_with(
            mock_engine, mock_get_config.return_value
        )
        mock_command.upgrade.assert_called_once_with(
            mock_get_config.return_value, mock.sentinel.version
        )

    @mock.patch.object(migration.alembic_migration, "MigrationContext")
    def test_db_version(self, mock_migration_context):
        mock_engine = mock.MagicMock()

        result = migration.db_version(mock_engine)

        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        mock_configure = mock_migration_context.configure
        mock_configure.assert_called_once_with(mock_conn)
        self.assertEqual(
            mock_configure.return_value.get_current_revision.return_value,
            result,
        )

    @mock.patch.object(migration, "command")
    @mock.patch.object(migration, "_get_alembic_config")
    def test_db_version_control(self, mock_get_config, mock_command):
        result = migration.db_version_control(mock.MagicMock(), mock.sentinel.version)

        self.assertEqual(mock_command.stamp.return_value, result)
        mock_command.stamp.assert_called_once_with(
            mock_get_config.return_value, mock.sentinel.version
        )
