# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

import importlib
import sys
from unittest import mock

from coriolis.tests import test_base

ENV_MODULE_NAME = "coriolis.db.sqlalchemy.alembic.env"


class AlembicEnvTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Alembic 'env.py' migration script."""

    def setUp(self):
        super(AlembicEnvTestCase, self).setUp()
        self.addCleanup(sys.modules.pop, ENV_MODULE_NAME, None)

    def _import_env(self, offline_mode, connection=None):
        # run_migrations_online or run_migrations_offline runs on module
        # import, so we need to set up what we need beforehand.
        sys.modules.pop(ENV_MODULE_NAME, None)

        mock_context = mock.MagicMock()
        mock_context.is_offline_mode.return_value = offline_mode
        mock_context.config.attributes.get.return_value = connection
        with mock.patch("alembic.context", mock_context):
            env = importlib.import_module(ENV_MODULE_NAME)

        return env, mock_context

    def test_offline_mode(self):
        env, mock_context = self._import_env(offline_mode=True)

        self.assertIs(env.config, mock_context.config)
        mock_context.config.get_main_option.assert_called_once_with(
            "sqlalchemy.url")
        mock_context.configure.assert_called_once_with(
            url=mock_context.config.get_main_option.return_value,
            target_metadata=env.target_metadata,
            render_as_batch=True,
            literal_binds=True,
            dialect_opts={"paramstyle": "named"},
        )
        mock_context.begin_transaction.assert_called_once_with()
        mock_context.run_migrations.assert_called_once_with()

    def test_online_mode_with_existing_connection(self):
        env, mock_context = self._import_env(
            offline_mode=False, connection=mock.sentinel.connection)

        mock_context.config.attributes.get.assert_called_once_with(
            "connection", None)
        mock_context.configure.assert_called_once_with(
            connection=mock.sentinel.connection,
            target_metadata=env.target_metadata,
            render_as_batch=True,
        )
        mock_context.begin_transaction.assert_called_once_with()
        mock_context.run_migrations.assert_called_once_with()

    @mock.patch("sqlalchemy.engine_from_config")
    def test_online_mode_creates_engine(self, mock_engine_from_config):
        env, mock_context = self._import_env(offline_mode=False)

        mock_engine_from_config.assert_called_once_with(
            mock_context.config.get_section.return_value,
            prefix="sqlalchemy.",
            poolclass=env.pool.NullPool,
        )
        mock_connectable = mock_engine_from_config.return_value
        mock_connectable.connect.assert_called_once_with()
        mock_connection = (
            mock_connectable.connect.return_value.__enter__.return_value)
        mock_context.configure.assert_called_once_with(
            connection=mock_connection,
            target_metadata=env.target_metadata,
            render_as_batch=True,
        )
        mock_context.begin_transaction.assert_called_once_with()
        mock_context.run_migrations.assert_called_once_with()
