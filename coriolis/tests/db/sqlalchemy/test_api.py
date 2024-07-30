# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from oslo_config import cfg
from oslo_db.sqlalchemy import session as db_session

from coriolis.db.sqlalchemy import api
from coriolis.db.sqlalchemy import migration
from coriolis import exception
from coriolis.tests import test_base


class DatabaseSqlalchemyApiTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy api."""

    @mock.patch.object(db_session, 'EngineFacade')
    def test_get_facade_none(self, mock_EngineFacade):
        cfg.CONF.database.connection = mock.sentinel.connection
        api._facade = None

        result = api.get_facade()

        self.assertEqual(
            mock_EngineFacade.return_value,
            result
        )
        mock_EngineFacade.assert_called_once_with(mock.sentinel.connection)

    @mock.patch.object(db_session, 'EngineFacade')
    def test_get_facade(self, mock_EngineFacade):
        api._facade = mock.sentinel.facade

        result = api.get_facade()

        self.assertEqual(
            mock.sentinel.facade,
            result
        )
        mock_EngineFacade.assert_not_called()

    @mock.patch.object(api, 'get_facade')
    def test_get_engine(self, mock_get_facade):
        result = api.get_engine()

        self.assertEqual(
            mock_get_facade.return_value.get_engine.return_value,
            result
        )

    @mock.patch.object(api, 'get_facade')
    def test_get_session(self, mock_get_facade):
        result = api.get_session()

        self.assertEqual(
            mock_get_facade.return_value.get_session.return_value,
            result
        )

    def test_get_backend(self):
        result = api.get_backend()

        self.assertEqual(
            api,
            result
        )

    @mock.patch.object(migration, 'db_sync')
    @mock.patch.object(api, 'db_version')
    def test_db_sync(
        self,
        mock_db_version,
        mock_db_sync
    ):
        result = api.db_sync(mock.sentinel.engine)

        self.assertEqual(
            mock_db_sync.return_value,
            result
        )
        mock_db_version.assert_not_called()
        mock_db_sync.assert_called_once_with(
            mock.sentinel.engine, version=None)

    @mock.patch.object(migration, 'db_sync')
    @mock.patch.object(api, 'db_version')
    def test_db_sync_version(
        self,
        mock_db_version,
        mock_db_sync
    ):
        mock_db_version.return_value = 1

        result = api.db_sync(mock.sentinel.engine, version=1)

        self.assertEqual(
            mock_db_sync.return_value,
            result
        )
        mock_db_version.assert_called_once_with(mock.sentinel.engine)
        mock_db_sync.assert_called_once_with(
            mock.sentinel.engine, version=1)

    @mock.patch.object(api, 'db_version')
    def test_db_sync_version_raise(
        self,
        mock_db_version
    ):
        mock_db_version.return_value = 2

        self.assertRaises(
            exception.CoriolisException,
            api.db_sync,
            mock.sentinel.engine,
            version=1
        )
        mock_db_version.assert_called_once_with(mock.sentinel.engine)

    @mock.patch.object(migration, 'db_version')
    def test_db_version(
        self,
        mock_db_version
    ):
        result = api.db_version(mock.sentinel.engine)

        self.assertEqual(
            mock_db_version.return_value,
            result
        )
        mock_db_version.assert_called_once_with(mock.sentinel.engine)
