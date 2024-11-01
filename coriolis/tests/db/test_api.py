# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock
import uuid

import ddt
import sqlalchemy.orm

from coriolis.db import api
from coriolis.db.sqlalchemy import api as sqlalchemy_api
from coriolis.db.sqlalchemy import models
from coriolis import context
from coriolis import exception
from coriolis.tests import test_base
from coriolis.tests import testutils


@ddt.ddt
class DBAPITestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the common Coriolis DB API."""

    @classmethod
    def setUpClass(cls):
        super(DBAPITestCase, cls).setUpClass()
        with mock.patch.object(sqlalchemy_api, 'CONF') as mock_conf:
            mock_conf.database.connection = "sqlite://"
            engine = api.get_engine()
            models.BASE.metadata.create_all(engine)
            cls.session = api.get_session()
            cls.context = context.get_admin_context()
            # cls.context.session = cls.session

    @classmethod
    def tearDownClass(cls):
        cls.session.rollback()
        cls.session.close()
        super(cls, DBAPITestCase).tearDownClass()


    def test_get_engine(self):
        self.assertEqual(api.get_engine(), api.IMPL.get_engine())

    def test_get_session(self):
        self.assertIsInstance(api.get_session(), sqlalchemy.orm.Session)

    @mock.patch.object(api, 'IMPL')
    def test_db_sync(self, mock_impl):
        self.assertEqual(
            api.db_sync(mock.sentinel.engine, version=mock.sentinel.version),
            mock_impl.db_sync.return_value)
        mock_impl.db_sync.assert_called_once_with(
            mock.sentinel.engine, version=mock.sentinel.version)

    @mock.patch.object(api, 'IMPL')
    def test_db_version(self, mock_impl):
        self.assertEqual(
            api.db_version(mock.sentinel.engine),
            mock_impl.db_version.return_value)
        mock_impl.db_version.assert_called_once_with(mock.sentinel.engine)

    def test__session(self):
        context = mock.Mock()
        self.assertEqual(api._session(context), context.session)

    @mock.patch.object(api, 'get_session')
    def test__session_no_context(self, mock_get_session):
        self.assertEqual(
            api._session(None),
            mock_get_session.return_value)

    @mock.patch.object(api, 'get_session')
    def test__session_sessionless_context(self, mock_get_session):
        context = mock.Mock(session=None)
        self.assertEqual(
            api._session(context),
            mock_get_session.return_value)

    @ddt.data(
        {"kwargs": {}, "expected_result": False},
        {"kwargs": {"user_id": None}, "expected_result": False},
        {"kwargs": {"user_id": "1", "project_id": None},
         "expected_result": False},
        {"kwargs": {"user_id": "1", "project_id": "1", "is_admin": True},
         "expected_result": False},
        {"kwargs": {"user_id": "1", "project_id": "1", "is_admin": False},
         "expected_result": True},
    )
    def test_is_user_context(self, data):
        context = mock.Mock(**data.get('kwargs', {}))
        self.assertEqual(
            api.is_user_context(context), data.get('expected_result'))

    @mock.patch.object(api, '_session')
    def test__model_query(self, mock_session):
        self.assertEqual(
            api._model_query(mock.sentinel.context, mock.sentinel.model),
            mock_session.return_value.query.return_value)
        mock_session.assert_called_once_with(
            mock.sentinel.context)
        mock_session.return_value.query.assert_called_once_with(
            mock.sentinel.model)

    def test__update_sqlalchemy_object_fields_non_dict_values(self):
        self.assertRaises(
            exception.InvalidInput, api._update_sqlalchemy_object_fields,
            mock.ANY, mock.ANY, None)

    def test__update_sqlalchemy_object_fields_conflict(self):
        updateable_fields = ["field1", "field2"]
        values_to_update = {"field1": "value1", "field3": "value3"}
        self.assertRaises(
            exception.Conflict, api._update_sqlalchemy_object_fields,
            mock.ANY, updateable_fields, values_to_update)

    def test__update_sqlalchemy_object_fields_invalid_obj_field(self):
        self.assertRaises(
            exception.InvalidInput, api._update_sqlalchemy_object_fields,
            models.Endpoint, ["invalid_field"], {"invalid_field": "new_value"})

    def test__update_sqlalchemy_object_fields(self):
        obj = models.Endpoint()
        obj.description = "initial test description"
        new_description = "updated test description"

        api._update_sqlalchemy_object_fields(
            obj, ["description"], {"description": new_description})
        self.assertEqual(obj.description, new_description)


class EndpointDBAPITestCase(DBAPITestCase):

    @classmethod
    def setUpClass(cls):
        super(EndpointDBAPITestCase, cls).setUpClass()
        cls.valid_endpoint = models.Endpoint()
        cls.valid_endpoint.id = str(uuid.uuid4())
        cls.valid_endpoint.user_id = "1"
        cls.valid_endpoint.project_id = "1"
        cls.valid_endpoint.connection_info = {
            "conn_info": {"secret": "info"}}
        cls.valid_endpoint.type = "openstack"
        cls.valid_endpoint.name = "test_openstack_endpoint"
        cls.valid_endpoint.description = (
            "Test Openstack Endpoint Description")
        # have at least one endpoint in DB
        cls.session.add(cls.valid_endpoint)
        cls.session.commit()

    def test_get_endpoints(self):
        result = testutils.get_wrapped_function(api.get_endpoints)(
            self.context)
        self.assertIn(self.valid_endpoint, result)
