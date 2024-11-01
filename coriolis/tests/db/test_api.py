# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import ddt
from unittest import mock

from coriolis.db import api
from coriolis import exception
from coriolis.tests import test_base
from coriolis.tests import testutils


class CoriolisTestException(Exception):
    pass


class CustomMock(mock.MagicMock):
    def __getattr__(self, name):
        return None


@ddt.ddt
class DBAPITestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis DB API."""

    def setUp(self):
        super(DBAPITestCase, self).setUp()
        self.IMPL = mock.Mock()
        api.IMPL = self.IMPL

    def test_get_engine(self):
        result = api.get_engine()

        self.assertEqual(
            self.IMPL.get_engine.return_value,
            result
        )

    def test__session(self):
        result = api.get_session()

        self.assertEqual(
            self.IMPL.get_session.return_value,
            result
        )

    def test_db_sync(self):
        result = api.db_sync(
            mock.sentinel.engine, version=mock.sentinel.version)

        self.assertEqual(
            self.IMPL.db_sync.return_value,
            result
        )
        self.IMPL.db_sync.assert_called_once_with(
            mock.sentinel.engine, version=mock.sentinel.version)

    def test_db_version(self):
        result = api.db_version(mock.sentinel.engine)

        self.assertEqual(
            self.IMPL.db_version.return_value,
            result
        )
        self.IMPL.db_version.assert_called_once_with(mock.sentinel.engine)

    @mock.patch.object(api, 'get_session')
    def test_session_context(self, mock_get_session):
        mock_context = mock.Mock()

        result = api._session(mock_context)

        self.assertEqual(
            mock_context.session,
            result
        )
        mock_get_session.assert_not_called()

    @mock.patch.object(api, 'get_session')
    def test_session(self, mock_get_session):
        mock_context = None

        result = api._session(mock_context)

        self.assertEqual(
            mock_get_session.return_value,
            result
        )
        mock_get_session.assert_called_once()

    @ddt.data(
        {
            "expected_result": False
        },
        {
            "has_context": True,
            "user_id": None,
            "project_id": None,
            "is_admin": False,
            "expected_result": False
        },
        {
            "has_context": True,
            "user_id": "mock_user_id",
            "project_id": "mock_project_id",
            "is_admin": True,
            "expected_result": False
        },
        {
            "has_context": True,
            "user_id": "mock_user_id",
            "project_id": "mock_project_id",
            "is_admin": False,
            "expected_result": True
        },
    )
    def test_is_user_context(self, data):
        if data.get("has_context", False) is False:
            mock_context = None
        else:
            mock_context = mock.Mock()
            mock_context.user_id = data.get("user_id")
            mock_context.project_id = data.get("project_id")
            mock_context.is_admin = data.get("is_admin")

        result = api.is_user_context(mock_context)

        self.assertEqual(
            data["expected_result"],
            result
        )

    @mock.patch.object(api, '_session')
    def test_model_query(self, mock_session):
        result = api._model_query(mock.sentinel.context, mock.sentinel.args)

        self.assertEqual(
            mock_session.return_value.query.return_value,
            result
        )
        mock_session.assert_called_once_with(mock.sentinel.context)
        mock_session.return_value.query.assert_called_once_with(
            mock.sentinel.args)

    def test_update_sqlalchemy_object_fields_values_not_dict(self):
        values_to_update = []

        self.assertRaises(
            exception.InvalidInput,
            api._update_sqlalchemy_object_fields,
            mock.sentinel.obj,
            mock.sentinel.updatablefields,
            values_to_update
        )

    def test_update_sqlalchemy_object_fields_field_not_updatable(self):
        obj = mock.Mock()
        updateable_fields = []
        values_to_update = {"key": "value"}

        self.assertRaises(
            exception.Conflict,
            api._update_sqlalchemy_object_fields,
            obj,
            updateable_fields,
            values_to_update
        )

    def test_update_sqlalchemy_object_fields_field_not_in_obj(self):
        obj = mock.Mock()
        del obj.key
        updateable_fields = ["key"]
        values_to_update = {"key": "value"}

        self.assertRaises(
            exception.InvalidInput,
            api._update_sqlalchemy_object_fields,
            obj,
            updateable_fields,
            values_to_update
        )

    def test_update_sqlalchemy_object_fields(self):
        obj = mock.Mock()
        updateable_fields = ["key"]
        values_to_update = {"key": "value"}

        api._update_sqlalchemy_object_fields(
            obj,
            updateable_fields,
            values_to_update
        )

        self.assertEqual(
            "value",
            obj.key
        )

    @mock.patch.object(api, 'is_user_context')
    @mock.patch.object(api, '_soft_delete_aware_query')
    def test_get_replica_schedules_filter(
        self,
        mock_soft_delete_aware_query,
        mock_is_user_context
    ):
        context = mock.Mock()

        result = api._get_replica_schedules_filter(
            context,
            mock.sentinel.replica_id,
            mock.sentinel.replica_id,
            expired=False
        )

        self.assertEqual(
            (mock_soft_delete_aware_query.return_value.join.return_value.
             filter.return_value.filter.return_value.filter.return_value.
             filter.return_value.filter.return_value),
            result
        )
        mock_is_user_context.assert_called_once_with(context)

    @mock.patch.object(api, 'is_user_context')
    @mock.patch.object(api, '_soft_delete_aware_query')
    def test_get_replica_schedules_filter_not_user_expired(
        self,
        mock_soft_delete_aware_query,
        mock_is_user_context
    ):
        context = mock.Mock()
        mock_is_user_context.return_value = False

        result = api._get_replica_schedules_filter(
            context,
            mock.sentinel.replica_id,
            mock.sentinel.replica_id,
            expired=True
        )

        self.assertEqual(
            (mock_soft_delete_aware_query.return_value.join.return_value.
             filter.return_value.filter.return_value.filter.return_value),
            result
        )
        mock_is_user_context.assert_called_once_with(context)

    @mock.patch.object(api, '_model_query')
    def test_soft_delete_aware_query(
        self,
        mock_model_query
    ):
        context = mock.Mock()
        context.show_deleted = None

        result = api._soft_delete_aware_query(context)

        self.assertEqual(
            mock_model_query.return_value.filter_by.return_value,
            result
        )

    @mock.patch.object(api, '_model_query')
    def test_soft_delete_aware_query_show_deleted(
        self,
        mock_model_query
    ):
        context = mock.Mock()
        context.show_deleted = None

        result = api._soft_delete_aware_query(context, show_deleted=True)

        self.assertEqual(
            mock_model_query.return_value,
            result
        )

        context.show_deleted = True

        result = api._soft_delete_aware_query(context)

        self.assertEqual(
            mock_model_query.return_value,
            result
        )

    @mock.patch.object(api, 'delete_endpoint_region_mapping')
    def test_try_unmap_regions(self, mock_delete_endpoint_region_mapping):
        _try_unmap_regions = testutils.get_wrapped_function(
            api._try_unmap_regions)

        with self.assertLogs('coriolis.db.api', level="DEBUG"):
            _try_unmap_regions(
                mock.sentinel.context,
                [mock.sentinel.region_id1, mock.sentinel.region_id2],
                mock.sentinel.endpoint_id
            )
            mock_delete_endpoint_region_mapping.assert_has_calls([
                mock.call(
                    mock.sentinel.context,
                    mock.sentinel.endpoint_id,
                    mock.sentinel.region_id1
                ),
                mock.call(
                    mock.sentinel.context,
                    mock.sentinel.endpoint_id,
                    mock.sentinel.region_id2
                )
            ])

    @mock.patch.object(api, 'delete_endpoint_region_mapping')
    def test_try_unmap_regions_fails(
        self,
        mock_delete_endpoint_region_mapping
    ):
        _try_unmap_regions = testutils.get_wrapped_function(
            api._try_unmap_regions)
        mock_delete_endpoint_region_mapping.side_effect = \
            CoriolisTestException()

        with self.assertLogs('coriolis.db.api', level="WARN"):
            _try_unmap_regions(
                mock.sentinel.context,
                [mock.sentinel.region_id1],
                mock.sentinel.endpoint_id
            )
            mock_delete_endpoint_region_mapping.assert_called_once_with(
                mock.sentinel.context,
                mock.sentinel.endpoint_id,
                mock.sentinel.region_id1
            )

    @mock.patch.object(api, '_try_unmap_regions')
    @mock.patch.object(api, '_update_sqlalchemy_object_fields')
    @mock.patch.object(api, 'add_endpoint_region_mapping')
    @mock.patch.object(api, 'get_region_mappings_for_endpoint')
    @mock.patch.object(api, 'get_region')
    @mock.patch.object(api, 'get_endpoint')
    def test_update_endpoint(
        self,
        mock_get_endpoint,
        mock_get_region,
        mock_get_region_mappings_for_endpoint,
        mock_add_endpoint_region_mapping,
        mock_update_sqlalchemy_object_fields,
        mock_try_unmap_regions
    ):
        region1 = mock.Mock()
        region2 = mock.Mock()
        region1.region_id = "region_id1"
        region2.region_id = "region_id2"
        updated_values = {
            "mapped_regions": ["region_id2", "region_id3", "region_id4"]}
        mock_get_region.return_value = region1
        mock_get_region_mappings_for_endpoint.return_value = [region1, region2]

        update_endpoint = testutils.get_wrapped_function(api.update_endpoint)

        with self.assertLogs('coriolis.db.api', level="DEBUG"):
            update_endpoint(
                mock.sentinel.context,
                mock.sentinel.endpoint_id,
                updated_values
            )
            mock_get_endpoint.assert_called_once_with(
                mock.sentinel.context, mock.sentinel.endpoint_id)
            mock_add_endpoint_region_mapping.assert_has_calls([
                mock.call(mock.sentinel.context, mock.ANY),
                mock.call(mock.sentinel.context, mock.ANY)
            ])
            mock_update_sqlalchemy_object_fields.assert_called_once_with(
                mock_get_endpoint.return_value,
                ["name", "description", "connection_info"],
                updated_values
            )
            mock_try_unmap_regions.assert_called_once_with(
                mock.sentinel.context,
                {region1.region_id},
                mock.sentinel.endpoint_id
            )

    @mock.patch.object(api, 'get_endpoint')
    def test_update_endpoint_not_found(self, mock_get_endpoint):
        mock_get_endpoint.return_value = None

        update_endpoint = testutils.get_wrapped_function(api.update_endpoint)

        self.assertRaises(
            exception.NotFound,
            update_endpoint,
            mock.sentinel.context,
            mock.sentinel.endpoint_id,
            mock.sentinel.updated_values
        )
        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id)

    @mock.patch.object(api, 'get_endpoint')
    def test_update_endpoint_not_dict(self, mock_get_endpoint):
        update_endpoint = testutils.get_wrapped_function(api.update_endpoint)

        self.assertRaises(
            exception.InvalidInput,
            update_endpoint,
            mock.sentinel.context,
            mock.sentinel.endpoint_id,
            mock.sentinel.updated_values
        )
        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id)

    @mock.patch.object(api, 'add_endpoint_region_mapping')
    @mock.patch.object(api, 'get_region_mappings_for_endpoint')
    @mock.patch.object(api, 'get_region')
    @mock.patch.object(api, 'get_endpoint')
    def test_update_endpoint_region_not_found(
        self,
        mock_get_endpoint,
        mock_get_region,
        mock_get_region_mappings_for_endpoint,
        mock_add_endpoint_region_mapping
    ):
        region1 = mock.Mock()
        region2 = mock.Mock()
        region1.region_id = "region_id1"
        region2.region_id = "region_id2"
        updated_values = {
            "mapped_regions": ["region_id2", "region_id3", "region_id4"]}
        mock_get_region.return_value = None
        mock_get_region_mappings_for_endpoint.return_value = [region1, region2]

        update_endpoint = testutils.get_wrapped_function(api.update_endpoint)

        self.assertRaises(
            exception.NotFound,
            update_endpoint,
            mock.sentinel.context,
            mock.sentinel.endpoint_id,
            updated_values
        )
        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id)
        mock_add_endpoint_region_mapping.assert_not_called()

    @mock.patch.object(api, '_try_unmap_regions')
    @mock.patch.object(api, 'add_endpoint_region_mapping')
    @mock.patch.object(api, 'get_region_mappings_for_endpoint')
    @mock.patch.object(api, 'get_region')
    @mock.patch.object(api, 'get_endpoint')
    def test_update_endpoint_add_mapping_fails(
        self,
        mock_get_endpoint,
        mock_get_region,
        mock_get_region_mappings_for_endpoint,
        mock_add_endpoint_region_mapping,
        mock_try_unmap_regions
    ):
        region1 = mock.Mock()
        region2 = mock.Mock()
        region1.region_id = "region_id1"
        region2.region_id = "region_id2"
        updated_values = {
            "mapped_regions": ["region_id2", "region_id3", "region_id4"]}
        mock_get_region.return_value = region1
        mock_get_region_mappings_for_endpoint.return_value = [region1, region2]
        mock_add_endpoint_region_mapping.side_effect = [
            None, CoriolisTestException()]

        update_endpoint = testutils.get_wrapped_function(api.update_endpoint)

        with self.assertLogs('coriolis.db.api', level=0):
            self.assertRaises(
                CoriolisTestException,
                update_endpoint,
                mock.sentinel.context,
                mock.sentinel.endpoint_id,
                updated_values
            )
            mock_get_endpoint.assert_called_once_with(
                mock.sentinel.context, mock.sentinel.endpoint_id)
            mock_add_endpoint_region_mapping.assert_has_calls([
                mock.call(mock.sentinel.context, mock.ANY),
                mock.call(mock.sentinel.context, mock.ANY)
            ])
            try:
                mock_try_unmap_regions.assert_called_once_with(
                    mock.sentinel.context,
                    ["region_id3"],
                    mock.sentinel.endpoint_id
                )
            except AssertionError:
                mock_try_unmap_regions.assert_called_once_with(
                    mock.sentinel.context,
                    ["region_id4"],
                    mock.sentinel.endpoint_id
                )

    @mock.patch.object(api, '_try_unmap_regions')
    @mock.patch.object(api, '_update_sqlalchemy_object_fields')
    @mock.patch.object(api, 'add_endpoint_region_mapping')
    @mock.patch.object(api, 'get_region_mappings_for_endpoint')
    @mock.patch.object(api, 'get_region')
    @mock.patch.object(api, 'get_endpoint')
    def test_update_endpoint_update_sqlalchemy_fails(
        self,
        mock_get_endpoint,
        mock_get_region,
        mock_get_region_mappings_for_endpoint,
        mock_add_endpoint_region_mapping,
        mock_update_sqlalchemy_object_fields,
        mock_try_unmap_regions
    ):
        region1 = mock.Mock()
        region2 = mock.Mock()
        region1.region_id = "region_id1"
        region2.region_id = "region_id2"
        updated_values = {
            "mapped_regions": ["region_id2", "region_id3", "region_id4"]}
        mock_get_region.return_value = region1
        mock_get_region_mappings_for_endpoint.return_value = [region1, region2]
        mock_update_sqlalchemy_object_fields.side_effect = \
            CoriolisTestException()

        update_endpoint = testutils.get_wrapped_function(api.update_endpoint)

        with self.assertLogs('coriolis.db.api', level="WARN"):
            self.assertRaises(
                CoriolisTestException,
                update_endpoint,
                mock.sentinel.context,
                mock.sentinel.endpoint_id,
                updated_values
            )
            mock_get_endpoint.assert_called_once_with(
                mock.sentinel.context, mock.sentinel.endpoint_id)
            mock_add_endpoint_region_mapping.assert_has_calls([
                mock.call(mock.sentinel.context, mock.ANY),
                mock.call(mock.sentinel.context, mock.ANY)
            ])
            mock_update_sqlalchemy_object_fields.assert_called_once_with(
                mock_get_endpoint.return_value,
                ["name", "description", "connection_info"],
                updated_values
            )
            mock_try_unmap_regions.assert_called_once_with(
                mock.sentinel.context,
                [mock.ANY, mock.ANY],
                mock.sentinel.endpoint_id
            )

    @mock.patch.object(api, '_soft_delete_aware_query')
    @mock.patch.object(api, 'is_user_context')
    def test_get_endpoints(
        self,
        mock_is_user_context,
        mock_soft_delete_aware_query
    ):
        context = mock.Mock()
        mock_soft_delete_aware_query.return_value = mock.Mock()
        mock_is_user_context.return_value = True
        get_endpoints = testutils.get_wrapped_function(api.get_endpoints)

        result = get_endpoints(context)

        self.assertEqual(
            (mock_soft_delete_aware_query.return_value.options.return_value.
             filter.return_value.filter.return_value.all.return_value),
            result
        )

        mock_is_user_context.return_value = False

        result = get_endpoints(context)

        self.assertEqual(
            (mock_soft_delete_aware_query.return_value.options.return_value.
             filter.return_value.all.return_value),
            result
        )

    @mock.patch.object(api, '_soft_delete_aware_query')
    @mock.patch.object(api, 'is_user_context')
    def test_get_endpoint(
        self,
        mock_is_user_context,
        mock_soft_delete_aware_query
    ):
        context = mock.Mock()
        mock_soft_delete_aware_query.return_value = mock.Mock()
        mock_is_user_context.return_value = True
        get_endpoint = testutils.get_wrapped_function(api.get_endpoint)

        result = get_endpoint(context, mock.sentinel.endpoint_id)

        self.assertEqual(
            (mock_soft_delete_aware_query.return_value.options.return_value.
             filter.return_value.filter.return_value.first.return_value),
            result
        )

        mock_is_user_context.return_value = False

        result = get_endpoint(context, mock.sentinel.endpoint_id)

        self.assertEqual(
            (mock_soft_delete_aware_query.return_value.options.return_value.
             filter.return_value.first.return_value),
            result
        )

    @mock.patch.object(api, '_session')
    def test_add_endpoint(
        self,
        mock_session
    ):
        context = mock.Mock()
        endpoint = mock.Mock()
        add_endpoint = testutils.get_wrapped_function(api.add_endpoint)

        add_endpoint(context, endpoint)

        mock_session.assert_called_once_with(context)
        mock_session.return_value.add.assert_called_once_with(endpoint)

    @mock.patch.object(api, 'delete_endpoint_region_mapping')
    @mock.patch.object(api, '_soft_delete_aware_query')
    @mock.patch.object(api, 'is_user_context')
    @mock.patch.object(api, 'get_endpoint')
    def test_delete_endpoint(
        self,
        mock_get_endpoint,
        mock_is_user_context,
        mock_soft_delete_aware_query,
        mock_delete_endpoint_region_mapping
    ):
        context = mock.Mock()
        endpoint = mock.Mock()
        region1 = mock.Mock()
        region2 = mock.Mock()
        endpoint.mapped_regions = [region1, region2]

        mock_get_endpoint.return_value = endpoint
        mock_is_user_context.return_value = True
        (mock_soft_delete_aware_query.return_value.filter_by.return_value.
         soft_delete.return_value) = 1
        delete_endpoint = testutils.get_wrapped_function(api.delete_endpoint)

        delete_endpoint(context, mock.sentinel.endpoint_id)

        mock_get_endpoint.assert_called_once_with(
            context, mock.sentinel.endpoint_id)
        (mock_soft_delete_aware_query.return_value.filter_by.
         assert_called_once_with)(
            id=mock.sentinel.endpoint_id, project_id=context.project_id)
        mock_delete_endpoint_region_mapping.assert_has_calls([
            mock.call(context, mock.sentinel.endpoint_id, region1.id),
            mock.call(context, mock.sentinel.endpoint_id, region2.id),
        ])

    @mock.patch.object(api, 'delete_endpoint_region_mapping')
    @mock.patch.object(api, '_soft_delete_aware_query')
    @mock.patch.object(api, 'is_user_context')
    @mock.patch.object(api, 'get_endpoint')
    def test_delete_endpoint_not_found(
        self,
        mock_get_endpoint,
        mock_is_user_context,
        mock_soft_delete_aware_query,
        mock_delete_endpoint_region_mapping
    ):
        context = mock.Mock()
        endpoint = mock.Mock()
        region1 = mock.Mock()
        region2 = mock.Mock()
        endpoint.mapped_regions = [region1, region2]

        mock_get_endpoint.return_value = endpoint
        mock_is_user_context.return_value = False
        (mock_soft_delete_aware_query.return_value.filter_by.return_value.
         soft_delete.return_value) = 0
        delete_endpoint = testutils.get_wrapped_function(api.delete_endpoint)

        self.assertRaises(
            exception.NotFound,
            delete_endpoint,
            context,
            mock.sentinel.endpoint_id
        )

        mock_get_endpoint.assert_called_once_with(
            context, mock.sentinel.endpoint_id)
        (mock_soft_delete_aware_query.return_value.filter_by.
         assert_called_once_with)(id=mock.sentinel.endpoint_id)
        mock_delete_endpoint_region_mapping.assert_not_called()

    @mock.patch.object(api, '_get_tasks_with_details_options')
    @mock.patch.object(api, '_soft_delete_aware_query')
    @mock.patch.object(api, 'is_user_context')
    def test_get_replica_tasks_executions(
        self,
        mock_is_user_context,
        mock_soft_delete_aware_query,
        mock_get_tasks_with_details_options
    ):
        context = mock.Mock()
        q = mock.Mock()
        db_result1 = mock.Mock()
        db_result2 = mock.Mock()
        db_result = [db_result1, db_result2]

        mock_is_user_context.return_value = True
        q.filter.return_value.filter.return_value.all.return_value = db_result
        mock_get_tasks_with_details_options.return_value = q
        get_replica_tasks_executions = testutils.get_wrapped_function(
            api.get_replica_tasks_executions)

        result = get_replica_tasks_executions(
            context,
            mock.sentinel.replica_id,
            include_tasks=True,
            include_task_info=True,
            to_dict=True
        )

        self.assertEqual(
            [db_result1.to_dict.return_value, db_result2.to_dict.return_value],
            result
        )
        mock_soft_delete_aware_query.assert_called_once_with(context, mock.ANY)

        (q.join.return_value.filter.return_value.filter.return_value.all.
         return_value) = db_result
        mock_soft_delete_aware_query.return_value = q

        result = get_replica_tasks_executions(
            context,
            mock.sentinel.replica_id,
            include_tasks=False,
            include_task_info=False,
            to_dict=False
        )

        self.assertEqual(
            db_result,
            result
        )

    @mock.patch.object(api, '_get_tasks_with_details_options')
    @mock.patch.object(api, '_soft_delete_aware_query')
    @mock.patch.object(api, 'is_user_context')
    def test_get_replica_tasks_execution(
        self,
        mock_is_user_context,
        mock_soft_delete_aware_query,
        mock_get_tasks_with_details_options
    ):
        context = mock.Mock()
        q = mock.Mock()
        db_result = mock.Mock()

        mock_is_user_context.return_value = True
        (q.filter.return_value.filter.return_value.first.
         return_value) = db_result
        mock_get_tasks_with_details_options.return_value = q
        get_replica_tasks_execution = testutils.get_wrapped_function(
            api.get_replica_tasks_execution)

        result = get_replica_tasks_execution(
            context,
            mock.sentinel.replica_id,
            mock.sentinel.execution_id,
            include_task_info=True,
            to_dict=True
        )

        self.assertEqual(
            db_result.to_dict.return_value,
            result
        )
        mock_soft_delete_aware_query.assert_called_once_with(context, mock.ANY)

        (q.join.return_value.filter.return_value.filter.return_value.all.
         return_value) = db_result
        mock_soft_delete_aware_query.return_value = q

        result = get_replica_tasks_execution(
            context,
            mock.sentinel.replica_id,
            mock.sentinel.execution_id,
            include_task_info=False,
            to_dict=False
        )

        self.assertEqual(
            db_result,
            result
        )

    @mock.patch.object(api, '_session')
    @mock.patch.object(api, '_model_query')
    @mock.patch.object(api, 'is_user_context')
    def test_add_replica_tasks_execution(
        self,
        mock_is_user_context,
        mock_model_query,
        mock_session
    ):
        context = mock.Mock()
        execution = mock.Mock()
        execution.action.project_id = mock.sentinel.project_id
        context.project_id = mock.sentinel.project_id
        mock_is_user_context.return_value = True
        (mock_model_query.return_value.filter.return_value.first.
         return_value) = [1]

        add_replica_tasks_execution = testutils.get_wrapped_function(
            api.add_replica_tasks_execution)

        add_replica_tasks_execution(context, execution)

        mock_session.return_value.add.assert_called_once_with(execution)
        self.assertEqual(
            2,
            execution.number
        )

    @mock.patch.object(api, 'is_user_context')
    def test_add_replica_tasks_execution_not_authorized(
        self,
        mock_is_user_context
    ):
        context = mock.Mock()
        execution = mock.Mock()
        execution.action.project_id = mock.sentinel.project_id
        context.project_id = mock.sentinel.other_project_id
        mock_is_user_context.return_value = True

        add_replica_tasks_execution = testutils.get_wrapped_function(
            api.add_replica_tasks_execution)

        self.assertRaises(
            exception.NotAuthorized,
            add_replica_tasks_execution,
            context,
            execution
        )

    @mock.patch.object(api, 'is_user_context')
    @mock.patch.object(api, '_soft_delete_aware_query')
    def test_delete_replica_tasks_execution(
        self,
        mock__soft_delete_aware_query,
        mock_is_user_context
    ):
        context = mock.Mock()
        q = mock.Mock()
        q.soft_delete.return_value = 1
        mock_is_user_context.return_value = True
        mock__soft_delete_aware_query.return_value.filter.return_value = q

        delete_replica_tasks_execution = testutils.get_wrapped_function(
            api.delete_replica_tasks_execution)

        delete_replica_tasks_execution(context, mock.sentinel.execution_id)
        mock_is_user_context.assert_called_once_with(context)

    @mock.patch.object(api, 'is_user_context')
    @mock.patch.object(api, '_soft_delete_aware_query')
    def test_delete_replica_tasks_execution_not_authorized(
        self,
        mock__soft_delete_aware_query,
        mock_is_user_context
    ):
        context = mock.Mock()
        q = mock.Mock()
        q.join.return_value.filter.return_value.first.return_value = False
        mock_is_user_context.return_value = True
        mock__soft_delete_aware_query.return_value.filter.return_value = q

        delete_replica_tasks_execution = testutils.get_wrapped_function(
            api.delete_replica_tasks_execution)

        self.assertRaises(
            exception.NotAuthorized,
            delete_replica_tasks_execution,
            context,
            mock.sentinel.execution_id
        )
        mock_is_user_context.assert_called_once_with(context)

    @mock.patch.object(api, 'is_user_context')
    @mock.patch.object(api, '_soft_delete_aware_query')
    def test_delete_replica_tasks_execution_not_found(
        self,
        mock__soft_delete_aware_query,
        mock_is_user_context
    ):
        context = mock.Mock()
        q = mock.Mock()
        q.soft_delete.return_value = 0
        mock_is_user_context.return_value = True
        mock__soft_delete_aware_query.return_value.filter.return_value = q

        delete_replica_tasks_execution = testutils.get_wrapped_function(
            api.delete_replica_tasks_execution)

        self.assertRaises(
            exception.NotFound,
            delete_replica_tasks_execution,
            context,
            mock.sentinel.execution_id
        )
        mock_is_user_context.assert_called_once_with(context)

    @mock.patch.object(api, '_get_replica_schedules_filter')
    def test_get_replica_schedules(
        self,
        mock_get_replica_schedules_filter,
    ):
        context = mock.Mock()

        get_replica_schedules = testutils.get_wrapped_function(
            api.get_replica_schedules)

        result = get_replica_schedules(
            context,
            replica_id=mock.sentinel.replica_id,
            expired=True
        )

        self.assertEqual(
            mock_get_replica_schedules_filter.return_value.all.return_value,
            result
        )
        mock_get_replica_schedules_filter.assert_called_once_with(
            context,
            replica_id=mock.sentinel.replica_id,
            expired=True
        )

    @mock.patch.object(api, '_get_replica_schedules_filter')
    def test_get_replica_schedule(
        self,
        mock_get_replica_schedule_filter,
    ):
        context = mock.Mock()

        get_replica_schedule = testutils.get_wrapped_function(
            api.get_replica_schedule)

        result = get_replica_schedule(
            context,
            replica_id=mock.sentinel.replica_id,
            schedule_id=mock.sentinel.schedule_id,
            expired=True
        )

        self.assertEqual(
            mock_get_replica_schedule_filter.return_value.first.return_value,
            result
        )
        mock_get_replica_schedule_filter.assert_called_once_with(
            context,
            replica_id=mock.sentinel.replica_id,
            schedule_id=mock.sentinel.schedule_id,
            expired=True
        )

    @mock.patch.object(api, 'get_replica_schedule')
    def test_update_replica_schedule(
        self,
        mock_get_replica_schedule
    ):
        pre_update_callable = mock.Mock()
        post_update_callable = mock.Mock()
        updated_values = {"enabled": True}
        update_replica_schedule = testutils.get_wrapped_function(
            api.update_replica_schedule)

        update_replica_schedule(
            mock.sentinel.context,
            mock.sentinel.replica_id,
            mock.sentinel.schedule_id,
            updated_values,
            pre_update_callable=pre_update_callable,
            post_update_callable=post_update_callable
        )

        mock_get_replica_schedule.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.replica_id,
            mock.sentinel.schedule_id
        )
        pre_update_callable.assert_called_once_with(
            schedule=mock_get_replica_schedule.return_value)
        post_update_callable.assert_called_once_with(
            mock.sentinel.context, mock_get_replica_schedule.return_value)
        self.assertEqual(
            mock_get_replica_schedule.return_value.enabled,
            True
        )

    @mock.patch.object(api, 'is_user_context')
    @mock.patch.object(api, '_soft_delete_aware_query')
    def test_delete_replica_schedule(
        self,
        mock_soft_delete_aware_query,
        mock_is_user_context
    ):
        context = mock.Mock()
        q = mock.Mock()
        pre_delete_callable = mock.Mock()
        post_delete_callable = mock.Mock()
        q.soft_delete.return_value = 1
        mock_soft_delete_aware_query.return_value.filter.return_value = q

        delete_replica_schedule = testutils.get_wrapped_function(
            api.delete_replica_schedule)

        delete_replica_schedule(
            context,
            replica_id=mock.sentinel.replica_id,
            schedule_id=mock.sentinel.schedule_id,
            pre_delete_callable=pre_delete_callable,
            post_delete_callable=post_delete_callable
        )

        mock_is_user_context.assert_called_once_with(context)
        mock_soft_delete_aware_query.assert_called_once_with(
            context, mock.ANY)
        pre_delete_callable.assert_called_once_with(
            context, q.first.return_value)
        post_delete_callable.assert_called_once_with(
            context, q.first.return_value)

    @mock.patch.object(api, 'is_user_context')
    @mock.patch.object(api, '_soft_delete_aware_query')
    def test_delete_replica_schedule_not_found(
        self,
        mock_soft_delete_aware_query,
        mock_is_user_context
    ):
        context = mock.Mock()
        q = mock.Mock()
        pre_delete_callable = mock.Mock()
        post_delete_callable = mock.Mock()
        q.first.return_value = None
        mock_soft_delete_aware_query.return_value.filter.return_value = q

        delete_replica_schedule = testutils.get_wrapped_function(
            api.delete_replica_schedule)

        self.assertRaises(
            exception.NotFound,
            delete_replica_schedule,
            context,
            replica_id=mock.sentinel.replica_id,
            schedule_id=mock.sentinel.schedule_id,
            pre_delete_callable=pre_delete_callable,
            post_delete_callable=post_delete_callable
        )

        mock_soft_delete_aware_query.assert_called_once_with(
            context, mock.ANY)
        mock_is_user_context.assert_not_called()

    @mock.patch.object(api, 'is_user_context')
    @mock.patch.object(api, '_soft_delete_aware_query')
    def test_delete_replica_schedule_delete_not_found(
        self,
        mock_soft_delete_aware_query,
        mock_is_user_context
    ):
        context = mock.Mock()
        q = mock.Mock()
        q.soft_delete.return_value = 0
        mock_soft_delete_aware_query.return_value.filter.return_value = q

        delete_replica_schedule = testutils.get_wrapped_function(
            api.delete_replica_schedule)

        self.assertRaises(
            exception.NotFound,
            delete_replica_schedule,
            context,
            replica_id=mock.sentinel.replica_id,
            schedule_id=mock.sentinel.schedule_id,
            pre_delete_callable=None,
            post_delete_callable=None
        )

        mock_soft_delete_aware_query.assert_called_once_with(
            context, mock.ANY)
        mock_is_user_context.assert_called_once_with(context)

    @mock.patch.object(api, 'is_user_context')
    @mock.patch.object(api, '_soft_delete_aware_query')
    def test_delete_replica_schedule_not_authorized(
        self,
        mock_soft_delete_aware_query,
        mock_is_user_context
    ):
        context = mock.Mock()
        q = mock.Mock()
        q.join.return_value.filter.return_value.first.return_value = None
        mock_soft_delete_aware_query.return_value.filter.return_value = q

        delete_replica_schedule = testutils.get_wrapped_function(
            api.delete_replica_schedule)

        self.assertRaises(
            exception.NotAuthorized,
            delete_replica_schedule,
            context,
            replica_id=mock.sentinel.replica_id,
            schedule_id=mock.sentinel.schedule_id,
            pre_delete_callable=None,
            post_delete_callable=None
        )

        mock_soft_delete_aware_query.assert_called_once_with(
            context, mock.ANY)
        mock_is_user_context.assert_called_once_with(context)

    @mock.patch.object(api, '_session')
    def test_add_replica_schedule(
        self,
        mock_session
    ):
        context = mock.Mock()
        schedule = mock.Mock()
        schedule.replica.project_id = mock.sentinel.project_id
        context.project_id = mock.sentinel.project_id
        post_create_callable = mock.Mock()
        add_replica_schedule = testutils.get_wrapped_function(
            api.add_replica_schedule)

        add_replica_schedule(
            context, schedule, post_create_callable=post_create_callable)

        mock_session.assert_called_once_with(context)
        mock_session.return_value.add.assert_called_once_with(schedule)
        post_create_callable.assert_called_once_with(context, schedule)

    @mock.patch.object(api, '_session')
    def test_add_replica_schedule_not_authorized(
        self,
        mock_session
    ):
        context = mock.Mock()
        schedule = mock.Mock()
        schedule.replica.project_id = mock.sentinel.project_id
        context.project_id = mock.sentinel.other_project_id
        post_create_callable = mock.Mock()
        add_replica_schedule = testutils.get_wrapped_function(
            api.add_replica_schedule)

        self.assertRaises(
            exception.NotAuthorized,
            add_replica_schedule,
            context,
            schedule,
            post_create_callable=post_create_callable
        )

        mock_session.assert_not_called()

    def test_get_replica_with_tasks_executions_options(self):
        mock_q = mock.Mock()
        _get_replica_with_tasks_executions_options = \
            testutils.get_wrapped_function(
                api._get_replica_with_tasks_executions_options)

        result = _get_replica_with_tasks_executions_options(mock_q)

        self.assertEqual(
            mock_q.options.return_value,
            result
        )

    @mock.patch.object(api, '_get_replica_with_tasks_executions_options')
    @mock.patch.object(api, '_soft_delete_aware_query')
    @mock.patch.object(api, 'is_user_context')
    def test_get_replicas(
        self,
        mock_is_user_context,
        mock_soft_delete_aware_query,
        mock_get_replica_with_tasks_executions_options
    ):
        context = mock.Mock()
        q = mock.Mock()
        db_result1 = mock.Mock()
        db_result2 = mock.Mock()
        db_result = [db_result1, db_result2]

        mock_is_user_context.return_value = True
        (q.options.return_value.filter.return_value.filter.return_value.all.
         return_value) = db_result
        mock_get_replica_with_tasks_executions_options.return_value = q
        get_replicas = testutils.get_wrapped_function(api.get_replicas)

        result = get_replicas(
            context,
            include_tasks_executions=True,
            include_task_info=True,
            to_dict=True
        )

        self.assertEqual(
            [db_result1.to_dict.return_value, db_result2.to_dict.return_value],
            result
        )
        mock_soft_delete_aware_query.assert_called_once_with(context, mock.ANY)

        mock_is_user_context.return_value = False
        q.filter.return_value.all.return_value = db_result
        mock_soft_delete_aware_query.return_value = q

        result = get_replicas(
            context,
            include_tasks_executions=False,
            include_task_info=False,
            to_dict=False
        )

        self.assertEqual(
            db_result,
            result
        )

    @mock.patch.object(api, '_get_replica_with_tasks_executions_options')
    @mock.patch.object(api, '_soft_delete_aware_query')
    @mock.patch.object(api, 'is_user_context')
    def test_get_replica(
        self,
        mock_is_user_context,
        mock_soft_delete_aware_query,
        mock_get_replica_with_tasks_executions_options
    ):
        context = mock.Mock()
        q = mock.Mock()
        replica = mock.Mock()

        mock_is_user_context.return_value = True
        (q.options.return_value.filter.return_value.filter.return_value.first.
         return_value) = replica
        mock_get_replica_with_tasks_executions_options.return_value = q
        get_replica = testutils.get_wrapped_function(api.get_replica)

        result = get_replica(
            context,
            mock.sentinel.replica_id,
            include_task_info=True,
            to_dict=True
        )

        self.assertEqual(
            replica.to_dict.return_value,
            result
        )
        mock_soft_delete_aware_query.assert_called_once_with(context, mock.ANY)

        mock_is_user_context.return_value = False
        q.filter.return_value.first.return_value = replica

        result = get_replica(
            context,
            mock.sentinel.replica_id,
            include_task_info=False,
            to_dict=False
        )

        self.assertEqual(
            replica,
            result
        )

    @mock.patch.object(api, '_soft_delete_aware_query')
    def test_get_endpoint_replicas_count(
        self,
        mock_soft_delete_aware_query
    ):
        context = mock.Mock()
        (mock_soft_delete_aware_query.return_value.filter_by.return_value.
         count.return_value) = 1
        get_endpoint_replicas_count = testutils.get_wrapped_function(
            api.get_endpoint_replicas_count)

        result = get_endpoint_replicas_count(
            context, mock.sentinel.endpoint_id)

        self.assertEqual(
            2,
            result
        )

    @mock.patch.object(api, '_session')
    def test_add_replica(
        self,
        mock_session
    ):
        context = mock.Mock()
        replica = mock.Mock()
        add_replica = testutils.get_wrapped_function(api.add_replica)

        add_replica(context, replica)

        mock_session.assert_called_once_with(context)
        mock_session.return_value.add.assert_called_once_with(replica)

    @mock.patch.object(api, 'is_user_context')
    @mock.patch.object(api, '_soft_delete_aware_query')
    def test_delete_transfer_action(
        self,
        mock_soft_delete_aware_query,
        mock_is_user_context
    ):
        context = mock.Mock()
        mock_is_user_context.return_value = True
        (mock_soft_delete_aware_query.return_value.filter_by.return_value.
         soft_delete.return_value) = 1

        _delete_transfer_action = testutils.get_wrapped_function(
            api._delete_transfer_action)

        _delete_transfer_action(
            context,
            mock.sentinel.cls,
            mock.sentinel.id
        )

        mock_is_user_context.assert_called_once_with(context)
        mock_soft_delete_aware_query.assert_has_calls([
            mock.call(context, mock.sentinel.cls),
            mock.call(context, mock.ANY)
        ], any_order=True)

    @mock.patch.object(api, 'is_user_context')
    @mock.patch.object(api, '_soft_delete_aware_query')
    def test_delete_transfer_action_not_found(
        self,
        mock_soft_delete_aware_query,
        mock_is_user_context
    ):
        context = mock.Mock()
        mock_is_user_context.return_value = True
        (mock_soft_delete_aware_query.return_value.filter_by.return_value.
         soft_delete.return_value) = 0

        _delete_transfer_action = testutils.get_wrapped_function(
            api._delete_transfer_action)

        self.assertRaises(
            exception.NotFound,
            _delete_transfer_action,
            context,
            mock.sentinel.cls,
            mock.sentinel.id
        )

        mock_is_user_context.assert_called_once_with(context)
        mock_soft_delete_aware_query.assert_called_once_with(
            context, mock.sentinel.cls)

    @mock.patch.object(api, '_delete_transfer_action')
    def test_delete_replica(
        self,
        mock_delete_transfer_action
    ):
        context = mock.Mock()

        delete_replica = testutils.get_wrapped_function(
            api.delete_replica)

        delete_replica(
            context,
            mock.sentinel.replica_id
        )

        mock_delete_transfer_action.assert_called_once_with(
            context,
            mock.ANY,
            mock.sentinel.replica_id
        )

    @mock.patch.object(api, 'is_user_context')
    @mock.patch.object(api, '_soft_delete_aware_query')
    def test_get_replica_migrations(
        self,
        mock_soft_delete_aware_query,
        mock_is_user_context
    ):
        context = mock.Mock()
        mock_is_user_context.return_value = True

        get_replica_migrations = testutils.get_wrapped_function(
            api.get_replica_migrations)

        result = get_replica_migrations(
            context,
            mock.sentinel.replica_id
        )

        self.assertEqual(
            (mock_soft_delete_aware_query.return_value.join.return_value.
             options.return_value.filter.return_value.filter.return_value.
             all.return_value),
            result
        )
        mock_is_user_context.assert_called_once_with(context)
        mock_soft_delete_aware_query.assert_called_once_with(context, mock.ANY)

    @mock.patch.object(api, '_get_migration_task_query_options')
    @mock.patch.object(api, '_soft_delete_aware_query')
    @mock.patch.object(api, 'is_user_context')
    def test_get_migrations(
        self,
        mock_is_user_context,
        mock_soft_delete_aware_query,
        mock_get_migration_task_query_options
    ):
        context = mock.Mock()
        q = mock.Mock()
        db_result1 = mock.Mock()
        db_result2 = mock.Mock()
        db_result = [db_result1, db_result2]

        mock_is_user_context.return_value = True
        q.options.return_value.filter_by.return_value.all.return_value = \
            db_result
        mock_get_migration_task_query_options.return_value = q
        get_migrations = testutils.get_wrapped_function(
            api.get_migrations)

        result = get_migrations(
            context,
            include_tasks=True,
            include_task_info=True,
            to_dict=True
        )

        self.assertEqual(
            [db_result1.to_dict.return_value, db_result2.to_dict.return_value],
            result
        )
        q.options.return_value.filter_by.assert_called_once_with(
            project_id=context.project_id)
        mock_soft_delete_aware_query.assert_called_once_with(context, mock.ANY)
        mock_get_migration_task_query_options.assert_called_once_with(
            mock_soft_delete_aware_query.return_value)

        q.reset_mock()
        mock_get_migration_task_query_options.reset_mock()
        mock_soft_delete_aware_query.return_value = q
        mock_is_user_context.return_value = False

        result = get_migrations(
            context,
            include_tasks=False,
            include_task_info=False,
            to_dict=False
        )

        self.assertEqual(
            db_result,
            result
        )
        q.options.return_value.filter_by.assert_called_once_with()
        mock_get_migration_task_query_options.assert_not_called()

    def test_get_tasks_with_details_options(self):
        query = mock.Mock()

        result = api._get_tasks_with_details_options(query)

        self.assertEqual(
            (query.options.return_value.options.return_value.options.
             return_value),
            result
        )

    def test_get_migration_task_query_options(self):
        query = mock.Mock()

        result = api._get_migration_task_query_options(query)

        self.assertEqual(
            (query.options.return_value.options.return_value.options.
             return_value),
            result
        )

    @mock.patch.object(api, '_get_migration_task_query_options')
    @mock.patch.object(api, '_soft_delete_aware_query')
    @mock.patch.object(api, 'is_user_context')
    def test_get_migration(
        self,
        mock_is_user_context,
        mock_soft_delete_aware_query,
        mock_get_migration_task_query_options
    ):
        context = mock.Mock()
        q = mock.Mock()
        db_result = mock.Mock()

        mock_is_user_context.return_value = True
        q.options.return_value.filter_by.return_value.first.return_value = \
            db_result
        mock_get_migration_task_query_options.return_value = q
        get_migration = testutils.get_wrapped_function(
            api.get_migration)

        result = get_migration(
            context,
            mock.sentinel.migration_id,
            include_task_info=True,
            to_dict=True
        )

        self.assertEqual(
            db_result.to_dict.return_value,
            result
        )
        q.options.return_value.filter_by.assert_called_once_with(
            id=mock.sentinel.migration_id, project_id=context.project_id)
        mock_soft_delete_aware_query.assert_called_once_with(context, mock.ANY)
        mock_get_migration_task_query_options.assert_called_once_with(
            mock_soft_delete_aware_query.return_value)

        q.reset_mock()
        mock_is_user_context.return_value = False
        q.filter_by.return_value.first.return_value = \
            db_result

        result = get_migration(
            context,
            mock.sentinel.migration_id,
            include_task_info=False,
            to_dict=False
        )

        self.assertEqual(
            db_result,
            result
        )
        q.filter_by.assert_called_once_with(id=mock.sentinel.migration_id)

    @mock.patch.object(api, '_session')
    def test_add_migration(
        self,
        mock_session
    ):
        context = mock.Mock()
        migration = mock.Mock()
        add_migration = testutils.get_wrapped_function(api.add_migration)

        add_migration(context, migration)

        mock_session.assert_called_once_with(context)
        mock_session.return_value.add.assert_called_once_with(migration)

    @mock.patch.object(api, '_delete_transfer_action')
    def test_delete_migration(
        self,
        mock_delete_transfer_action
    ):
        context = mock.Mock()

        delete_migration = testutils.get_wrapped_function(
            api.delete_migration)

        delete_migration(
            context,
            mock.sentinel.migration_id
        )

        mock_delete_transfer_action.assert_called_once_with(
            context,
            mock.ANY,
            mock.sentinel.migration_id
        )

    @mock.patch.object(api, 'set_action_last_execution_status')
    @mock.patch.object(api, '_soft_delete_aware_query')
    @mock.patch.object(api, 'is_user_context')
    def test_set_execution_status(
        self,
        mock_is_user_context,
        mock_soft_delete_aware_query,
        mock_set_action_last_execution_status
    ):
        context = mock.Mock()
        execution = mock.Mock()
        mock_is_user_context.return_value = True
        mock_soft_delete_aware_query.return_value = execution
        set_execution_status = testutils.get_wrapped_function(
            api.set_execution_status)

        result = set_execution_status(
            context,
            mock.sentinel.execution_id,
            mock.sentinel.status,
            update_action_status=True,
        )

        self.assertEqual(
            mock.sentinel.status,
            result.status
        )
        mock_soft_delete_aware_query.assert_called_once_with(context, mock.ANY)
        mock_set_action_last_execution_status.assert_called_once_with(
            context,
            (execution.join.return_value.filter.return_value.filter.
             return_value.first.return_value.action_id),
            mock.sentinel.status
        )

        mock_set_action_last_execution_status.reset_mock()
        mock_is_user_context.return_value = False

        result = set_execution_status(
            context,
            mock.sentinel.execution_id,
            mock.sentinel.status,
            update_action_status=False,
        )

        self.assertEqual(
            mock.sentinel.status,
            result.status
        )
        mock_set_action_last_execution_status.assert_not_called()

    @mock.patch.object(api, '_soft_delete_aware_query')
    @mock.patch.object(api, 'is_user_context')
    def test_set_execution_status_not_found(
        self,
        mock_is_user_context,
        mock_soft_delete_aware_query
    ):
        context = mock.Mock()
        execution = mock.Mock()
        mock_is_user_context.return_value = True
        mock_soft_delete_aware_query.return_value = execution
        (execution.join.return_value.filter.return_value.filter.return_value.
         first.return_value) = None
        set_execution_status = testutils.get_wrapped_function(
            api.set_execution_status)

        self.assertRaises(
            exception.NotFound,
            set_execution_status,
            context,
            mock.sentinel.execution_id,
            mock.sentinel.status,
            update_action_status=False
        )
        mock_soft_delete_aware_query.assert_called_once_with(context, mock.ANY)

    @mock.patch.object(api, '_soft_delete_aware_query')
    @mock.patch.object(api, 'is_user_context')
    def test_get_action(
        self,
        mock_is_user_context,
        mock_soft_delete_aware_query
    ):
        context = mock.Mock()
        action = mock.Mock()
        mock_is_user_context.return_value = True
        mock_soft_delete_aware_query.return_value = action
        get_action = testutils.get_wrapped_function(api.get_action)

        result = get_action(
            context,
            mock.sentinel.action_id,
            include_task_info=True,
        )

        self.assertEqual(
            (action.options.return_value.filter.return_value.filter.
             return_value.first.return_value),
            result
        )
        mock_soft_delete_aware_query.assert_called_once_with(context, mock.ANY)

    @mock.patch.object(api, '_soft_delete_aware_query')
    @mock.patch.object(api, 'is_user_context')
    def test_get_action_not_found(
        self,
        mock_is_user_context,
        mock_soft_delete_aware_query
    ):
        context = mock.Mock()
        action = mock.Mock()
        mock_is_user_context.return_value = False
        mock_soft_delete_aware_query.return_value = action
        action.filter.return_value.first.return_value = None
        get_action = testutils.get_wrapped_function(api.get_action)

        self.assertRaises(
            exception.NotFound,
            get_action,
            context,
            mock.sentinel.action_id,
            include_task_info=False
        )

        mock_soft_delete_aware_query.assert_called_once_with(context, mock.ANY)

    @mock.patch.object(api, 'get_action')
    def test_set_action_last_execution_status(
        self,
        mock_get_action
    ):
        set_action_last_execution_status = testutils.get_wrapped_function(
            api.set_action_last_execution_status)

        set_action_last_execution_status(
            mock.sentinel.context,
            mock.sentinel.action_id,
            mock.sentinel.last_execution_status
        )

        mock_get_action.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.action_id)
        self.assertEqual(
            mock.sentinel.last_execution_status,
            mock_get_action.return_value.last_execution_status
        )

    @ddt.data(
        {
            "instance_name": "instance1",
            "instance_info": {"info": mock.sentinel.info},
            "new_instance_info": {"new_info": mock.sentinel.new_info},
            "expected_result": {
                'info': mock.sentinel.info,
                'new_info': mock.sentinel.new_info
            }
        },
        {
            "instance_name": "instance2",
            "instance_info": {"info": mock.sentinel.info},
            "new_instance_info": {"new_info": mock.sentinel.new_info},
            "expected_result": {
                'new_info': mock.sentinel.new_info
            }
        },
        {
            "instance_name": "instance1",
            "instance_info": {"info": mock.sentinel.info},
            "new_instance_info": {"info": mock.sentinel.new_info},
            "expected_result": {
                'info': mock.sentinel.new_info
            }
        },
        {
            "instance_name": "instance1",
            "instance_info": {"info": mock.sentinel.info},
            "new_instance_info": None,
            "expected_result": {
                'info': mock.sentinel.info
            }
        }
    )
    @mock.patch.object(api, 'get_action')
    def test_update_transfer_action_info_for_instance(
        self,
        data,
        mock_get_action
    ):
        action = mock.Mock()
        action_info = {
            "instance1": data["instance_info"]
        }
        action.info = action_info
        mock_get_action.return_value = action
        update_transfer_action_info_for_instance = \
            testutils.get_wrapped_function(
                api.update_transfer_action_info_for_instance)

        with self.assertLogs('coriolis.db.api', level="DEBUG"):
            result = update_transfer_action_info_for_instance(
                mock.sentinel.context,
                mock.sentinel.action_id,
                data["instance_name"],
                data["new_instance_info"]
            )

        self.assertEqual(
            data["expected_result"],
            result
        )
