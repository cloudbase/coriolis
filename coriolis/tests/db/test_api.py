# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.
import datetime
from unittest import mock
import uuid

import ddt
from oslo_utils import timeutils
import sqlalchemy

from coriolis import constants
from coriolis.db import api
from coriolis.db.sqlalchemy import api as sqlalchemy_api
from coriolis.db.sqlalchemy import models
from coriolis import exception
from coriolis.tests import test_base
from coriolis.tests import testutils

CONTEXT_MOCK = mock.MagicMock()
DEFAULT_INSTANCE = "instance1"
DEFAULT_USER_ID = "1"
DEFAULT_PROJECT_ID = "1"
DEFAULT_TASK_INFO = {DEFAULT_INSTANCE: {"volumes_info": []}}
DEFAULT_EXECUTION_STATUS = constants.EXECUTION_STATUS_RUNNING


def get_valid_endpoint(
        endpoint_id=None, user_id=DEFAULT_USER_ID,
        project_id=DEFAULT_PROJECT_ID, connection_info=None,
        endpoint_type="openstack", name="test_name",
        description="Endpoint Description"):
    if endpoint_id is None:
        endpoint_id = str(uuid.uuid4())
    if connection_info is None:
        connection_info = {"conn_info": {"secret": "info"}}

    endpoint = models.Endpoint()
    endpoint.id = endpoint_id
    endpoint.user_id = user_id
    endpoint.project_id = project_id
    endpoint.connection_info = connection_info
    endpoint.type = endpoint_type
    endpoint.name = name
    endpoint.description = description

    return endpoint


def create_valid_tasks_execution():
    valid_tasks_execution = models.TasksExecution()
    valid_tasks_execution.id = str(uuid.uuid4())
    valid_tasks_execution.status = DEFAULT_EXECUTION_STATUS
    valid_tasks_execution.type = constants.EXECUTION_TYPE_TRANSFER_EXECUTION
    valid_tasks_execution.number = 1

    valid_task = models.Task()
    valid_task.id = str(uuid.uuid4())
    valid_task.execution = valid_tasks_execution
    valid_task.instance = DEFAULT_INSTANCE
    valid_task.status = constants.TASK_STATUS_RUNNING
    valid_task.task_type = (
        constants.TASK_TYPE_VALIDATE_TRANSFER_SOURCE_INPUTS)
    valid_task.index = 1
    valid_task.on_error = False

    valid_progress_update = models.TaskProgressUpdate()
    valid_progress_update.id = str(uuid.uuid4())
    valid_progress_update.task = valid_task
    valid_progress_update.index = 1
    valid_progress_update.current_step = 0

    valid_task_event = models.TaskEvent()
    valid_task_event.id = str(uuid.uuid4())
    valid_task_event.task = valid_task
    valid_task_event.level = constants.TASK_EVENT_INFO
    valid_task_event.index = 1
    valid_task_event.message = "event message test"
    return valid_tasks_execution


class BaseDBAPITestCase(test_base.CoriolisBaseTestCase):

    valid_data = {
        "user_scope": {},
        "outer_scope": {}
    }

    @classmethod
    def setup_scoped_data(cls, region_id, project_id="1"):
        data = dict()
        valid_endpoint_source = get_valid_endpoint(
            endpoint_type='vmware', project_id=project_id)
        cls.session.add(valid_endpoint_source)
        data['source_endpoint'] = valid_endpoint_source
        valid_endpoint_destination = get_valid_endpoint(
            endpoint_type='openstack', project_id=project_id)
        cls.session.add(valid_endpoint_destination)
        data['destination_endpoint'] = valid_endpoint_destination

        valid_endpoint_region_mapping = models.EndpointRegionMapping()
        valid_endpoint_region_mapping.id = str(uuid.uuid4())
        valid_endpoint_region_mapping.endpoint_id = valid_endpoint_source.id
        valid_endpoint_region_mapping.region_id = region_id
        cls.session.add(valid_endpoint_region_mapping)
        data['endpoint_mapping'] = valid_endpoint_region_mapping

        valid_transfer = models.Transfer()
        valid_transfer.id = str(uuid.uuid4())
        valid_transfer.user_id = project_id
        valid_transfer.project_id = project_id
        valid_transfer.base_id = valid_transfer.id
        valid_transfer.scenario = constants.TRANSFER_SCENARIO_REPLICA
        valid_transfer.last_execution_status = DEFAULT_EXECUTION_STATUS
        valid_transfer.executions = []
        valid_transfer.instances = [DEFAULT_INSTANCE]
        valid_transfer.info = DEFAULT_TASK_INFO
        valid_transfer.origin_endpoint_id = valid_endpoint_source.id
        valid_transfer.destination_endpoint_id = valid_endpoint_destination.id
        cls.session.add(valid_transfer)
        data['transfer'] = valid_transfer

        valid_tasks_execution = create_valid_tasks_execution()
        valid_tasks_execution.action = valid_transfer
        cls.session.add(valid_tasks_execution)
        data['tasks_execution'] = valid_tasks_execution
        data['task'] = valid_tasks_execution.tasks[0]

        valid_transfer_schedule = models.TransferSchedule()
        valid_transfer_schedule.id = str(uuid.uuid4())
        valid_transfer_schedule.transfer = valid_transfer
        valid_transfer_schedule.schedule = {}
        valid_transfer_schedule.expiration_date = timeutils.utcnow()
        valid_transfer_schedule.enabled = True
        valid_transfer_schedule.shutdown_instance = False
        valid_transfer_schedule.trust_id = str(uuid.uuid4())
        cls.session.add(valid_transfer_schedule)
        data['transfer_schedule'] = valid_transfer_schedule

        valid_deployment = models.Deployment()
        valid_deployment.id = str(uuid.uuid4())
        valid_deployment.user_id = project_id
        valid_deployment.project_id = project_id
        valid_deployment.base_id = valid_deployment.id
        valid_deployment.last_execution_status = DEFAULT_EXECUTION_STATUS
        valid_deployment.instances = [DEFAULT_INSTANCE]
        valid_deployment.info = DEFAULT_TASK_INFO
        valid_deployment.origin_endpoint_id = valid_endpoint_source.id
        valid_deployment.destination_endpoint_id = (
            valid_endpoint_destination.id)
        valid_deployment.transfer = valid_transfer

        deployment_execution = create_valid_tasks_execution()
        deployment_execution.action = valid_deployment
        cls.session.add(valid_deployment)
        data['deployment'] = valid_deployment
        data['deployment_execution'] = deployment_execution

        return data

    @classmethod
    def setup_database_data(cls):
        cls.valid_region = models.Region()
        cls.valid_region.id = str(uuid.uuid4())
        cls.valid_region.name = "region1"
        cls.valid_region.enabled = True
        cls.session.add(cls.valid_region)

        cls.valid_data['user_scope'] = cls.setup_scoped_data(
            cls.valid_region.id)
        cls.valid_data['outer_scope'] = cls.setup_scoped_data(
            cls.valid_region.id, project_id="2")
        cls.session.commit()

    @classmethod
    def setUpClass(cls):
        super(BaseDBAPITestCase, cls).setUpClass()
        with mock.patch.object(sqlalchemy_api, 'CONF') as mock_conf:
            mock_conf.database.connection = "sqlite://"
            engine = api.get_engine()
            models.BASE.metadata.create_all(engine)
            cls.session = api.get_session()
            cls.setup_database_data()

    def setUp(self):
        super(BaseDBAPITestCase, self).setUp()
        self.context = CONTEXT_MOCK
        self.context.session = self.session
        self.context.show_deleted = False
        self.context.user = DEFAULT_USER_ID
        self.context.project_id = DEFAULT_PROJECT_ID
        self.context.is_admin = False

    def tearDown(self):
        self.context.reset_mock()
        super(BaseDBAPITestCase, self).tearDown()

    @classmethod
    def tearDownClass(cls):
        cls.session.rollback()
        cls.session.close()
        super(BaseDBAPITestCase, cls).tearDownClass()


@ddt.ddt
class DBAPITestCase(BaseDBAPITestCase):
    """Test suite for the common Coriolis DB API."""

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
        self.assertEqual(api._session(self.context), self.context.session)

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
        {"kwargs": None, "expected_result": False},
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
        kwargs = data.get('kwargs')
        if kwargs is None:
            context = None
        else:
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

    def test__soft_delete_aware_query_show_deleted_kwarg(self):
        valid_endpoint = get_valid_endpoint()
        self.session.add(valid_endpoint)
        self.session.commit()

        testutils.get_wrapped_function(api.delete_endpoint)(
            self.context, valid_endpoint.id)
        self.context.show_deleted = False
        result = api._soft_delete_aware_query(
            self.context, models.Endpoint, show_deleted=True).filter(
                models.Endpoint.id == valid_endpoint.id).first()
        self.assertEqual(result.id, valid_endpoint.id)
        self.assertIsNotNone(result.deleted_at)

    def test__soft_delete_aware_query_context_show_deleted(self):
        valid_endpoint = get_valid_endpoint()
        self.session.add(valid_endpoint)
        self.session.commit()

        testutils.get_wrapped_function(api.delete_endpoint)(
            self.context, valid_endpoint.id)
        self.context.show_deleted = True
        result = api._soft_delete_aware_query(
            self.context, models.Endpoint).filter(
                models.Endpoint.id == valid_endpoint.id).first()
        self.assertEqual(result.id, valid_endpoint.id)
        self.assertIsNotNone(result.deleted_at)


class EndpointDBAPITestCase(BaseDBAPITestCase):

    @classmethod
    def setUpClass(cls):
        super(EndpointDBAPITestCase, cls).setUpClass()
        cls.valid_endpoint_source = cls.valid_data['user_scope'].get(
            'source_endpoint')
        cls.valid_endpoint_region_mapping = cls.valid_data['user_scope'].get(
            'endpoint_mapping')
        cls.outer_scope_endpoint = cls.valid_data['outer_scope'].get(
            'source_endpoint')

    def test_get_endpoints(self):
        result = api.get_endpoints(self.context)
        self.assertIn(self.valid_endpoint_source, result)

    def test_get_endpoints_admin(self):
        self.context.is_admin = True
        result = api.get_endpoints(self.context)
        self.assertIn(self.outer_scope_endpoint, result)

    def test_get_endpoints_out_of_user_scope(self):
        result = api.get_endpoints(self.context)
        self.assertNotIn(self.outer_scope_endpoint, result)

    def test_get_endpoint(self):
        result = api.get_endpoint(self.context, self.valid_endpoint_source.id)
        self.assertEqual(result, self.valid_endpoint_source)

    def test_get_endpoint_admin_context(self):
        self.context.is_admin = True
        result = api.get_endpoint(self.context, self.outer_scope_endpoint.id)
        self.assertEqual(result, self.outer_scope_endpoint)

    def test_get_endpoint_out_of_user_scope(self):
        result = api.get_endpoint(self.context, self.outer_scope_endpoint.id)
        self.assertIsNone(result)

    def test_add_endpoint(self):
        self.context.user = "2"
        self.context.project_id = "2"
        new_endpoint_id = str(uuid.uuid4())
        new_endpoint = get_valid_endpoint(
            endpoint_id=new_endpoint_id,
            connection_info={"conn_info": {"new": "info"}},
            endpoint_type="vmware", name="new_endpoint",
            description="New Endpoint")
        api.add_endpoint(self.context, new_endpoint)
        result = api.get_endpoint(self.context, new_endpoint_id)
        self.assertEqual(result, new_endpoint)

    def test_update_endpoint_not_found(self):
        self.assertRaises(
            exception.NotFound, api.update_endpoint,
            self.context, "invalid_id", mock.ANY)

    def test_update_endpoint_invalid_values(self):
        self.assertRaises(
            exception.InvalidInput, api.update_endpoint,
            self.context, self.valid_endpoint_source.id, None)

    def test_update_endpoint_invalid_column(self):
        self.assertRaises(
            exception.Conflict, api.update_endpoint,
            self.context, self.valid_endpoint_source.id, {"type": "openstack"})

    def test_update_endpoint_region_not_found(self):
        self.assertRaises(
            exception.NotFound, api.update_endpoint, self.context,
            self.valid_endpoint_source.id,
            {"mapped_regions": ["invalid_region_id"]})

    def test_update_endpoint(self):
        new_region_id = str(uuid.uuid4())
        new_endpoint_name = "new_name"
        new_region = models.Region()
        new_region.id = new_region_id
        new_region.name = "new_region"
        new_region.enabled = True
        self.session.add(new_region)
        self.session.commit()

        api.update_endpoint(
            self.context, self.valid_endpoint_source.id,
            {"mapped_regions": [new_region_id], "name": new_endpoint_name})
        result = api.get_endpoint(self.context, self.valid_endpoint_source.id)
        old_endpoint_region_mapping = api.get_endpoint_region_mapping(
            self.context, self.valid_endpoint_source.id, self.valid_region.id)
        new_endpoint_region_mapping = api.get_endpoint_region_mapping(
            self.context, self.valid_endpoint_source.id, new_region_id)[0]
        self.assertEqual(result.name, new_endpoint_name)
        self.assertEqual(old_endpoint_region_mapping, [])
        self.assertEqual(new_endpoint_region_mapping.region_id, new_region_id)
        self.assertEqual(
            new_endpoint_region_mapping.endpoint_id,
            self.valid_endpoint_source.id)

    @mock.patch.object(api, 'delete_endpoint_region_mapping')
    @mock.patch.object(api, 'add_endpoint_region_mapping')
    @mock.patch.object(api, 'get_region')
    @mock.patch.object(api, '_update_sqlalchemy_object_fields')
    def test_update_endpoint_remapping_failure(
            self, mock_update_obj, mock_get_region, mock_add_mapping,
            mock_delete_mapping):
        mock_add_mapping.side_effect = [Exception, None]

        self.assertRaises(
            Exception, api.update_endpoint,
            self.context, self.valid_endpoint_source.id,
            {"mapped_regions": [mock.sentinel.region_id]})
        mock_get_region.assert_called_with(
            self.context, mock.sentinel.region_id)

        mock_delete_mapping.side_effect = Exception
        mock_update_obj.side_effect = Exception
        self.assertRaises(
            Exception, api.update_endpoint, self.context,
            self.valid_endpoint_source.id,
            {"mapped_regions": [mock.sentinel.region_id]})

    def test_delete_endpoint(self):
        new_endpoint = get_valid_endpoint()
        new_endpoint_id = new_endpoint.id
        new_endpoint_region_mapping = self.valid_endpoint_region_mapping
        new_endpoint_region_mapping.endpoint_id = new_endpoint_id
        api.add_endpoint(self.context, new_endpoint)

        api.delete_endpoint(self.context, new_endpoint_id)
        result = api.get_endpoint(self.context, new_endpoint_id)
        mappings = api.get_endpoint_region_mapping(
            self.context, new_endpoint_id, self.valid_region.id)
        self.assertIsNone(result)
        self.assertEqual(mappings, [])

    def test_delete_endpoint_not_found(self):
        self.assertRaises(
            exception.NotFound, api.delete_endpoint, self.context, "no_id")

    def test_delete_endpoint_admin_context(self):
        self.context.is_admin = True
        self.context.show_deleted = True
        new_outer_scope_endpoint = get_valid_endpoint()
        new_outer_scope_endpoint.user_id = "3"
        new_outer_scope_endpoint.project_id = "3"
        api.add_endpoint(self.context, new_outer_scope_endpoint)

        api.delete_endpoint(
            self.context, new_outer_scope_endpoint.id)
        result = api.get_endpoint(self.context, new_outer_scope_endpoint.id)
        self.assertIsNotNone(result.deleted_at)

    def test_delete_endpoint_out_of_user_scope(self):
        new_outer_scope_endpoint = get_valid_endpoint(
            user_id="3", project_id="3")
        self.session.add(new_outer_scope_endpoint)
        self.session.commit()

        self.assertRaises(
            exception.NotFound, api.delete_endpoint, self.context,
            new_outer_scope_endpoint.id)


class TransferTasksExecutionDBAPITestCase(BaseDBAPITestCase):

    @classmethod
    def setUpClass(cls):
        super(TransferTasksExecutionDBAPITestCase, cls).setUpClass()
        cls.valid_transfer = cls.valid_data['user_scope'].get('transfer')
        cls.valid_task = cls.valid_data['user_scope'].get('task')
        cls.valid_tasks_execution = cls.valid_data['user_scope'].get(
            'tasks_execution')
        cls.outer_scope_transfer = cls.valid_data['outer_scope'].get(
            'transfer')
        cls.outer_scope_tasks_execution = cls.valid_data['outer_scope'].get(
            "tasks_execution")

    def setUp(self):
        super(TransferTasksExecutionDBAPITestCase, self).setUp()
        self.outer_scope_tasks_execution.status = DEFAULT_EXECUTION_STATUS
        self.valid_tasks_execution.status = DEFAULT_EXECUTION_STATUS

    @staticmethod
    def _create_dummy_execution(action):
        new_tasks_execution = models.TasksExecution()
        new_tasks_execution.id = str(uuid.uuid4())
        new_tasks_execution.action = action
        new_tasks_execution.status = constants.EXECUTION_STATUS_UNEXECUTED
        new_tasks_execution.type = constants.EXECUTION_TYPE_TRANSFER_EXECUTION
        new_tasks_execution.number = 0

        return new_tasks_execution

    def test_get_transfer_tasks_executions_include_info(self):
        result = api.get_transfer_tasks_executions(
            self.context, self.valid_transfer.id, include_task_info=True)
        self.assertTrue(hasattr(result[0].action, 'info'))

    def test_get_transfer_tasks_executions_include_tasks(self):
        result = api.get_transfer_tasks_executions(
            self.context, self.valid_transfer.id, include_tasks=True)
        tasks = []
        for e in result:
            tasks.extend(e.tasks)

        self.assertIn(self.valid_task, tasks)

    def test_get_transfer_tasks_executions_to_dict(self):
        result = api.get_transfer_tasks_executions(
            self.context, self.valid_transfer.id, to_dict=True)
        execution_ids = [e['id'] for e in result]
        self.assertIn(self.valid_tasks_execution.id, execution_ids)

    def test_get_transfer_tasks_executions(self):
        result = api.get_transfer_tasks_executions(
            self.context, self.valid_transfer.id)
        self.assertIn(self.valid_tasks_execution, result)

    def test_get_transfer_tasks_executions_admin(self):
        self.context.is_admin = True
        result = api.get_transfer_tasks_executions(
            self.context, self.outer_scope_transfer.id)
        self.assertIn(self.outer_scope_tasks_execution, result)

    def test_get_transfer_tasks_execution_out_of_user_scope(self):
        result = api.get_transfer_tasks_executions(
            self.context, self.outer_scope_transfer.id)
        self.assertEqual(result, [])

    def test_get_transfer_tasks_execution(self):
        result = api.get_transfer_tasks_execution(
            self.context, self.valid_transfer.id,
            self.valid_tasks_execution.id)
        self.assertEqual(result, self.valid_tasks_execution)

    def test_get_transfer_tasks_execution_admin(self):
        self.context.is_admin = True
        result = api.get_transfer_tasks_execution(
            self.context, self.outer_scope_transfer.id,
            self.outer_scope_tasks_execution.id)
        self.assertEqual(result, self.outer_scope_tasks_execution)

    def test_get_transfer_tasks_execution_out_of_user_context(self):
        result = api.get_transfer_tasks_execution(
            self.context, self.outer_scope_transfer.id,
            self.outer_scope_tasks_execution.id)
        self.assertIsNone(result)

    def test_get_transfer_tasks_execution_include_task_info(self):
        result = api.get_transfer_tasks_execution(
            self.context, self.valid_transfer.id,
            self.valid_tasks_execution.id, include_task_info=True)
        self.assertTrue(hasattr(result.action, 'info'))

    def test_get_transfer_tasks_execution_to_dict(self):
        result = api.get_transfer_tasks_execution(
            self.context, self.valid_transfer.id,
            self.valid_tasks_execution.id, to_dict=True)
        self.assertEqual(result['id'], self.valid_tasks_execution.id)

    def test_add_transfer_tasks_execution(self):
        new_tasks_execution = self._create_dummy_execution(self.valid_transfer)

        api.add_transfer_tasks_execution(self.context, new_tasks_execution)
        result = api.get_transfer_tasks_execution(
            self.context, self.valid_transfer.id, new_tasks_execution.id)
        self.assertEqual(new_tasks_execution, result)
        self.assertGreater(result.number, 0)

    def test_add_transfer_tasks_execution_admin(self):
        self.context.is_admin = True
        new_tasks_execution = self._create_dummy_execution(
            self.outer_scope_transfer)
        api.add_transfer_tasks_execution(self.context, new_tasks_execution)
        result = api.get_transfer_tasks_execution(
            self.context, self.outer_scope_transfer.id, new_tasks_execution.id)
        self.assertEqual(new_tasks_execution, result)

    def test_add_transfer_tasks_execution_out_of_user_context(self):
        new_tasks_execution = self._create_dummy_execution(
            self.outer_scope_transfer)
        self.assertRaises(
            exception.NotAuthorized, api.add_transfer_tasks_execution,
            self.context, new_tasks_execution)

    def test_delete_transfer_tasks_execution(self):
        new_tasks_execution = self._create_dummy_execution(self.valid_transfer)
        api.add_transfer_tasks_execution(self.context, new_tasks_execution)
        api.delete_transfer_tasks_execution(
            self.context, new_tasks_execution.id)
        result = api.get_transfer_tasks_execution(
            self.context, self.valid_transfer.id, new_tasks_execution.id)
        self.assertIsNone(result)

    def test_delete_transfer_tasks_execution_admin(self):
        self.context.is_admin = True
        new_tasks_execution = self._create_dummy_execution(
            self.outer_scope_transfer)
        api.add_transfer_tasks_execution(self.context, new_tasks_execution)
        api.delete_transfer_tasks_execution(
            self.context, new_tasks_execution.id)
        result = api.get_transfer_tasks_execution(
            self.context, self.outer_scope_transfer.id, new_tasks_execution.id)
        self.assertIsNone(result)

    def test_delete_transfer_tasks_execution_out_of_user_scope(self):
        self.context.is_admin = True
        new_tasks_execution = self._create_dummy_execution(
            self.outer_scope_transfer)
        api.add_transfer_tasks_execution(self.context, new_tasks_execution)

        self.context.is_admin = False
        self.assertRaises(
            exception.NotAuthorized, api.delete_transfer_tasks_execution,
            self.context, new_tasks_execution.id)

    def test_delete_transfer_tasks_execution_not_found(self):
        self.context.is_admin = True
        self.assertRaises(
            exception.NotFound, api.delete_transfer_tasks_execution,
            self.context, "invalid_id")

    def test_set_execution_status_admin(self):
        self.context.is_admin = True
        new_status = constants.EXECUTION_STATUS_COMPLETED
        result = api.set_execution_status(
            self.context, self.outer_scope_tasks_execution.id, new_status,
            update_action_status=False)
        self.assertEqual(result.status, new_status)

    def test_set_execution_status_out_of_user_scope(self):
        self.assertRaises(
            exception.NotFound, api.set_execution_status, self.context,
            self.outer_scope_tasks_execution.id, mock.ANY,
            update_action_status=False)

    def test_set_execution_status_not_found(self):
        self.assertRaises(
            exception.NotFound, api.set_execution_status, self.context,
            "invalid_id", mock.ANY,
            update_action_status=False)

    def test_set_execution_status_update_action_status(self):
        new_status = constants.EXECUTION_STATUS_COMPLETED
        api.set_execution_status(
            self.context, self.valid_tasks_execution.id, new_status)
        self.assertEqual(self.valid_transfer.last_execution_status, new_status)


class TransferSchedulesDBAPITestCase(BaseDBAPITestCase):

    @classmethod
    def setUpClass(cls):
        super(TransferSchedulesDBAPITestCase, cls).setUpClass()
        cls.valid_transfer_schedule = cls.valid_data['user_scope'].get(
            'transfer_schedule')
        cls.valid_transfer = cls.valid_data['user_scope'].get('transfer')
        cls.outer_scope_transfer_schedule = cls.valid_data['outer_scope'].get(
            'transfer_schedule')
        cls.outer_scope_transfer = cls.valid_data['outer_scope'].get(
            'transfer')

    @staticmethod
    def _create_dummy_transfer_schedule(transfer, expiration_date):
        ts = models.TransferSchedule()
        ts.id = str(uuid.uuid4())
        ts.transfer = transfer
        ts.schedule = {}
        ts.expiration_date = expiration_date
        ts.enabled = True
        ts.shutdown_instance = False
        ts.trust_id = str(uuid.uuid4())

        return ts

    def test__get_transfer_schedules_filter(self):
        result = api._get_transfer_schedules_filter(self.context).all()
        self.assertIn(self.valid_transfer_schedule, result)

    def test__get_transfer_schedules_filter_admin(self):
        self.context.is_admin = True
        result = api._get_transfer_schedules_filter(
            self.context, schedule_id=self.outer_scope_transfer_schedule.id
        ).first()
        self.assertEqual(result, self.outer_scope_transfer_schedule)

    def test__get_transfer_schedules_filter_out_of_user_context(self):
        result = api._get_transfer_schedules_filter(
            self.context, schedule_id=self.outer_scope_transfer_schedule.id
        ).first()
        self.assertIsNone(result)

    def test__get_transfer_schedules_filter_by_transfer(self):
        result = api._get_transfer_schedules_filter(
            self.context, transfer_id=self.valid_transfer_schedule.transfer_id)
        self.assertEqual(result.first(), self.valid_transfer_schedule)

    def test__get_transfer_schedules_filter_by_schedule_id(self):
        result = api._get_transfer_schedules_filter(
            self.context, schedule_id=self.valid_transfer_schedule.id).first()
        self.assertEqual(result, self.valid_transfer_schedule)

    def test__get_transfer_schedules_filter_by_not_expired(self):
        expiration_date = timeutils.utcnow() + datetime.timedelta(days=1)
        unexpired_transfer_schedule = self._create_dummy_transfer_schedule(
            self.valid_transfer, expiration_date=expiration_date)
        self.session.add(unexpired_transfer_schedule)
        expiration_null_transfer_schedule = (
            self._create_dummy_transfer_schedule(
                self.valid_transfer, expiration_date=None))
        self.session.add(expiration_null_transfer_schedule)
        result = api._get_transfer_schedules_filter(
            self.context, expired=False).all()
        self.assertIn(unexpired_transfer_schedule, result)
        self.assertIn(expiration_null_transfer_schedule, result)

    def test_get_transfer_schedules(self):
        result = api.get_transfer_schedules(self.context)
        self.assertIn(self.valid_transfer_schedule, result)

    def test_get_transfer_schedule(self):
        result = api.get_transfer_schedule(
            self.context, self.valid_transfer.id,
            self.valid_transfer_schedule.id)
        self.assertEqual(result, self.valid_transfer_schedule)

    def test_update_transfer_schedule(self):
        pre_update_mock = mock.Mock()
        post_update_mock = mock.Mock()
        api.update_transfer_schedule(
            self.context, self.valid_transfer.id,
            self.valid_transfer_schedule.id, {"shutdown_instance": True},
            pre_update_callable=pre_update_mock,
            post_update_callable=post_update_mock)
        result = api.get_transfer_schedule(
            self.context, self.valid_transfer.id,
            self.valid_transfer_schedule.id)
        self.assertEqual(result.shutdown_instance, True)
        pre_update_mock.assert_called_once_with(
            schedule=self.valid_transfer_schedule)
        post_update_mock.assert_called_once_with(
            self.context, self.valid_transfer_schedule)

    def test_delete_transfer_schedule_not_found(self):
        self.assertRaises(exception.NotFound, api.delete_transfer_schedule,
                          self.context, self.valid_transfer.id, "invalid")

    def test_delete_transfer_schedule_admin(self):
        self.context.is_admin = True
        outer_scope_schedule = self._create_dummy_transfer_schedule(
            self.outer_scope_transfer, None)
        self.session.add(outer_scope_schedule)
        api.delete_transfer_schedule(
            self.context, self.outer_scope_transfer.id,
            outer_scope_schedule.id)
        result = api.get_transfer_schedule(
            self.context, self.outer_scope_transfer.id,
            outer_scope_schedule.id)
        self.assertIsNone(result)

    def test_delete_transfer_schedule_out_of_user_context(self):
        outer_scope_schedule = self._create_dummy_transfer_schedule(
            self.outer_scope_transfer, None)
        self.session.add(outer_scope_schedule)
        self.assertRaises(
            exception.NotAuthorized, api.delete_transfer_schedule,
            self.context, self.outer_scope_transfer.id,
            outer_scope_schedule.id)

    def test_delete_transfer_schedule(self):
        dummy_transfer_schedule = self._create_dummy_transfer_schedule(
            self.valid_transfer, None)
        self.session.add(dummy_transfer_schedule)
        pre_delete_mock = mock.Mock()
        post_delete_mock = mock.Mock()
        api.delete_transfer_schedule(
            self.context, self.valid_transfer.id, dummy_transfer_schedule.id,
            pre_delete_callable=pre_delete_mock,
            post_delete_callable=post_delete_mock)
        result = api.get_transfer_schedule(
            self.context, self.valid_transfer.id, dummy_transfer_schedule.id)
        self.assertIsNone(result)
        pre_delete_mock.assert_called_once_with(
            self.context, dummy_transfer_schedule)
        post_delete_mock.assert_called_once_with(
            self.context, dummy_transfer_schedule)

    def test_delete_transfer_schedule_already_deleted(self):
        dummy_transfer_schedule = self._create_dummy_transfer_schedule(
            self.valid_transfer, None)
        self.session.add(dummy_transfer_schedule)

        def pre_delete(context, schedule):
            schedule.deleted = True
            schedule.deleted_at = timeutils.utcnow()
            context.session.commit()

        self.assertRaises(
            exception.NotFound, api.delete_transfer_schedule,
            self.context, self.valid_transfer.id, dummy_transfer_schedule.id,
            pre_delete_callable=pre_delete)

    def test_add_transfer_schedule(self):
        new_schedule = self._create_dummy_transfer_schedule(
            self.valid_transfer, None)
        post_add_mock = mock.Mock()
        api.add_transfer_schedule(
            self.context, new_schedule, post_create_callable=post_add_mock)
        result = api.get_transfer_schedule(
            self.context, self.valid_transfer.id, new_schedule.id)
        self.assertEqual(result, new_schedule)
        post_add_mock.assert_called_once_with(self.context, new_schedule)

    def test_add_transfer_schedule_out_of_user_context(self):
        new_schedule = self._create_dummy_transfer_schedule(
            self.outer_scope_transfer, None)
        self.assertRaises(
            exception.NotAuthorized, api.add_transfer_schedule,
            self.context, new_schedule)


class TransfersDBAPITestCase(BaseDBAPITestCase):

    @classmethod
    def setUpClass(cls):
        super(TransfersDBAPITestCase, cls).setUpClass()
        cls.valid_transfer = cls.valid_data['user_scope'].get('transfer')
        cls.valid_transfer_execution = cls.valid_data['user_scope'].get(
            'tasks_execution')
        cls.outer_scope_transfer = cls.valid_data['outer_scope'].get(
            'transfer')

    @staticmethod
    def _create_dummy_transfer(scenario=constants.TRANSFER_SCENARIO_REPLICA,
                               origin_endpoint_id=str(uuid.uuid4()),
                               destination_endpoint_id=str(uuid.uuid4()),
                               project_id=DEFAULT_PROJECT_ID):
        transfer = models.Transfer()
        transfer.id = str(uuid.uuid4())
        transfer.user_id = project_id
        transfer.project_id = project_id
        transfer.base_id = transfer.id
        transfer.scenario = scenario
        transfer.last_execution_status = DEFAULT_EXECUTION_STATUS
        transfer.executions = []
        transfer.instances = [DEFAULT_INSTANCE]
        transfer.info = DEFAULT_TASK_INFO
        transfer.origin_endpoint_id = origin_endpoint_id
        transfer.destination_endpoint_id = destination_endpoint_id

        return transfer

    def test_get_transfers_admin(self):
        self.context.is_admin = True
        result = api.get_transfers(self.context)
        self.assertIn(self.outer_scope_transfer, result)

    def test_get_transfers_out_of_user_context(self):
        result = api.get_transfers(self.context)
        self.assertNotIn(self.outer_scope_transfer, result)

    def test_get_transfers(self):
        result = api.get_transfers(self.context)
        self.assertIn(self.valid_transfer, result)

    def test_get_transfers_include_tasks_executions(self):
        result = api.get_transfers(self.context, include_tasks_executions=True)
        executions = []
        for transfer in result:
            executions.extend(transfer.executions)
        self.assertIn(self.valid_transfer_execution, executions)

    def test_get_transfers_include_task_info(self):
        result = api.get_transfers(self.context, include_task_info=True)
        self.assertTrue(hasattr(result[0], 'info'))

    def test_get_transfers_transfer_scenario(self):
        scenario = constants.TRANSFER_SCENARIO_REPLICA
        result = api.get_transfers(self.context, transfer_scenario=scenario)
        self.assertTrue(all([res.scenario == scenario for res in result]))

    def test_get_transfers_to_dict(self):
        result = api.get_transfers(self.context, to_dict=True)
        transfer_ids = [res['id'] for res in result]
        self.assertIn(self.valid_transfer.id, transfer_ids)

    def test_get_transfer_admin(self):
        self.context.is_admin = True
        result = api.get_transfer(self.context, self.outer_scope_transfer.id)
        self.assertEqual(result, self.outer_scope_transfer)

    def test_get_transfer_include_task_info(self):
        result = api.get_transfer(
            self.context, self.valid_transfer.id, include_task_info=True)
        self.assertEqual(result.info, DEFAULT_TASK_INFO)

    def test_get_transfer_by_scenario(self):
        result = api.get_transfer(
            self.context, self.valid_transfer.id,
            transfer_scenario=constants.TRANSFER_SCENARIO_REPLICA)
        self.assertEqual(result, self.valid_transfer)

    def test_get_transfer_out_of_user_scope(self):
        result = api.get_transfer(self.context, self.outer_scope_transfer.id)
        self.assertIsNone(result)

    def test_get_transfer_to_dict(self):
        result = api.get_transfer(
            self.context, self.valid_transfer.id, to_dict=True)
        self.assertEqual(result['id'], self.valid_transfer.id)

        result = api.get_transfer(self.context, "invalid", to_dict=True)
        self.assertIsNone(result)

    def test_get_endpoint_transfers_count(self):
        origin_endpoint_id = str(uuid.uuid4())
        dest_endpoint_id = str(uuid.uuid4())
        dummy_transfer_replica = self._create_dummy_transfer(
            origin_endpoint_id=origin_endpoint_id,
            destination_endpoint_id=dest_endpoint_id)
        dummy_transfer_migration = self._create_dummy_transfer(
            scenario=constants.TRANSFER_SCENARIO_LIVE_MIGRATION,
            origin_endpoint_id=origin_endpoint_id,
            destination_endpoint_id=dest_endpoint_id)
        self.session.add(dummy_transfer_replica)
        self.session.add(dummy_transfer_migration)

        result = api.get_endpoint_transfers_count(
            self.context, origin_endpoint_id)
        self.assertEqual(result, 2)

        result = api.get_endpoint_transfers_count(
            self.context, origin_endpoint_id,
            transfer_scenario=constants.TRANSFER_SCENARIO_REPLICA)
        self.assertEqual(result, 1)

    def test_add_transfer(self):
        dummy_transfer = self._create_dummy_transfer()
        api.add_transfer(self.context, dummy_transfer)
        result = api.get_transfer(self.context, dummy_transfer.id)
        self.assertEqual(result, dummy_transfer)


class DeploymentsDBAPITestCase(BaseDBAPITestCase):

    @classmethod
    def setUpClass(cls):
        super(DeploymentsDBAPITestCase, cls).setUpClass()
        cls.user_deployment = cls.valid_data['user_scope'].get('deployment')
        cls.outer_scope_deployment = cls.valid_data['outer_scope'].get(
            'deployment')
        cls.user_deployment_execution = cls.valid_data['user_scope'].get(
            'deployment_execution')
        cls.outer_scope_deployment_execution = cls.valid_data[
            'outer_scope'].get('deployment_execution')
        cls.user_deployment_task = cls.user_deployment_execution.tasks[0]
        cls.user_transfer = cls.valid_data['user_scope'].get('transfer')

    @staticmethod
    def _create_dummy_deployment(transfer_id,
                                 origin_endpoint_id=str(uuid.uuid4()),
                                 destination_endpoint_id=str(uuid.uuid4()),
                                 project_id=DEFAULT_PROJECT_ID):
        deployment = models.Deployment()
        deployment.id = str(uuid.uuid4())
        deployment.user_id = project_id
        deployment.project_id = project_id
        deployment.base_id = deployment.id
        deployment.transfer_id = transfer_id
        deployment.last_execution_status = DEFAULT_EXECUTION_STATUS
        deployment.executions = []
        deployment.instances = [DEFAULT_INSTANCE]
        deployment.info = DEFAULT_TASK_INFO
        deployment.origin_endpoint_id = origin_endpoint_id
        deployment.destination_endpoint_id = destination_endpoint_id

        return deployment

    def test_get_transfers_deployments_admin(self):
        self.context.is_admin = True
        result = api.get_transfer_deployments(
            self.context, self.user_deployment.transfer_id)
        self.assertIn(self.user_deployment, result)

    def test_get_transfer_deployments_out_of_user_context(self):
        result = api.get_transfer_deployments(
            self.context, self.outer_scope_deployment.transfer_id)
        self.assertNotIn(self.outer_scope_deployment, result)

    def test_get_transfer_deployments(self):
        result = api.get_transfer_deployments(
            self.context, self.user_deployment.transfer_id)
        self.assertIn(self.user_deployment, result)

    def test_get_deployments_admin(self):
        self.context.is_admin = True
        result = api.get_deployments(self.context)
        self.assertIn(self.outer_scope_deployment, result)
        self.assertIn(self.outer_scope_deployment_execution,
                      self.outer_scope_deployment.executions)

    def test_get_deployments_include_tasks(self):
        result = api.get_deployments(self.context, include_tasks=True)
        self.assertIn(self.user_deployment, result)
        tasks = []
        for dep in result:
            for execution in dep.executions:
                tasks.extend(execution.tasks)
        self.assertIn(self.user_deployment_task, tasks)

    def test_get_deployments_include_task_info(self):
        result = api.get_deployments(self.context, include_task_info=True)
        for dep in result:
            if dep.id == self.user_deployment.id:
                self.assertEqual(dep.info, DEFAULT_TASK_INFO)

    def test_get_deployments_out_of_user_context(self):
        result = api.get_deployments(self.context)
        self.assertNotIn(self.outer_scope_deployment, result)

    def test_get_deployments(self):
        result = api.get_deployments(self.context)
        self.assertIn(self.user_deployment, result)

    def test_get_deployments_to_dict(self):
        result = api.get_deployments(self.context, to_dict=True)
        self.assertIn(self.user_deployment.id, [d['id'] for d in result])

    def test_get_deployment_admin(self):
        self.context.is_admin = True
        result = api.get_deployment(
            self.context, self.outer_scope_deployment.id)
        self.assertEqual(self.outer_scope_deployment, result)

    def test_get_deployment_include_task_info(self):
        result = api.get_deployment(self.context, self.user_deployment.id,
                                    include_task_info=True)
        self.assertEqual(result.info, self.user_deployment.info)

    def test_get_deployment_out_of_user_context(self):
        result = api.get_deployment(
            self.context, self.outer_scope_deployment.id)
        self.assertIsNone(result)

    def test_get_deployment(self):
        result = api.get_deployment(self.context, self.user_deployment.id)
        self.assertEqual(result, self.user_deployment)

    def test_get_deployment_to_dict(self):
        result = api.get_deployment(self.context, self.user_deployment.id,
                                    to_dict=True)
        self.assertEqual(result['id'], self.user_deployment.id)

    def test_add_deployment(self):
        dummy_deployment = self._create_dummy_deployment(self.user_transfer.id)
        api.add_deployment(self.context, dummy_deployment)
        result = api.get_deployment(self.context, dummy_deployment.id)
        self.assertEqual(result, dummy_deployment)


class BaseTransferActionDBAPITestCase(BaseDBAPITestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseTransferActionDBAPITestCase, cls).setUpClass()
        cls.user_transfer = cls.valid_data['user_scope'].get('transfer')
        cls.outer_scope_transfer = cls.valid_data['outer_scope'].get(
            'transfer')

    def test_get_action_admin(self):
        self.context.is_admin = True
        result = api.get_action(self.context, self.outer_scope_transfer.id)
        self.assertEqual(result, self.outer_scope_transfer)

    def test_get_action_not_found(self):
        self.assertRaises(
            exception.NotFound, api.get_action, self.context,
            self.outer_scope_transfer.id)

    def test_get_action_include_task_info(self):
        result = api.get_action(
            self.context, self.user_transfer.id, include_task_info=True)
        self.assertEqual(result.info, self.user_transfer.info)
