# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import datetime
import jsonschema
from webob import exc

from coriolis.api.v1 import replica_schedules
from coriolis.api.v1.views import replica_schedule_view
from coriolis import exception
from coriolis.replica_cron import api
from coriolis import schemas
from coriolis.tests import test_base


class ReplicaScheduleControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Replica Schedule v1 API"""

    def setUp(self):
        super(ReplicaScheduleControllerTestCase, self).setUp()
        self.replica_schedules = replica_schedules.ReplicaScheduleController()

    @mock.patch.object(replica_schedule_view, 'single')
    @mock.patch.object(api.API, 'get_schedule')
    def test_show(
        self,
        mock_get_schedule,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        replica_id = mock.sentinel.replica_id

        result = self.replica_schedules.show(mock_req, replica_id, id)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_schedules:show")
        mock_get_schedule.assert_called_once_with(mock_context, replica_id, id)
        mock_single.assert_called_once_with(mock_get_schedule.return_value)

    @mock.patch.object(replica_schedule_view, 'single')
    @mock.patch.object(api.API, 'get_schedule')
    def test_show_not_found(
        self,
        mock_get_schedule,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        replica_id = mock.sentinel.replica_id
        mock_get_schedule.return_value = None

        self.assertRaises(
            exc.HTTPNotFound,
            self.replica_schedules.show,
            mock_req,
            replica_id,
            id
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_schedules:show")
        mock_get_schedule.assert_called_once_with(mock_context, replica_id, id)
        mock_single.assert_not_called()

    @mock.patch.object(replica_schedule_view, 'collection')
    @mock.patch.object(api.API, 'get_schedules')
    def test_index(
        self,
        mock_get_schedules,
        mock_collection
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        replica_id = mock.sentinel.replica_id
        mock_req.GET = {"show_expired": "False"}

        result = self.replica_schedules.index(mock_req, replica_id)
        self.assertEqual(
            mock_collection.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_schedules:list")
        mock_get_schedules.assert_called_once_with(
            mock_context,
            replica_id,
            expired=False
        )
        mock_collection.assert_called_once_with(
            mock_get_schedules.return_value)

    @mock.patch('coriolis.schemas.SCHEDULE_API_BODY_SCHEMA')
    @mock.patch.object(schemas, 'validate_value')
    def test_validate_schedule(
        self,
        mock_validate_value,
        mock_schedule_api_body_schema
    ):
        schedule = mock.sentinel.schedule

        result = self.replica_schedules._validate_schedule(schedule)

        self.assertEqual(
            schedule,
            result
        )

        mock_validate_value.assert_called_once_with(
            schedule, mock_schedule_api_body_schema['properties']['schedule'])

    def test_validate_expiration_date_is_none(
        self
    ):
        expiration_date = None

        result = self.replica_schedules._validate_expiration_date(
            expiration_date)

        self.assertEqual(
            None,
            result
        )

    def test_validate_expiration_date_past(
        self
    ):
        expiration_date = '1970-1-1'

        self.assertRaises(
            exception.InvalidInput,
            self.replica_schedules._validate_expiration_date,
            expiration_date
        )

    def test_validate_expiration_date(
        self
    ):
        expiration_date = '9999-12-31'

        result = self.replica_schedules._validate_expiration_date(
            expiration_date)

        self.assertEqual(
            datetime.datetime(9999, 12, 31),
            result
        )

    @mock.patch.object(replica_schedules.ReplicaScheduleController,
                       '_validate_expiration_date')
    @mock.patch.object(schemas, 'validate_value')
    @mock.patch.object(jsonschema, 'FormatChecker')
    @mock.patch.object(replica_schedules.ReplicaScheduleController,
                       '_validate_schedule')
    def test_validate_create_body(
        self,
        mock_validate_schedule,
        mock_format_checker,
        mock_validate_value,
        mock_validate_expiration_date
    ):
        schedule = mock.sentinel.schedule
        date = mock.sentinel.date
        mock_body = {
            "schedule": schedule,
            "enabled": False,
            "expiration_date": date,
            "shutdown_instance": True
        }
        expected_result = (
            mock_validate_schedule.return_value,
            False,
            mock_validate_expiration_date.return_value,
            True
        )

        result = self.replica_schedules._validate_create_body(mock_body)

        self.assertEqual(
            expected_result,
            result
        )

        mock_validate_schedule.assert_called_once_with(schedule)
        mock_validate_value.assert_called_once_with(
            mock_body, schemas.SCHEDULE_API_BODY_SCHEMA,
            format_checker=mock_format_checker.return_value
        )
        mock_validate_expiration_date.assert_called_once_with(date)

    @mock.patch.object(replica_schedules.ReplicaScheduleController,
                       '_validate_expiration_date')
    @mock.patch.object(schemas, 'validate_value')
    @mock.patch.object(jsonschema, 'FormatChecker')
    @mock.patch.object(replica_schedules.ReplicaScheduleController,
                       '_validate_schedule')
    def test_validate_create_body_no_expiration_date(
        self,
        mock_validate_schedule,
        mock_format_checker,
        mock_validate_value,
        mock_validate_expiration_date
    ):
        schedule = mock.sentinel.schedule
        mock_body = {
            "schedule": schedule,
            "enabled": False,
            "shutdown_instance": True
        }
        expected_result = (
            mock_validate_schedule.return_value,
            False,
            None,
            True
        )

        result = self.replica_schedules._validate_create_body(mock_body)

        self.assertEqual(
            expected_result,
            result
        )

        mock_validate_schedule.assert_called_once_with(schedule)
        mock_validate_value.assert_called_once_with(
            mock_body, schemas.SCHEDULE_API_BODY_SCHEMA,
            format_checker=mock_format_checker.return_value
        )
        mock_validate_expiration_date.assert_not_called()

    def test_validate_create_body_no_schedule(
        self,
    ):
        schedule = None
        mock_body = {"schedule": schedule}

        self.assertRaises(
            exception.InvalidInput,
            self.replica_schedules._validate_create_body,
            mock_body
        )

    @mock.patch.object(replica_schedules.ReplicaScheduleController,
                       '_validate_expiration_date')
    @mock.patch.object(schemas, 'validate_value')
    @mock.patch.object(jsonschema, 'FormatChecker')
    @mock.patch.object(replica_schedules.ReplicaScheduleController,
                       '_validate_schedule')
    def test_validate_update_body(
        self,
        mock_validate_schedule,
        mock_format_checker,
        mock_validate_value,
        mock_validate_expiration_date
    ):
        schedule = mock.sentinel.schedule
        date = mock.sentinel.date
        mock_update_body = {
            "schedule": schedule,
            "enabled": False,
            "expiration_date": date,
            "shutdown_instance": True
        }
        expected_result = {
            "schedule": mock_validate_schedule.return_value,
            "enabled": False,
            "expiration_date": mock_validate_expiration_date.return_value,
            "shutdown_instance": True
        }

        result = self.replica_schedules._validate_update_body(mock_update_body)

        self.assertEqual(
            expected_result,
            result
        )

        mock_validate_schedule.assert_called_once_with(schedule)
        mock_validate_value.assert_called_once_with(
            expected_result, schemas.SCHEDULE_API_BODY_SCHEMA,
            format_checker=mock_format_checker.return_value
        )
        mock_validate_expiration_date.assert_called_once_with(date)

    @mock.patch.object(replica_schedules.ReplicaScheduleController,
                       '_validate_expiration_date')
    @mock.patch.object(schemas, 'validate_value')
    @mock.patch.object(jsonschema, 'FormatChecker')
    @mock.patch.object(replica_schedules.ReplicaScheduleController,
                       '_validate_schedule')
    def test_validate_update_body_none(
        self,
        mock_validate_schedule,
        mock_format_checker,
        mock_validate_value,
        mock_validate_expiration_date
    ):
        mock_update_body = {}
        expected_result = {}

        result = self.replica_schedules._validate_update_body(mock_update_body)

        self.assertEqual(
            expected_result,
            result
        )

        mock_validate_schedule.assert_not_called()
        mock_validate_value.assert_called_once_with(
            expected_result, schemas.SCHEDULE_API_BODY_SCHEMA,
            format_checker=mock_format_checker.return_value
        )
        mock_validate_expiration_date.assert_not_called()

    @mock.patch.object(replica_schedule_view, 'single')
    @mock.patch.object(api.API, 'create')
    @mock.patch.object(replica_schedules.ReplicaScheduleController,
                       '_validate_create_body')
    def test_create(
        self,
        mock_validate_create_body,
        mock_create,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        replica_id = mock.sentinel.replica_id
        body = mock.sentinel.body
        schedule = mock.sentinel.schedule
        exp_date = mock.sentinel.exp_date
        mock_validate_create_body.return_value = (
            schedule, False, exp_date, True)

        result = self.replica_schedules.create(mock_req, replica_id, body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_schedules:create")
        mock_validate_create_body.assert_called_once_with(body)
        mock_create.assert_called_once_with(
            mock_context, replica_id, schedule, False, exp_date, True)
        mock_single.assert_called_once_with(mock_create.return_value)

    @mock.patch.object(replica_schedules.ReplicaScheduleController,
                       '_validate_create_body')
    def test_create_except(
        self,
        mock_validate_create_body,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        replica_id = mock.sentinel.replica_id
        body = mock.sentinel.body
        mock_validate_create_body.side_effect = Exception("err")

        self.assertRaises(
            exception.InvalidInput,
            self.replica_schedules.create,
            mock_req,
            replica_id,
            body
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_schedules:create")
        mock_validate_create_body.assert_called_once_with(body)

    @mock.patch.object(replica_schedule_view, 'single')
    @mock.patch.object(api.API, 'update')
    @mock.patch.object(replica_schedules.ReplicaScheduleController,
                       '_validate_update_body')
    def test_update(
        self,
        mock_validate_update_body,
        mock_update,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        replica_id = mock.sentinel.replica_id
        id = mock.sentinel.id
        body = mock.sentinel.body

        result = self.replica_schedules.update(mock_req, replica_id, id, body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_schedules:update")
        mock_validate_update_body.assert_called_once_with(body)
        mock_update.assert_called_once_with(
            mock_context, replica_id, id,
            mock_validate_update_body.return_value)
        mock_single.assert_called_once_with(mock_update.return_value)

    @mock.patch.object(replica_schedules.ReplicaScheduleController,
                       '_validate_update_body')
    def test_update_except(
        self,
        mock_validate_update_body,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        replica_id = mock.sentinel.replica_id
        id = mock.sentinel.id
        body = mock.sentinel.body
        mock_validate_update_body.side_effect = Exception("err")

        self.assertRaises(
            exception.InvalidInput,
            self.replica_schedules.update,
            mock_req,
            replica_id,
            id,
            body
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_schedules:update")
        mock_validate_update_body.assert_called_once_with(body)

    @mock.patch.object(api.API, 'delete')
    def test_delete(
        self,
        mock_delete
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        replica_id = mock.sentinel.replica_id
        id = mock.sentinel.id

        self.assertRaises(
            exc.HTTPNoContent,
            self.replica_schedules.delete,
            mock_req,
            replica_id,
            id
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_schedules:delete")
        mock_delete.assert_called_once_with(mock_context, replica_id, id)
