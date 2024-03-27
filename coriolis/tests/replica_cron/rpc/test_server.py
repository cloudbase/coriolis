# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import datetime
import ddt
import json

from coriolis.conductor.rpc import client as rpc_client
from coriolis import exception
from coriolis.replica_cron.rpc import server
from coriolis.tests import test_base


class TriggerReplicaTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis _trigger_replica function."""

    def test__trigger_replica(self):
        mock_conductor_client = mock.MagicMock()

        mock_conductor_client.execute_replica_tasks.return_value = {
            'id': mock.sentinel.id,
            'action_id': mock.sentinel.action_id
        }

        result = server._trigger_replica(
            mock.sentinel.ctxt,
            mock_conductor_client,
            mock.sentinel.replica_id, False)

        mock_conductor_client.execute_replica_tasks.assert_called_once_with(
            mock.sentinel.ctxt, mock.sentinel.replica_id, False)

        self.assertEqual(
            result, 'Execution %s for Replica %s' % (
                mock.sentinel.id, mock.sentinel.action_id))

    def test__trigger_replica_invalid_replica_state(self):
        mock_conductor_client = mock.MagicMock()

        mock_conductor_client.execute_replica_tasks.\
            side_effect = exception.InvalidReplicaState(reason='test_reason')

        server._trigger_replica(
            mock.sentinel.ctxt,
            mock_conductor_client,
            mock.sentinel.action_id, False)


@ddt.ddt
class ReplicaCronServerEndpointTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis ReplicaCronServerEndpoint class."""

    @mock.patch.object(server.ReplicaCronServerEndpoint, '_init_cron')
    def setUp(self, _):
        super(ReplicaCronServerEndpointTestCase, self).setUp()
        self.server = server.ReplicaCronServerEndpoint()

    @ddt.data(
        {
            'input': {
                'expiration_date': 'test_expiration_date',
                'schedule': 'test_schedule'
            },
            'expected': {
                'expiration_date': 'test_normalized_date',
                'schedule': 'test_json'
            }
        },
        {
            'input': {
                'schedule': 'test_schedule'
            },
            'expected': {
                'schedule': 'test_json'
            }
        },
        {
            'input': {
                'expiration_date': 'test_expiration_date',
                'schedule': ['test_schedule']
            },
            'expected': {
                'expiration_date': 'test_normalized_date',
                'schedule': ['test_schedule']
            }
        }
    )
    @mock.patch.object(server.timeutils, 'parse_isotime')
    @mock.patch.object(server.timeutils, 'normalize_time')
    @mock.patch.object(json, 'loads')
    def test__deserialize_schedule(self, data, mock_json_loads,
                                   mock_normalize_time, mock_parse_isotime):
        mock_json_loads.return_value = 'test_json'
        mock_parse_isotime.return_value = 'test_date'
        mock_normalize_time.return_value = 'test_normalized_date'

        result = self.server._deserialize_schedule(data['input'])

        if 'expiration_date' in data['input']:
            mock_parse_isotime.assert_called_once_with('test_expiration_date')
            mock_normalize_time.assert_called_once_with('test_date')
        if isinstance(data['input']['schedule'], str):
            mock_json_loads.assert_called_once_with('test_schedule')

        self.assertEqual(result, data['expected'])

    @mock.patch.object(server.ReplicaCronServerEndpoint,
                       '_deserialize_schedule')
    @mock.patch.object(server, '_trigger_replica')
    @mock.patch.object(server.timeutils, 'utcnow')
    @mock.patch.object(server.context, 'get_admin_context')
    @mock.patch.object(server.cron, 'CronJob')
    @mock.patch.object(server.cron.Cron, 'register')
    def test__register_schedule(self, mock_register, mock_cron_job,
                                mock_get_admin_context, mock_utcnow,
                                mock_trigger_replica,
                                mock_deserialize_schedule):
        mock_get_admin_context.return_value = 'test_admin_context'
        mock_utcnow.return_value = datetime.datetime(2022, 1, 1)
        mock_deserialize_schedule.return_value = {
            'id': 'test_id',
            'enabled': True,
            'expiration_date': datetime.datetime(2022, 12, 31),
            'schedule': 'test_schedule'
        }
        test_schedule = {
            'trust_id': 'test_schedule_trust_id',
            'replica_id': 'test_schedule_replica_id',
            'shutdown_instance': 'test_schedule_shutdown_instance'
        }

        mock_cron_job.return_value = mock_cron_job

        self.server._register_schedule(test_schedule)

        mock_deserialize_schedule.assert_called_once_with(test_schedule)
        mock_get_admin_context.assert_called_once_with(
            trust_id='test_schedule_trust_id')
        mock_cron_job.assert_called_once_with(
            'test_id', 'Scheduled job for test_id', 'test_schedule',
            True, datetime.datetime(2022, 12, 31), None, None,
            mock_trigger_replica, 'test_admin_context',
            self.server._rpc_client, 'test_schedule_replica_id',
            'test_schedule_shutdown_instance')
        mock_register.assert_called_once()

    @mock.patch.object(server.ReplicaCronServerEndpoint,
                       '_deserialize_schedule')
    @mock.patch.object(server.timeutils, 'utcnow')
    def test__register_schedule_expired(self, mock_utcnow,
                                        mock_deserialize_schedule):
        mock_utcnow.return_value = datetime.datetime(2022, 12, 31)
        mock_deserialize_schedule.return_value = {
            'id': 'test_id',
            'enabled': True,
            'expiration_date': datetime.datetime(2022, 1, 1),
            'schedule': 'test_schedule'
        }
        test_schedule = {
            'trust_id': 'test_schedule_trust_id',
            'replica_id': 'test_schedule_replica_id',
            'shutdown_instance': 'test_schedule_shutdown_instance'
        }

        self.server._register_schedule(test_schedule)
        mock_deserialize_schedule.assert_called_once_with(test_schedule)

    @mock.patch.object(server.timeutils, 'utcnow')
    @mock.patch.object(server.ReplicaCronServerEndpoint, '_get_all_schedules')
    @mock.patch.object(server.ReplicaCronServerEndpoint, '_register_schedule')
    @mock.patch.object(server.cron.Cron, 'start')
    def test__init_cron(self, mock_cron_start, mock_register_schedule,
                        mock_get_all_schedules, mock_utcnow):
        mock_utcnow.return_value = datetime.datetime(2022, 1, 1)
        mock_get_all_schedules.return_value = [
            {'id': 'schedule1'},
            {'id': 'schedule2'},
        ]
        self.server._cron.start = mock_cron_start

        self.server._init_cron()

        mock_utcnow.assert_called_once()
        mock_get_all_schedules.assert_called_once()
        mock_register_schedule.assert_has_calls([
            mock.call({'id': 'schedule1'}, date=mock_utcnow.return_value),
            mock.call({'id': 'schedule2'}, date=mock_utcnow.return_value),
        ])
        mock_cron_start.assert_called_once()

    @mock.patch.object(server.ReplicaCronServerEndpoint, '_get_all_schedules')
    @mock.patch.object(server.ReplicaCronServerEndpoint, '_register_schedule')
    def test__init_cron_with_exception(self, mock_register_schedule,
                                       mock_get_all_schedules):
        mock_get_all_schedules.return_value = [
            {'id': 'schedule1'},
            {'id': 'schedule2'},
        ]
        mock_register_schedule.side_effect = Exception('test_exception')

        self.server._init_cron()

        mock_get_all_schedules.assert_called_once()
        mock_register_schedule.assert_has_calls([
            mock.call({'id': 'schedule1'}, date=mock.ANY),
            mock.call({'id': 'schedule2'}, date=mock.ANY),
        ])

    @mock.patch.object(rpc_client.ConductorClient, 'get_replica_schedules')
    def test__get_all_schedules(self, mock_get_replica_schedules):
        result = self.server._get_all_schedules()

        mock_get_replica_schedules.assert_called_once_with(
            self.server._admin_ctx, expired=False)

        self.assertEqual(result, mock_get_replica_schedules.return_value)

    @mock.patch.object(server.ReplicaCronServerEndpoint, '_register_schedule')
    @mock.patch.object(server.timeutils, 'utcnow')
    def test_register(self, mock_utcnow, mock_register_schedule):
        mock_utcnow.return_value = datetime.datetime(2022, 1, 1)
        schedule = {'id': 'schedule1', 'schedule': 'schedule_data'}

        self.server.register(mock.sentinel.ctxt, schedule)

        mock_utcnow.assert_called_once()
        mock_register_schedule.assert_called_once_with(
            schedule, date=mock_utcnow.return_value)

    @mock.patch.object(server.cron.Cron, 'unregister')
    def test_unregister(self, mock_unregister):
        schedule = {'id': 'schedule1'}

        self.server.unregister(mock.sentinel.ctxt, schedule)

        mock_unregister.assert_called_once_with(schedule['id'])

    @mock.patch.object(server.utils, 'get_diagnostics_info')
    def test_get_diagnostics(self, mock_get_diagnostics_info):
        result = self.server.get_diagnostics(mock.sentinel.ctxt)

        mock_get_diagnostics_info.assert_called_once_with()

        self.assertEqual(result, mock_get_diagnostics_info.return_value)
