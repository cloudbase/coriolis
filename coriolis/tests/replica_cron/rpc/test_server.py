# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

import datetime
import ddt

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
            mock.sentinel.transfer_id, False)

        mock_conductor_client.execute_replica_tasks.assert_called_once_with(
            mock.sentinel.ctxt, mock.sentinel.transfer_id, False)

        self.assertEqual(
            result, 'Execution %s for Replica %s' % (
                mock.sentinel.id, mock.sentinel.action_id))

    def test__trigger_replica_invalid_replica_state(self):
        mock_conductor_client = mock.MagicMock()

        mock_conductor_client.execute_replica_tasks.side_effect = (
            exception.InvalidReplicaState(reason='test_reason'))

        with self.assertLogs('coriolis.replica_cron.rpc.server',
                             level=logging.INFO):
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
                'expiration_date': '2024-03-28T22:00:00.000000+02:00',
                'schedule': '{"hour": 22, "minute": 0, "month": 7}'
            },
            'expected': {
                'expiration_date': datetime.datetime(2024, 3, 28, 20, 0),
                'schedule': {"hour": 22, "minute": 0, "month": 7}
            }
        },
    )
    def test__deserialize_schedule(self, data):
        result = self.server._deserialize_schedule(data['input'])
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

        with self.assertLogs('coriolis.replica_cron.rpc.server',
                             level=logging.INFO):
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

        with self.assertLogs('coriolis.replica_cron.rpc.server',
                             level=logging.ERROR):
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
