# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.tests import test_base
from coriolis.transfer_cron import api as transfers_cron_module


class APITestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis API class."""

    def setUp(self):
        super(APITestCase, self).setUp()
        self.api = transfers_cron_module.API()
        self.rpc_client = mock.MagicMock()
        self.api._rpc_client = self.rpc_client
        self.ctxt = mock.sentinel.ctxt
        self.transfer_id = mock.sentinel.transfer_id
        self.schedule_id = mock.sentinel.schedule_id

    def test_create(self):
        schedule = mock.sentinel.schedule
        enabled = mock.sentinel.enabled
        exp_date = mock.sentinel.exp_date
        shutdown_instance = mock.sentinel.shutdown_instance
        auto_deploy = mock.sentinel.auto_deploy

        result = self.api.create(
            self.ctxt, self.transfer_id, schedule, enabled, exp_date,
            shutdown_instance, auto_deploy)

        self.rpc_client.create_transfer_schedule.assert_called_once_with(
            self.ctxt, self.transfer_id, schedule, enabled, exp_date,
            shutdown_instance, auto_deploy)
        self.assertEqual(result,
                         self.rpc_client.create_transfer_schedule.return_value)

    def test_get_schedules(self):
        result = self.api.get_schedules(self.ctxt, self.transfer_id)

        self.rpc_client.get_transfer_schedules.assert_called_once_with(
            self.ctxt, self.transfer_id, expired=True)
        self.assertEqual(result,
                         self.rpc_client.get_transfer_schedules.return_value)

    def test_get_schedule(self):
        result = self.api.get_schedule(self.ctxt, self.transfer_id,
                                       self.schedule_id)

        self.rpc_client.get_transfer_schedule.assert_called_once_with(
            self.ctxt, self.transfer_id, self.schedule_id, expired=True)
        self.assertEqual(result,
                         self.rpc_client.get_transfer_schedule.return_value)

    def test_update(self):
        update_values = mock.sentinel.update_values

        result = self.api.update(self.ctxt, self.transfer_id, self.schedule_id,
                                 update_values)

        self.rpc_client.update_transfer_schedule.assert_called_once_with(
            self.ctxt, self.transfer_id, self.schedule_id, update_values)
        self.assertEqual(result,
                         self.rpc_client.update_transfer_schedule.return_value)

    def test_delete(self):
        self.api.delete(self.ctxt, self.transfer_id, self.schedule_id)
        self.rpc_client.delete_transfer_schedule.assert_called_once_with(
            self.ctxt, self.transfer_id, self.schedule_id)
