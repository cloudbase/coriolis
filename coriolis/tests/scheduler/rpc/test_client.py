# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

import oslo_messaging

from coriolis import constants
from coriolis import exception
from coriolis.scheduler.rpc import client
from coriolis.tasks import factory as tasks_factory
from coriolis.tests import test_base


class SchedulerClientTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Scheduler Worker RPC client."""

    def setUp(self):
        super(SchedulerClientTestCase, self).setUp()
        self.client = client.SchedulerClient()
        self.task = {'id': 'task_id', 'task_type': 'task_type'}
        self.origin_endpoint = {
            'id': 'origin_id',
            'mapped_regions': [{'id': 'region1'}, {'id': 'region2'}],
            'type': 'origin_type'
        }
        self.destination_endpoint = {
            'id': 'destination_id',
            'mapped_regions': [{'id': 'region3'}, {'id': 'region4'}],
            'type': 'destination_type'
        }

    @mock.patch('coriolis.scheduler.rpc.client.CONF')
    @mock.patch.object(oslo_messaging, 'Target')
    def test__init__(self, mock_target, mock_conf):
        expected_timeout = 120
        mock_conf.scheduler.scheduler_rpc_timeout = expected_timeout

        result = client.SchedulerClient()
        mock_target.assert_called_once_with(
            topic='coriolis_scheduler', version=client.VERSION)

        self.assertEqual(result._target, mock_target.return_value)
        self.assertEqual(result._timeout, expected_timeout)

    def test__init__without_timeout(self):
        result = client.SchedulerClient()
        self.assertEqual(result._timeout, 60)

    def test__init__with_timeout(self):
        result = client.SchedulerClient(timeout=120)
        self.assertEqual(result._timeout, 120)

    @mock.patch.object(client.SchedulerClient, '_call')
    def test_get_diagnostics(self, mock_call):
        ctxt = mock.sentinel.ctxt
        result = self.client.get_diagnostics(ctxt)

        mock_call.assert_called_once_with(ctxt, 'get_diagnostics')
        self.assertEqual(result, mock_call.return_value)

    @mock.patch.object(client.SchedulerClient, '_call')
    def test_get_workers_for_specs(self, mock_call):
        ctxt = mock.sentinel.ctxt
        provider_requirements = mock.sentinel.provider_requirements
        region_sets = mock.sentinel.region_sets
        enabled = mock.sentinel.enabled

        result = self.client.get_workers_for_specs(
            ctxt, provider_requirements=provider_requirements,
            region_sets=region_sets, enabled=enabled)

        mock_call.assert_called_once_with(
            ctxt, 'get_workers_for_specs', region_sets=region_sets,
            enabled=enabled, provider_requirements=provider_requirements)
        self.assertEqual(result, mock_call.return_value)

    @mock.patch('random.choice')
    @mock.patch.object(client.SchedulerClient, 'get_workers_for_specs')
    def test_get_any_worker_service(self, mock_get_workers_for_specs,
                                    mock_random_choice):
        ctxt = mock.sentinel.ctxt
        raise_if_none = mock.sentinel.raise_if_none
        mock_service = mock.MagicMock()
        mock_get_workers_for_specs.return_value = mock_service
        mock_random_choice.return_value = mock_service

        result = self.client.get_any_worker_service(
            ctxt, raise_if_none=raise_if_none, random_choice=True)

        mock_get_workers_for_specs.assert_called_once_with(ctxt)
        mock_random_choice.assert_called_once_with(mock_service)
        self.assertEqual(result, mock_service)

    @mock.patch.object(client.SchedulerClient, 'get_workers_for_specs')
    def test_get_any_worker_service_no_services_no_raise(self,
                                                         mock_get_workers):
        mock_get_workers.return_value = []
        result = self.client.get_any_worker_service(
            mock.sentinel.ctxt, raise_if_none=False)
        self.assertIsNone(result)

    @mock.patch.object(client.SchedulerClient, 'get_workers_for_specs')
    def test_get_any_worker_service_no_services(self, mock_get_workers):
        mock_get_workers.return_value = []
        self.assertRaises(
            exception.NoWorkerServiceError,
            self.client.get_any_worker_service, mock.sentinel.ctxt)

    @mock.patch.object(client.SchedulerClient, 'get_workers_for_specs')
    def test_get_any_worker_service_random_choice(self, mock_get_workers):
        service_mock1 = {'id': 'test_id1'}
        service_mock2 = {'id': 'test_id2'}
        mock_get_workers.return_value = [service_mock1, service_mock2]

        result = self.client.get_any_worker_service(
            mock.sentinel.ctxt, random_choice=True)

        self.assertIsInstance(result, dict)

    @mock.patch.object(client.SchedulerClient, '_call')
    def test_get_worker_service_for_specs(self, mock_call):
        ctxt = mock.sentinel.ctxt
        provider_requirements = mock.sentinel.provider_requirements
        region_sets = mock.sentinel.region_sets
        enabled = mock.sentinel.enabled
        raise_on_no_matches = mock.sentinel.raise_on_no_matches

        self.client.get_worker_service_for_specs(
            ctxt, provider_requirements=provider_requirements,
            region_sets=region_sets, enabled=enabled,
            raise_on_no_matches=raise_on_no_matches)

        mock_call.assert_called_once_with(
            ctxt, 'get_workers_for_specs', region_sets=region_sets,
            enabled=enabled, provider_requirements=provider_requirements)

    @mock.patch.object(client.SchedulerClient, 'get_workers_for_specs')
    def test_get_worker_service_for_specs_no_services_no_raise(
            self, mock_get_workers):
        mock_get_workers.return_value = []
        result = self.client.get_worker_service_for_specs(
            mock.sentinel.ctxt, raise_on_no_matches=False)
        self.assertIsNone(result)

    @mock.patch.object(client.SchedulerClient, 'get_workers_for_specs')
    def test_get_worker_service_for_specs_no_services(self, mock_get_workers):
        mock_get_workers.return_value = []
        self.assertRaises(
            exception.NoSuitableWorkerServiceError,
            self.client.get_worker_service_for_specs, mock.sentinel.ctxt)

    @mock.patch.object(client.SchedulerClient, 'get_workers_for_specs')
    @mock.patch('random.choice')
    def test_get_worker_service_for_specs_random_choice(
            self, mock_random_choice, mock_get_workers):
        service_mock1 = {'id': 'test_id1'}
        service_mock2 = {'id': 'test_id2'}
        mock_get_workers.return_value = [service_mock1, service_mock2]
        mock_random_choice.return_value = service_mock1

        result = self.client.get_worker_service_for_specs(
            mock.sentinel.ctxt, random_choice=True)

        mock_random_choice.assert_called_once_with([
            service_mock1, service_mock2])
        mock_get_workers.assert_called_once_with(
            mock.sentinel.ctxt, provider_requirements=None, region_sets=None,
            enabled=True)
        self.assertEqual(result, service_mock1)

    @mock.patch('random.choice')
    @mock.patch.object(client.SchedulerClient, 'get_workers_for_specs')
    def test_get_worker_service_for_specs_no_random_choice(
            self, get_workers_for_specs, mock_random_choice):
        mock_service = mock.MagicMock()
        get_workers_for_specs.return_value = [mock_service]

        result = self.client.get_worker_service_for_specs(
            mock.sentinel.ctxt, random_choice=False)

        mock_random_choice.assert_not_called()
        get_workers_for_specs.assert_called_once_with(
            mock.sentinel.ctxt, provider_requirements=None, region_sets=None,
            enabled=True)
        self.assertEqual(result, mock_service)

    @mock.patch.object(client.SchedulerClient, 'get_worker_service_for_specs')
    @mock.patch.object(tasks_factory, 'get_task_runner_class')
    def test_get_worker_service_for_task_different_platforms(
            self, mock_get_task_runner_class, get_worker_service_for_specs):
        for platform in [constants.TASK_PLATFORM_SOURCE,
                         constants.TASK_PLATFORM_DESTINATION,
                         constants.TASK_PLATFORM_BILATERAL]:
            mock_get_task_runner_class.return_value.get_required_platform.\
                return_value = platform
            mock_get_task_runner_class.return_value.\
                get_required_provider_types.return_value = {
                    constants.PROVIDER_PLATFORM_SOURCE: 'provider_type'}
            get_worker_service_for_specs.return_value = {'id': 'test_id'}

            result = self.client.get_worker_service_for_task(
                mock.sentinel.ctxt, self.task, self.origin_endpoint,
                self.destination_endpoint, retry_period=0)

            self.assertEqual(result, {'id': 'test_id'})
            self.assertIsInstance(result, dict)

    @mock.patch.object(client.SchedulerClient, 'get_worker_service_for_specs')
    @mock.patch.object(tasks_factory, 'get_task_runner_class')
    def test_get_worker_service_for_task_retry(
            self, mock_get_task_runner_class, mock_get_worker_service):
        mock_get_task_runner_class.return_value.get_required_platform.\
            return_value = constants.TASK_PLATFORM_SOURCE
        mock_get_task_runner_class.return_value.get_required_provider_types.\
            return_value = {constants.PROVIDER_PLATFORM_DESTINATION:
                            'provider_type'}
        mock_get_worker_service.side_effect = [Exception(), {'id': 'test_id'}]

        with self.assertLogs('coriolis.scheduler.rpc.client',
                             level=logging.WARN):
            self.client.get_worker_service_for_task(
                mock.sentinel.ctxt, self.task, self.origin_endpoint,
                self.destination_endpoint, retry_period=0)

    @mock.patch.object(client.SchedulerClient, 'get_worker_service_for_specs')
    @mock.patch.object(tasks_factory, 'get_task_runner_class')
    def test_get_worker_service_for_task_no_suitable_worker(
            self, mock_get_task_runner_class, mock_get_worker_service):
        mock_get_task_runner_class.return_value.get_required_platform.\
            return_value = constants.TASK_PLATFORM_SOURCE
        mock_get_task_runner_class.return_value.get_required_provider_types.\
            return_value = {constants.PROVIDER_PLATFORM_SOURCE: 'type'}
        mock_get_worker_service.side_effect = [
            exception.NoSuitableWorkerServiceError()]

        self.assertRaises(
            exception.NoSuitableWorkerServiceError,
            self.client.get_worker_service_for_task, mock.sentinel.ctxt,
            self.task, self.origin_endpoint, self.destination_endpoint,
            retry_period=0, retry_count=0)
