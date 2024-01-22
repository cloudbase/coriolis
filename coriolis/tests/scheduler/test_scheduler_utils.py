# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import constants
from coriolis import exception
from coriolis.scheduler import scheduler_utils
from coriolis.tests import test_base


class CoriolisTestException(Exception):
    pass


class SchedulerUtilsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis scheduler utils package."""

    def setUp(self):
        super(SchedulerUtilsTestCase, self).setUp()
        self.scheduler_client = mock.MagicMock()
        self.rpc_client_class = mock.MagicMock()
        self.service = mock.MagicMock()
        self.ctxt = mock.MagicMock()

    def test_get_rpc_client_for_service(self):
        with mock.patch.dict(
            scheduler_utils.RPC_TOPIC_TO_CLIENT_CLASS_MAP,
            {constants.WORKER_MAIN_MESSAGING_TOPIC: self.rpc_client_class},
            clear=True
        ):
            self.service.topic = constants.WORKER_MAIN_MESSAGING_TOPIC
            self.service.host = 'test_host'

            result = scheduler_utils.get_rpc_client_for_service(self.service)

            self.rpc_client_class.assert_called_once_with(
                topic='coriolis_worker.test_host')

            self.assertEqual(result, self.rpc_client_class.return_value)

    def test_get_rpc_client_for_service_different_topic(self):
        with mock.patch.dict(
            scheduler_utils.RPC_TOPIC_TO_CLIENT_CLASS_MAP,
            {mock.sentinel.topic: self.rpc_client_class},
            clear=True
        ):
            self.service.topic = mock.sentinel.topic
            self.service.host = 'host'

            result = scheduler_utils.get_rpc_client_for_service(self.service)

            self.rpc_client_class.assert_called_once_with(
                topic=mock.sentinel.topic)

            self.assertEqual(result, self.rpc_client_class.return_value)

    def test_get_rpc_client_for_service_with_exception(self):
        self.service.topic = 'non-existent-topic'
        self.service.host = 'host'

        self.assertRaises(exception.NotFound,
                          scheduler_utils.get_rpc_client_for_service,
                          self.service)

    def test_get_any_worker_service_no_services(self):
        self.scheduler_client.get_workers_for_specs.return_value = []

        self.assertRaises(exception.NoWorkerServiceError,
                          scheduler_utils.get_any_worker_service,
                          self.scheduler_client, self.ctxt)

    @mock.patch('coriolis.scheduler.scheduler_utils.db_api.get_service')
    @mock.patch('random.choice')
    def test_get_any_worker_service_random_choice(self, mock_random_choice,
                                                  get_service_mock):
        service_mock1 = {'id': 'test_id1'}
        service_mock2 = {'id': 'test_id2'}

        self.scheduler_client.get_workers_for_specs.return_value = [
            service_mock1, service_mock2]

        get_service_mock.return_value = [service_mock1, service_mock2]
        mock_random_choice.return_value = service_mock1

        result = scheduler_utils.get_any_worker_service(
            self.scheduler_client, self.ctxt, random_choice=True)

        mock_random_choice.assert_called_once_with([
            service_mock1, service_mock2])
        get_service_mock.assert_called_once_with(
            self.ctxt, service_mock1['id'])
        self.assertEqual(result, get_service_mock.return_value)

    @mock.patch('coriolis.scheduler.scheduler_utils.db_api.get_service')
    def test_get_any_worker_service_raw_dict(self, get_service_mock):
        service_mock = {'id': 'test_id'}

        self.scheduler_client.get_workers_for_specs.return_value = [
            service_mock]

        result = scheduler_utils.get_any_worker_service(
            self.scheduler_client, self.ctxt, raw_dict=True)

        get_service_mock.assert_not_called()
        self.assertEqual(result, service_mock)

    def test_get_worker_rpc_for_host(self):
        with mock.patch.dict(
            scheduler_utils.RPC_TOPIC_TO_CLIENT_CLASS_MAP,
            {constants.WORKER_MAIN_MESSAGING_TOPIC: self.rpc_client_class},
            clear=True
        ):
            host = 'test_host'
            client_args = ('arg1', 'arg2')
            client_kwargs = {'key1': 'value1', 'key2': 'value2'}

            result = scheduler_utils.get_worker_rpc_for_host(
                host, *client_args, **client_kwargs)

            self.rpc_client_class.assert_called_once_with(
                *client_args, topic='coriolis_worker.test_host',
                **client_kwargs)

            self.assertEqual(result, self.rpc_client_class.return_value)
