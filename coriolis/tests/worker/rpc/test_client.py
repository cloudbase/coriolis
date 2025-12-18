# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import oslo_messaging

from coriolis import constants
from coriolis.tests import test_base
from coriolis.worker.rpc import client

ENDPOINT_INSTANCE_ARGS = {
    "platform_name": "platform",
    "connection_info": "conn_info",
    "source_environment": "source_env",
}
ENDPOINT_RESOURCE_ARGS = {
    "platform_name": "platform",
    "connection_info": "conn_info",
    "env": "env",
}
ENDPOINT_OPT_ARGS = {
    **ENDPOINT_RESOURCE_ARGS,
    "option_names": ["opt1", "opt2"],
}
EXEC_TASK_ARGS = {
    "task_id": "task_id1",
    "task_type": "type1",
    "origin": "origin",
    "destination": "dest",
    "instance": "instance1",
    "task_info": {"key1": "info1", "key2": "info2"},
}
POOL_VALIDATION_ARGS = {
    "platform_name": "platform",
    "pool_environment": {"opt1": "value1", "opt2": "value2"},
}


class WorkerClientTestCase(test_base.CoriolisRPCClientTestCase):

    def setUp(self):
        super(WorkerClientTestCase, self).setUp()
        self.client = client.WorkerClient()

    @mock.patch('coriolis.worker.rpc.client.CONF')
    @mock.patch.object(oslo_messaging, 'Target')
    def test__init__default_args(self, mock_target, mock_conf):
        expected_timeout = 120
        mock_conf.worker.worker_rpc_timeout = expected_timeout
        result = client.WorkerClient()
        mock_target.assert_called_once_with(
            topic=constants.WORKER_MAIN_MESSAGING_TOPIC,
            version=client.VERSION)
        self.assertEqual(result._target, mock_target.return_value)
        self.assertEqual(
            result._timeout, expected_timeout)

    @mock.patch.object(oslo_messaging, 'Target')
    def test__init__custom_args(self, mock_target):
        expected_timeout = 40
        host = 'host'
        topic = 'topic'
        expected_topic = "topic.host"

        result = client.WorkerClient(
            timeout=expected_timeout, host=host, base_worker_topic=topic)
        mock_target.assert_called_once_with(
            topic=expected_topic, version=client.VERSION)
        self.assertEqual(result._target, mock_target.return_value)
        self.assertEqual(result._timeout, expected_timeout)

    def test_from_service_definition_invalid_topic(self):
        self.assertRaises(
            ValueError, self.client.from_service_definition,
            {'topic': 'invalid_service'})

    def test_from_service_definition(self):
        rpc_client = self.client.from_service_definition(
            {'topic': constants.WORKER_MAIN_MESSAGING_TOPIC})
        self.assertIsInstance(rpc_client, client.WorkerClient)

    def test_begin_task(self):
        args = EXEC_TASK_ARGS
        custom_args = {"report_to_conductor": True}
        self._test(
            self.client.begin_task, args, rpc_op='_cast',
            server_fun_name='exec_task', custom_args=custom_args)

    def test_run_task(self):
        args = EXEC_TASK_ARGS
        custom_args = {"report_to_conductor": False}
        self._test(self.client.run_task, args, server_fun_name='exec_task',
                   custom_args=custom_args)

    def test_cancel_task(self):
        args = {
            "task_id": "id1",
            "process_id": "pid1",
            "force": False,
        }
        self._test(self.client.cancel_task, args)

    def test_get_endpoint_instances(self):
        args = {
            **ENDPOINT_INSTANCE_ARGS,
            'marker': None,
            'limit': None,
            'instance_name_pattern': None,
            'refresh': False
        }
        self._test(self.client.get_endpoint_instances, args)

    def test_get_endpoint_instance(self):
        args = {
            **ENDPOINT_INSTANCE_ARGS,
            'instance_name': 'instance1',
        }
        self._test(self.client.get_endpoint_instance, args)

    def test_get_endpoint_destination_options(self):
        self._test(
            self.client.get_endpoint_destination_options, ENDPOINT_OPT_ARGS)

    def test_get_endpoint_source_options(self):
        self._test(self.client.get_endpoint_source_options, ENDPOINT_OPT_ARGS)

    def test_get_endpoint_networks(self):
        self._test(self.client.get_endpoint_networks, ENDPOINT_RESOURCE_ARGS)

    def test_validate_endpoint_connection(self):
        args = {
            "platform_name": "platform",
            "connection_info": {"user": "admin", "password": "S3kr3t"},
        }
        self._test(self.client.validate_endpoint_connection, args)

    def test_validate_endpoint_target_environment(self):
        args = {
            "platform_name": "platform",
            "target_env": {"option1": "value1", "option2": "value2"},
        }
        self._test(self.client.validate_endpoint_target_environment, args)

    def test_validate_endpoint_source_environment(self):
        args = {
            "platform_name": "platform",
            "source_env": {"option1": "value1", "option2": "value2"},
        }
        self._test(self.client.validate_endpoint_source_environment, args)

    def test_get_endpoint_storage(self):
        self._test(self.client.get_endpoint_storage, ENDPOINT_RESOURCE_ARGS)

    def test_get_available_providers(self):
        self._test(self.client.get_available_providers, {})

    def test_get_provider_schemas(self):
        args = {
            "platform_name": "platform",
            "provider_type": "type1",
        }
        self._test(self.client.get_provider_schemas, args)

    def test_get_diagnostics(self):
        self._test(self.client.get_diagnostics, {})

    def test_get_service_status(self):
        self._test(self.client.get_service_status, {})

    def test_get_endpoint_source_minion_pool_options(self):
        self._test(self.client.get_endpoint_source_minion_pool_options,
                   ENDPOINT_OPT_ARGS)

    def test_get_endpoint_minion_pool_options(self):
        self._test(self.client.get_endpoint_destination_minion_pool_options,
                   ENDPOINT_OPT_ARGS)

    def test_validate_endpoint_source_minion_pool_options(self):
        self._test(self.client.validate_endpoint_source_minion_pool_options,
                   POOL_VALIDATION_ARGS)

    def test_validate_endpoint_destination_minion_pool_options(self):
        self._test(
            self.client.validate_endpoint_destination_minion_pool_options,
            POOL_VALIDATION_ARGS)
