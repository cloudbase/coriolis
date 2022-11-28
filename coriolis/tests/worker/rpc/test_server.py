# Copyright 2022 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock
import os
import signal
import psutil
from coriolis.worker.rpc import server
from coriolis.tests import test_base
from coriolis.providers import factory as providers_factory
from coriolis import schemas, constants, utils, exception


class WorkerServerEndpointTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Worker RPC server."""

    @mock.patch.object(server.WorkerServerEndpoint, '_register_worker_service')
    def setUp(self, _):
        super(WorkerServerEndpointTestCase, self).setUp()
        self.server = server.WorkerServerEndpoint()

    @mock.patch.object(psutil, 'Process')
    def test_cancel_task(self, mock_process):
        self.server.cancel_task(
            mock.sentinel.context, mock.sentinel.task_id, mock.sentinel.proccess_id, False)

        # Cancel task should be called with send_signal when not forced
        mock_process.return_value.send_signal.assert_called_once_with(
            signal.SIGINT)
        mock_process.return_value.kill.assert_not_called()

        # Cancel task should be called with kill when forced
        mock_process.reset_mock()
        self.server.cancel_task(
            mock.sentinel.context, mock.sentinel.task_id, mock.sentinel.proccess_id, True)

        mock_process.return_value.send_signal.assert_not_called()
        mock_process.return_value.kill.assert_called_once()

        # If windows, kill should be called
        mock_process.reset_mock()
        with mock.patch.object(os, 'name', 'nt'):
            self.server.cancel_task(
                mock.sentinel.context, mock.sentinel.task_id, mock.sentinel.proccess_id, False)

            mock_process.return_value.send_signal.assert_not_called()
            mock_process.return_value.kill.assert_called_once()

        # If process is not found it should just confirm task is cancelled
        mock_process.reset_mock()
        mock_process.side_effect = psutil.NoSuchProcess(
            mock.sentinel.proccess_id)
        with mock.patch.object(server.WorkerServerEndpoint, '_rpc_conductor_client') as mock_client:
            self.server.cancel_task(
                mock.sentinel.context, mock.sentinel.task_id, mock.sentinel.proccess_id, False)
            mock_client.confirm_task_cancellation.assert_called_once()

    @mock.patch.object(server.WorkerServerEndpoint, '_exec_task_process')
    @mock.patch.object(server.WorkerServerEndpoint, '_rpc_conductor_client')
    @mock.patch.object(utils, 'sanitize_task_info')
    def test_exec_task(self, mock_sanitize, mock_client, mock_exec):
        mock_exec.return_value = mock.sentinel.task_result

        def call_exec_task(report_to_conductor=True):
            return self.server.exec_task(mock.sentinel.context, mock.sentinel.task_id,
                                         mock.sentinel.task_type, mock.sentinel.origin,
                                         mock.sentinel.destination, mock.sentinel.instance,
                                         mock.sentinel.task_info, report_to_conductor)

        # Calling without reporting to conductor
        result = call_exec_task(False)

        mock_exec.assert_called_once_with(mock.sentinel.context, mock.sentinel.task_id,
                                          mock.sentinel.task_type, mock.sentinel.origin,
                                          mock.sentinel.destination, mock.sentinel.instance,
                                          mock.sentinel.task_info, report_to_conductor=False)

        # If not report_to_conductor, the task result should be returned
        self.assertEqual(mock.sentinel.task_result, result)

        mock_exec.reset_mock()
        result = call_exec_task()

        mock_exec.assert_called_once_with(mock.sentinel.context, mock.sentinel.task_id,
                                          mock.sentinel.task_type, mock.sentinel.origin,
                                          mock.sentinel.destination, mock.sentinel.instance,
                                          mock.sentinel.task_info, report_to_conductor=True)

        # If report_to_conductor, None is returned and conductor client is called
        self.assertEqual(result, None)
        mock_client.task_completed.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.task_id, mock.sentinel.task_result)

        # TaskProcessCanceledException handling when reporting to conductor
        mock_exec.reset_mock()
        mock_client.reset_mock()
        mock_exec.side_effect = exception.TaskProcessCanceledException(
            'mock_message')
        call_exec_task()
        mock_client.confirm_task_cancellation.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.task_id, 'mock_message')

        # TaskProcessCanceledException handling when not reporting to conductor
        mock_client.reset_mock()
        mock_exec.reset_mock()
        self.assertRaises(
            exception.TaskProcessCanceledException, lambda: call_exec_task(False))
        mock_client.confirm_task_cancellation.assert_not_called()

        # NoSuitableWorkerServiceError handling when reporting to conductor
        mock_client.reset_mock()
        mock_exec.reset_mock()
        mock_exec.side_effect = exception.NoSuitableWorkerServiceError()
        # No exception should be raised
        call_exec_task()

        # NoSuitableWorkerServiceError handling when not reporting to conductor
        mock_client.reset_mock()
        mock_exec.reset_mock()
        mock_exec.side_effect = exception.NoSuitableWorkerServiceError()
        self.assertRaises(
            exception.NoSuitableWorkerServiceError, lambda: call_exec_task(False))

        # Exception handling when reporting to conductor
        mock_client.reset_mock()
        mock_exec.reset_mock()
        mock_exec.side_effect = Exception('mock_message')
        call_exec_task()
        mock_client.set_task_error.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.task_id, 'mock_message')

        # Exception handling when not reporting to conductor
        mock_client.reset_mock()
        mock_exec.reset_mock()
        mock_exec.side_effect = Exception('mock_message')
        self.assertRaises(Exception, lambda: call_exec_task(False))

    @mock.patch.object(schemas, 'validate_value')
    @mock.patch.object(utils, 'get_secret_connection_info')
    @mock.patch.object(providers_factory, 'get_provider')
    def test_get_endpoint_instances(self, mock_get_provider, mock_get_secret, mock_validate):
        # return_value is a list of instances
        mock_get_provider.return_value.get_instances.return_value = [
            mock.sentinel.instance_1, mock.sentinel.instance_2]

        instances_info = self.server.get_endpoint_instances(mock.sentinel.context, mock.sentinel.platform_name,
                                                            mock.sentinel.connection_info, mock.sentinel.source_environment,
                                                            mock.sentinel.marker, mock.sentinel.limit,
                                                            mock.sentinel.instance_name_pattern)

        mock_get_provider.assert_called_once_with(
            mock.sentinel.platform_name, constants.PROVIDER_TYPE_ENDPOINT_INSTANCES, None)
        mock_get_secret.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.connection_info)
        mock_get_provider.return_value.get_instances.assert_called_once_with(
            mock.sentinel.context, mock_get_secret.return_value,
            mock.sentinel.source_environment, last_seen_id=mock.sentinel.marker,
            limit=mock.sentinel.limit, instance_name_pattern=mock.sentinel.instance_name_pattern)

        # values are validated for each instance returned by the provider
        mock_validate.assert_has_calls([
            mock.call(mock.sentinel.instance_1,
                      schemas.CORIOLIS_VM_INSTANCE_INFO_SCHEMA),
            mock.call(mock.sentinel.instance_2, schemas.CORIOLIS_VM_INSTANCE_INFO_SCHEMA)])

        # the validated values are returned
        self.assertEqual(instances_info, [
                         mock.sentinel.instance_1, mock.sentinel.instance_2])

    @mock.patch.object(schemas, 'validate_value')
    @mock.patch.object(utils, 'get_secret_connection_info')
    @mock.patch.object(providers_factory, 'get_provider')
    def test_get_endpoint_instance(self, mock_get_provider, mock_get_secret, mock_validate):
        # return_value is a single instance
        mock_get_provider.return_value.get_instance.return_value = mock.sentinel.instance

        instance_info = self.server.get_endpoint_instance(mock.sentinel.context, mock.sentinel.platform_name,
                                                          mock.sentinel.connection_info, mock.sentinel.source_environment,
                                                          mock.sentinel.instance_name)

        mock_get_provider.assert_called_once_with(
            mock.sentinel.platform_name, constants.PROVIDER_TYPE_ENDPOINT_INSTANCES, None)
        mock_get_secret.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.connection_info)
        mock_get_provider.return_value.get_instance.assert_called_once_with(
            mock.sentinel.context, mock_get_secret.return_value,
            mock.sentinel.source_environment, mock.sentinel.instance_name)

        # value is validated
        mock_validate.assert_called_once_with(
            mock.sentinel.instance, schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA)

        # the validated value is returned
        self.assertEqual(instance_info, mock.sentinel.instance)
