# Copyright 2022 Cloudbase Solutions Srl
# All Rights Reserved.

import multiprocessing
import os
import signal
from unittest import mock

import ddt
import eventlet
import psutil

from coriolis import constants
from coriolis import exception
from coriolis.providers import factory as providers_factory
from coriolis import schemas
from coriolis.tests import test_base
from coriolis import utils
from coriolis.worker.rpc import server


@ddt.ddt
class WorkerServerEndpointTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Worker RPC server."""

    @mock.patch.object(server.WorkerServerEndpoint,
                       "_register_worker_service")
    def setUp(self, _):  # pylint: disable=arguments-differ
        super(WorkerServerEndpointTestCase, self).setUp()
        self.server = server.WorkerServerEndpoint()

    @mock.patch.object(server.WorkerServerEndpoint,
                       "_start_process_with_custom_library_paths")
    @mock.patch.object(server, "_task_process")
    @mock.patch.object(eventlet, "spawn")
    @mock.patch.object(server.WorkerServerEndpoint, "_rpc_conductor_client")
    @mock.patch.object(
        server.WorkerServerEndpoint, "_get_extra_library_paths_for_providers"
    )
    @mock.patch.object(multiprocessing, "get_context")
    def test_exec_task_process(
        self,
        mock_process_context,
        mock_get_extra_lib_paths,
        mock_rpc_client,
        mock_spawn,
        mock_task_process,
        mock_start_process,
    ):
        def call_exec_task_process(report_to_conductor=True):
            return self.server._exec_task_process(
                mock.sentinel.context,
                mock.sentinel.task_id,
                mock.sentinel.task_type,
                mock.sentinel.origin,
                mock.sentinel.destination,
                mock.sentinel.instance,
                mock.sentinel.task_info,
                report_to_conductor=report_to_conductor,
            )

        call_exec_task_process()

        # Process context is called with correct arguments
        mock_process_context.return_value.Process.assert_called_once_with(
            target=mock_task_process,
            args=(
                mock.sentinel.context,
                mock.sentinel.task_id,
                mock.sentinel.task_type,
                mock.sentinel.origin,
                mock.sentinel.destination,
                mock.sentinel.instance,
                mock.sentinel.task_info,
                mock_process_context.return_value.Queue.return_value,
                mock_process_context.return_value.Queue.return_value,
            ),
        )

        mock_get_extra_lib_paths.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.task_type,
            mock.sentinel.origin,
            mock.sentinel.destination,
        )

        # Report to conductor is true, so set task host is called
        mock_rpc_client.set_task_host.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.task_id, utils.get_hostname()
        )

        mock_start_process.assert_called_once_with(
            mock_process_context.return_value.Process.return_value,
            mock_get_extra_lib_paths.return_value,
        )

        # Report to conductor is true, so set_task_process is called
        mock_rpc_client.set_task_process.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock_process_context.return_value.Process.return_value.pid,
        )

        mock_rpc_client.reset_mock()
        call_exec_task_process(report_to_conductor=False)
        # Report to conductor is false, so set_task_host and
        # set_task_process is not called
        mock_rpc_client.set_task_host.assert_not_called()
        mock_rpc_client.set_task_process.assert_not_called()

        # Exception and KeyboardInterrupt are caught
        mock_rpc_client.set_task_host.side_effect = KeyboardInterrupt
        self.assertRaises(KeyboardInterrupt, call_exec_task_process)

        # If task cancelling text is in the exception message,
        # raise TaskIsCancelling
        mock_rpc_client.set_task_host.side_effect = Exception(
            f"Task {mock.sentinel.task_id} is in CANCELLING status."
        )

        self.assertRaises(exception.TaskIsCancelling, call_exec_task_process)

        # Returns the spawned process result
        mock_rpc_client.set_task_host.side_effect = None
        result = call_exec_task_process()
        self.assertEqual(result, mock_spawn.return_value.wait.return_value)

        # if return value is None, raise TaskProcessCanceledException
        mock_spawn.return_value.wait.return_value = None
        self.assertRaises(
            exception.TaskProcessCanceledException, call_exec_task_process
        )

        # if return value is string, raise TaskProcessException
        mock_spawn.return_value.wait.return_value = "Test string"
        self.assertRaises(
            exception.TaskProcessException, call_exec_task_process
        )

    @mock.patch.object(psutil, "Process")
    def test_cancel_task(self, mock_process):
        self.server.cancel_task(
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.proccess_id,
            False,
        )

        # Cancel task should be called with send_signal when not forced
        mock_process.return_value.send_signal.assert_called_once_with(
            signal.SIGINT
        )
        mock_process.return_value.kill.assert_not_called()

        # Cancel task should be called with kill when forced
        mock_process.reset_mock()
        self.server.cancel_task(
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.proccess_id,
            True,
        )

        mock_process.return_value.send_signal.assert_not_called()
        mock_process.return_value.kill.assert_called_once()

        # If windows, kill should be called
        mock_process.reset_mock()
        with mock.patch.object(os, "name", "nt"):
            self.server.cancel_task(
                mock.sentinel.context,
                mock.sentinel.task_id,
                mock.sentinel.proccess_id,
                False,
            )

            mock_process.return_value.send_signal.assert_not_called()
            mock_process.return_value.kill.assert_called_once()

        # If process is not found it should just confirm task is cancelled
        mock_process.reset_mock()
        mock_process.side_effect = psutil.NoSuchProcess(
            mock.sentinel.proccess_id
        )
        with mock.patch.object(
            server.WorkerServerEndpoint, "_rpc_conductor_client"
        ) as mock_client:
            self.server.cancel_task(
                mock.sentinel.context,
                mock.sentinel.task_id,
                mock.sentinel.proccess_id,
                False,
            )
            mock_client.confirm_task_cancellation.assert_called_once()

    @mock.patch.object(server.WorkerServerEndpoint, "_exec_task_process")
    @mock.patch.object(server.WorkerServerEndpoint, "_rpc_conductor_client")
    @mock.patch.object(utils, "sanitize_task_info")
    def test_exec_task(self, _, mock_client, mock_exec):
        mock_exec.return_value = mock.sentinel.task_result

        def call_exec_task(report_to_conductor=True):
            return self.server.exec_task(
                mock.sentinel.context,
                mock.sentinel.task_id,
                mock.sentinel.task_type,
                mock.sentinel.origin,
                mock.sentinel.destination,
                mock.sentinel.instance,
                mock.sentinel.task_info,
                report_to_conductor,
            )

        # Calling without reporting to conductor
        result = call_exec_task(False)

        mock_exec.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.task_type,
            mock.sentinel.origin,
            mock.sentinel.destination,
            mock.sentinel.instance,
            mock.sentinel.task_info,
            report_to_conductor=False,
        )

        # If not report_to_conductor, the task result should be returned
        self.assertEqual(mock.sentinel.task_result, result)

        mock_exec.reset_mock()
        result = call_exec_task()

        mock_exec.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.task_type,
            mock.sentinel.origin,
            mock.sentinel.destination,
            mock.sentinel.instance,
            mock.sentinel.task_info,
            report_to_conductor=True,
        )

        # If report_to_conductor, None is returned
        # and conductor client is called
        self.assertEqual(result, None)
        mock_client.task_completed.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.task_result,
        )

        # TaskProcessCanceledException handling when reporting to conductor
        mock_exec.reset_mock()
        mock_client.reset_mock()
        mock_exec.side_effect = exception.TaskProcessCanceledException(
            "mock_message"
        )
        call_exec_task()
        mock_client.confirm_task_cancellation.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.task_id, "mock_message"
        )

        # TaskProcessCanceledException handling when not reporting to conductor
        mock_client.reset_mock()
        mock_exec.reset_mock()
        self.assertRaises(
            exception.TaskProcessCanceledException,
            lambda: call_exec_task(False),
        )
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
            exception.NoSuitableWorkerServiceError,
            lambda: call_exec_task(False),
        )

        # Exception handling when reporting to conductor
        mock_client.reset_mock()
        mock_exec.reset_mock()
        mock_exec.side_effect = Exception("mock_message")
        call_exec_task()
        mock_client.set_task_error.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.task_id, "mock_message"
        )

        # Exception handling when not reporting to conductor
        mock_client.reset_mock()
        mock_exec.reset_mock()
        mock_exec.side_effect = Exception("mock_message")
        self.assertRaises(Exception,  # noqa: H202
                          lambda: call_exec_task(False))

    @mock.patch.object(schemas, "validate_value")
    @mock.patch.object(utils, "get_secret_connection_info")
    @mock.patch.object(providers_factory, "get_provider")
    def test_get_endpoint_instances(
        self, mock_get_provider, mock_get_secret, mock_validate
    ):
        # return_value is a list of instances
        mock_get_provider.return_value.get_instances.return_value = [
            mock.sentinel.instance_1,
            mock.sentinel.instance_2,
        ]

        instances_info = self.server.get_endpoint_instances(
            mock.sentinel.context,
            mock.sentinel.platform_name,
            mock.sentinel.connection_info,
            mock.sentinel.source_environment,
            mock.sentinel.marker,
            mock.sentinel.limit,
            mock.sentinel.instance_name_pattern,
        )

        mock_get_provider.assert_called_once_with(
            mock.sentinel.platform_name,
            constants.PROVIDER_TYPE_ENDPOINT_INSTANCES,
            None,
        )
        mock_get_secret.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.connection_info
        )
        mock_get_provider.return_value.get_instances.assert_called_once_with(
            mock.sentinel.context,
            mock_get_secret.return_value,
            mock.sentinel.source_environment,
            last_seen_id=mock.sentinel.marker,
            limit=mock.sentinel.limit,
            instance_name_pattern=mock.sentinel.instance_name_pattern,
        )

        # values are validated for each instance returned by the provider
        mock_validate.assert_has_calls(
            [
                mock.call(
                    mock.sentinel.instance_1,
                    schemas.CORIOLIS_VM_INSTANCE_INFO_SCHEMA,
                ),
                mock.call(
                    mock.sentinel.instance_2,
                    schemas.CORIOLIS_VM_INSTANCE_INFO_SCHEMA,
                ),
            ]
        )

        # the validated values are returned
        self.assertEqual(
            instances_info,
            [mock.sentinel.instance_1, mock.sentinel.instance_2],
        )

    @mock.patch.object(schemas, "validate_value")
    @mock.patch.object(utils, "get_secret_connection_info")
    @mock.patch.object(providers_factory, "get_provider")
    def test_get_endpoint_instance(
        self, mock_get_provider, mock_get_secret, mock_validate
    ):
        # return_value is a single instance
        mock_get_provider.return_value.get_instance.return_value = (
            mock.sentinel.instance
        )

        instance_info = self.server.get_endpoint_instance(
            mock.sentinel.context,
            mock.sentinel.platform_name,
            mock.sentinel.connection_info,
            mock.sentinel.source_environment,
            mock.sentinel.instance_name,
        )

        mock_get_provider.assert_called_once_with(
            mock.sentinel.platform_name,
            constants.PROVIDER_TYPE_ENDPOINT_INSTANCES,
            None,
        )
        mock_get_secret.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.connection_info
        )
        mock_get_provider.return_value.get_instance.assert_called_once_with(
            mock.sentinel.context,
            mock_get_secret.return_value,
            mock.sentinel.source_environment,
            mock.sentinel.instance_name,
        )

        # value is validated
        mock_validate.assert_called_once_with(
            mock.sentinel.instance, schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA
        )

        # the validated value is returned
        self.assertEqual(instance_info, mock.sentinel.instance)

    @mock.patch.object(schemas, "validate_value")
    @mock.patch.object(utils, "get_secret_connection_info")
    @mock.patch.object(providers_factory, "get_provider")
    def test_get_endpoint_destination_options(
        self, mock_get_provider, mock_get_secret, mock_validate
    ):
        def call_get_endpoint_destination_options():
            return self.server.get_endpoint_destination_options(
                mock.sentinel.context,
                mock.sentinel.platform_name,
                mock.sentinel.connection_info,
                mock.sentinel.environment,
                mock.sentinel.option_names,
            )

        options = call_get_endpoint_destination_options()

        mock_get_provider.assert_called_once_with(
            mock.sentinel.platform_name,
            constants.PROVIDER_TYPE_DESTINATION_ENDPOINT_OPTIONS,
            None,
            raise_if_not_found=False,
        )
        mock_get_secret.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.connection_info
        )
        mock_get_provider.return_value\
            .get_target_environment_options.assert_called_once_with(
                mock.sentinel.context,
                mock_get_secret.return_value,
                env=mock.sentinel.environment,
                option_names=mock.sentinel.option_names,
            )
        mock_validate.assert_called_once_with(
            mock_get_provider.return_value
            .get_target_environment_options.return_value,
            schemas.CORIOLIS_DESTINATION_ENVIRONMENT_OPTIONS_SCHEMA,
        )

        self.assertEqual(
            options,
            mock_get_provider.return_value
            .get_target_environment_options.return_value,
        )

        # if the provider is not found, raise InvalidInput
        mock_get_provider.return_value = None
        self.assertRaises(
            exception.InvalidInput, call_get_endpoint_destination_options
        )

    @mock.patch.object(schemas, "validate_value")
    @mock.patch.object(utils, "get_secret_connection_info")
    @mock.patch.object(providers_factory, "get_provider")
    def test_get_endpoint_source_minion_pool_options(
        self, mock_get_provider, mock_get_secret, mock_validate
    ):
        def call_get_endpoint_source_minion_pool_options():
            return self.server.get_endpoint_source_minion_pool_options(
                mock.sentinel.context,
                mock.sentinel.platform_name,
                mock.sentinel.connection_info,
                mock.sentinel.environment,
                mock.sentinel.option_names,
            )

        options = call_get_endpoint_source_minion_pool_options()

        mock_get_provider.assert_called_once_with(
            mock.sentinel.platform_name,
            constants.PROVIDER_TYPE_SOURCE_MINION_POOL,
            None,
            raise_if_not_found=False,
        )
        mock_get_secret.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.connection_info
        )
        mock_get_provider.return_value\
            .get_minion_pool_options.assert_called_once_with(
                mock.sentinel.context,
                mock_get_secret.return_value,
                env=mock.sentinel.environment,
                option_names=mock.sentinel.option_names,
            )
        mock_validate.assert_called_once_with(
            mock_get_provider.return_value
            .get_minion_pool_options.return_value,
            schemas.CORIOLIS_DESTINATION_ENVIRONMENT_OPTIONS_SCHEMA,
        )

        self.assertEqual(
            options,
            mock_get_provider.return_value
            .get_minion_pool_options.return_value,
        )

        # if the provider is not found, raise InvalidInput
        mock_get_provider.return_value = None
        self.assertRaises(
            exception.InvalidInput,
            call_get_endpoint_source_minion_pool_options,
        )

    @mock.patch.object(schemas, "validate_value")
    @mock.patch.object(utils, "get_secret_connection_info")
    @mock.patch.object(providers_factory, "get_provider")
    def test_get_endpoint_destination_minion_pool_options(
        self, mock_get_provider, mock_get_secret, mock_validate
    ):
        def call_get_endpoint_destination_minion_pool_options():
            return self.server.get_endpoint_destination_minion_pool_options(
                mock.sentinel.context,
                mock.sentinel.platform_name,
                mock.sentinel.connection_info,
                mock.sentinel.environment,
                mock.sentinel.option_names,
            )

        options = call_get_endpoint_destination_minion_pool_options()

        mock_get_provider.assert_called_once_with(
            mock.sentinel.platform_name,
            constants.PROVIDER_TYPE_DESTINATION_MINION_POOL,
            None,
            raise_if_not_found=False,
        )
        mock_get_secret.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.connection_info
        )
        mock_get_provider.return_value\
            .get_minion_pool_options.assert_called_once_with(
                mock.sentinel.context,
                mock_get_secret.return_value,
                env=mock.sentinel.environment,
                option_names=mock.sentinel.option_names,
            )
        mock_validate.assert_called_once_with(
            mock_get_provider.return_value
            .get_minion_pool_options.return_value,
            schemas.CORIOLIS_DESTINATION_ENVIRONMENT_OPTIONS_SCHEMA,
        )

        self.assertEqual(
            options,
            mock_get_provider.return_value
            .get_minion_pool_options.return_value,
        )

        # if the provider is not found, raise InvalidInput
        mock_get_provider.return_value = None
        self.assertRaises(
            exception.InvalidInput,
            call_get_endpoint_destination_minion_pool_options,
        )

    @mock.patch.object(schemas, "validate_value")
    @mock.patch.object(utils, "get_secret_connection_info")
    @mock.patch.object(providers_factory, "get_provider")
    def test_get_endpoint_source_options(
        self, mock_get_provider, mock_get_secret, mock_validate
    ):
        def call_get_endpoint_source_options():
            return self.server.get_endpoint_source_options(
                mock.sentinel.context,
                mock.sentinel.platform_name,
                mock.sentinel.connection_info,
                mock.sentinel.environment,
                mock.sentinel.option_names,
            )

        options = call_get_endpoint_source_options()

        mock_get_provider.assert_called_once_with(
            mock.sentinel.platform_name,
            constants.PROVIDER_TYPE_SOURCE_ENDPOINT_OPTIONS,
            None,
            raise_if_not_found=False,
        )
        mock_get_secret.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.connection_info
        )
        mock_get_provider.return_value\
            .get_source_environment_options.assert_called_once_with(
                mock.sentinel.context,
                mock_get_secret.return_value,
                env=mock.sentinel.environment,
                option_names=mock.sentinel.option_names,
            )
        mock_validate.assert_called_once_with(
            mock_get_provider.return_value
            .get_source_environment_options.return_value,
            schemas.CORIOLIS_SOURCE_ENVIRONMENT_OPTIONS_SCHEMA,
        )

        self.assertEqual(
            options,
            mock_get_provider.return_value
            .get_source_environment_options.return_value,
        )

        # if the provider is not found, raise InvalidInput
        mock_get_provider.return_value = None
        self.assertRaises(
            exception.InvalidInput, call_get_endpoint_source_options
        )

    @mock.patch.object(schemas, "validate_value")
    @mock.patch.object(utils, "get_secret_connection_info")
    @mock.patch.object(providers_factory, "get_provider")
    def test_get_endpoint_networks(
        self, mock_get_provider, mock_get_secret, mock_validate
    ):

        mock_get_provider.return_value.get_networks.return_value = [
            mock.sentinel.networkInfo1,
            mock.sentinel.networkInfo2,
        ]
        networks_info = self.server.get_endpoint_networks(
            mock.sentinel.context,
            mock.sentinel.platform_name,
            mock.sentinel.connection_info,
            mock.sentinel.environment,
        )

        mock_get_provider.assert_called_once_with(
            mock.sentinel.platform_name,
            constants.PROVIDER_TYPE_ENDPOINT_NETWORKS,
            None,
        )
        mock_get_secret.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.connection_info
        )
        mock_get_provider.return_value.get_networks.assert_called_once_with(
            mock.sentinel.context,
            mock_get_secret.return_value,
            mock.sentinel.environment,
        )
        mock_validate.assert_has_calls(
            [
                mock.call(
                    mock.sentinel.networkInfo1,
                    schemas.CORIOLIS_VM_NETWORK_SCHEMA,
                ),
                mock.call(
                    mock.sentinel.networkInfo2,
                    schemas.CORIOLIS_VM_NETWORK_SCHEMA,
                ),
            ]
        )

        self.assertEqual(
            networks_info,
            mock_get_provider.return_value.get_networks.return_value,
        )

        # no networks returned
        mock_validate.reset_mock()
        mock_get_provider.return_value.get_networks.return_value = []
        networks_info = self.server.get_endpoint_networks(
            mock.sentinel.context,
            mock.sentinel.platform_name,
            mock.sentinel.connection_info,
            mock.sentinel.environment,
        )
        mock_validate.assert_not_called()
        self.assertEqual(networks_info, [])

    @mock.patch.object(schemas, "validate_value")
    @mock.patch.object(utils, "get_secret_connection_info")
    @mock.patch.object(providers_factory, "get_provider")
    def test_get_endpoint_storage(
        self, mock_get_provider, mock_get_secret, mock_validate
    ):

        storage = self.server.get_endpoint_storage(
            mock.sentinel.context,
            mock.sentinel.platform_name,
            mock.sentinel.connection_info,
            mock.sentinel.environment,
        )

        mock_get_provider.assert_called_once_with(
            mock.sentinel.platform_name,
            constants.PROVIDER_TYPE_ENDPOINT_STORAGE,
            None,
        )
        mock_get_secret.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.connection_info
        )
        mock_get_provider.return_value.get_storage.assert_called_once_with(
            mock.sentinel.context,
            mock_get_secret.return_value,
            mock.sentinel.environment,
        )
        mock_validate.assert_called_once_with(
            mock_get_provider.return_value.get_storage.return_value,
            schemas.CORIOLIS_VM_STORAGE_SCHEMA,
        )

        self.assertEqual(
            storage, mock_get_provider.return_value.get_storage.return_value
        )

    @mock.patch.object(providers_factory, "get_available_providers")
    def test_get_available_providers(self, mock_get_available_providers):
        result = self.server.get_available_providers(mock.sentinel.context)
        mock_get_available_providers.assert_called_once()
        self.assertEqual(result, mock_get_available_providers.return_value)

    @mock.patch.object(schemas, "validate_value")
    @mock.patch.object(providers_factory, "get_provider")
    def test_validate_endpoint_source_environment(
        self, mock_get_provider, mock_validate
    ):
        result = self.server.validate_endpoint_source_environment(
            mock.sentinel.context,
            mock.sentinel.source_platform_name,
            mock.sentinel.source_environment,
        )

        mock_get_provider.assert_called_once_with(
            mock.sentinel.source_platform_name,
            constants.PROVIDER_TYPE_REPLICA_EXPORT,
            None,
        )
        mock_validate.assert_called_once_with(
            mock.sentinel.source_environment,
            mock_get_provider.return_value
            .get_source_environment_schema.return_value,
        )

        self.assertEqual(result, (True, None))

        # handle SchemaValidationException
        mock_validate.side_effect = exception.SchemaValidationException(
            "test")
        result = self.server.validate_endpoint_source_environment(
            mock.sentinel.context,
            mock.sentinel.source_platform_name,
            mock.sentinel.source_environment,
        )

        self.assertEqual(result, (False, "test"))

    @mock.patch.object(schemas, "validate_value")
    @mock.patch.object(providers_factory, "get_provider")
    def test_validate_endpoint_target_environment(
        self, mock_get_provider, mock_validate
    ):
        result = self.server.validate_endpoint_target_environment(
            mock.sentinel.context,
            mock.sentinel.target_platform_name,
            mock.sentinel.target_environment,
        )

        mock_get_provider.assert_called_once_with(
            mock.sentinel.target_platform_name,
            constants.PROVIDER_TYPE_OS_MORPHING,
            None,
        )
        mock_validate.assert_called_once_with(
            mock.sentinel.target_environment,
            mock_get_provider.return_value
            .get_target_environment_schema.return_value,
        )

        self.assertEqual(result, (True, None))

        # handle SchemaValidationException
        mock_validate.side_effect = exception.SchemaValidationException(
            "test")
        result = self.server.validate_endpoint_target_environment(
            mock.sentinel.context,
            mock.sentinel.target_platform_name,
            mock.sentinel.target_environment,
        )

        self.assertEqual(result, (False, "test"))

    @mock.patch.object(schemas, "validate_value")
    @mock.patch.object(providers_factory, "get_provider")
    def test_validate_endpoint_source_minion_pool_options(
        self, mock_get_provider, mock_validate
    ):
        result = self.server.validate_endpoint_source_minion_pool_options(
            mock.sentinel.context,
            mock.sentinel.source_platform_name,
            mock.sentinel.pool_environment,
        )

        mock_get_provider.assert_called_once_with(
            mock.sentinel.source_platform_name,
            constants.PROVIDER_TYPE_SOURCE_MINION_POOL,
            None,
        )
        mock_validate.assert_called_once_with(
            mock.sentinel.pool_environment,
            mock_get_provider.return_value
            .get_minion_pool_environment_schema.return_value,
        )

        self.assertEqual(result, (True, None))

        # handle SchemaValidationException
        mock_validate.side_effect = exception.SchemaValidationException(
            "test")
        result = self.server.validate_endpoint_source_minion_pool_options(
            mock.sentinel.context,
            mock.sentinel.source_platform_name,
            mock.sentinel.pool_environment,
        )

        self.assertEqual(result, (False, "test"))

    @mock.patch.object(schemas, "validate_value")
    @mock.patch.object(providers_factory, "get_provider")
    def test_validate_endpoint_destination_minion_pool_options(
        self, mock_get_provider, mock_validate
    ):
        result = self.server.validate_endpoint_destination_minion_pool_options(
            mock.sentinel.context,
            mock.sentinel.destination_platform_name,
            mock.sentinel.pool_environment,
        )

        mock_get_provider.assert_called_once_with(
            mock.sentinel.destination_platform_name,
            constants.PROVIDER_TYPE_DESTINATION_MINION_POOL,
            None,
        )
        mock_validate.assert_called_once_with(
            mock.sentinel.pool_environment,
            mock_get_provider.return_value
            .get_minion_pool_environment_schema.return_value,
        )

        self.assertEqual(result, (True, None))

        # handle SchemaValidationException
        mock_validate.side_effect = exception.SchemaValidationException(
            "test")
        result = self.server.validate_endpoint_destination_minion_pool_options(
            mock.sentinel.context,
            mock.sentinel.destination_platform_name,
            mock.sentinel.pool_environment,
        )

        self.assertEqual(result, (False, "test"))

    @mock.patch.object(schemas, "validate_value")
    @mock.patch.object(providers_factory, "get_provider")
    @mock.patch.object(utils, "get_secret_connection_info")
    def test_validate_endpoint_connection(
        self, mock_get_secret, mock_get_provider, mock_validate
    ):
        def call_validate_endpoint_connection():
            return self.server.validate_endpoint_connection(
                mock.sentinel.context,
                mock.sentinel.platform_name,
                mock.sentinel.connection_info,
            )

        result = call_validate_endpoint_connection()

        mock_get_provider.assert_called_once_with(
            mock.sentinel.platform_name, constants.PROVIDER_TYPE_ENDPOINT, None
        )
        mock_get_secret.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.connection_info
        )
        mock_validate.assert_called_once_with(
            mock_get_secret.return_value,
            mock_get_provider.return_value
            .get_connection_info_schema.return_value,
        )
        mock_get_provider.return_value\
            .validate_connection.assert_called_once_with(
                mock.sentinel.context, mock_get_secret.return_value
            )

        self.assertEqual(result, (True, None))

        # handle SchemaValidationException
        mock_validate.side_effect = exception.SchemaValidationException()
        result = call_validate_endpoint_connection()

        self.assertEqual(result[0], False)

        # handle ConnectionValidationException
        mock_validate.side_effect = exception.ConnectionValidationException()
        result = call_validate_endpoint_connection()
        self.assertEqual(result[0], False)

        # handle BaseException
        mock_validate.side_effect = BaseException("test")
        result = call_validate_endpoint_connection()
        self.assertEqual(result[0], False)

    @mock.patch.object(providers_factory, "get_provider")
    @ddt.data(
        (
            constants.PROVIDER_TYPE_ENDPOINT,
            "connection_info_schema"
        ),
        (
            constants.PROVIDER_TYPE_REPLICA_IMPORT,
            "destination_environment_schema",
        ),
        (
            constants.PROVIDER_TYPE_REPLICA_EXPORT,
            "source_environment_schema"
        ),
        (
            constants.PROVIDER_TYPE_SOURCE_MINION_POOL,
            "source_minion_pool_environment_schema",
        ),
        (
            constants.PROVIDER_TYPE_DESTINATION_MINION_POOL,
            "destination_minion_pool_environment_schema",
        ),
    )
    def test_get_provider_schemas(self, schema_type, mock_get_provider):
        provider_schemas = self.server.get_provider_schemas(
            mock.sentinel.context, mock.sentinel.platform_name, schema_type[0]
        )

        mock_get_provider.assert_called_once_with(
            mock.sentinel.platform_name, schema_type[0], None
        )

        assert schema_type[1] in provider_schemas

    @mock.patch.object(utils, "get_diagnostics_info")
    def test_get_diagnostics(self, mock_get_diagnostics_info):
        result = self.server.get_diagnostics(mock.sentinel.context)
        self.assertEqual(result, mock_get_diagnostics_info.return_value)
