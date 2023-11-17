# Copyright 2022 Cloudbase Solutions Srl
# All Rights Reserved.
import multiprocessing
import os
import shutil
import signal
import tempfile
from unittest import mock

import ddt
import eventlet
from oslo_log import log as logging
import psutil
from six.moves import queue

from coriolis.conductor.rpc import client as conductor_client
from coriolis.conductor.rpc import utils as cond_rpc_utils
from coriolis import constants
from coriolis import context
from coriolis import exception
from coriolis.minion_manager.rpc import client as minion_client
from coriolis.providers import factory as providers_factory
from coriolis import schemas
from coriolis.tasks import factory as task_runners_factory
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

    @mock.patch.object(minion_client, 'MinionManagerPoolRpcEventHandler')
    def test__get_event_handler_for_task_type_minion(
            self, mock_minion_event_handler):
        result = server._get_event_handler_for_task_type(
            constants.TASK_TYPE_VALIDATE_SOURCE_MINION_POOL_OPTIONS,
            mock.sentinel.ctxt,
            mock.sentinel.task_object_id)
        mock_minion_event_handler.assert_called_once_with(
            mock.sentinel.ctxt, mock.sentinel.task_object_id)
        self.assertEqual(result, mock_minion_event_handler.return_value)

    @mock.patch.object(conductor_client, 'ConductorTaskRpcEventHandler')
    def test__get_event_handler_for_task_type(
            self, mock_conductor_event_handler):
        result = server._get_event_handler_for_task_type(
            constants.TASK_TYPE_REPLICATE_DISKS,
            mock.sentinel.ctxt,
            mock.sentinel.task_object_id)
        mock_conductor_event_handler.assert_called_once_with(
            mock.sentinel.ctxt, mock.sentinel.task_object_id)
        self.assertEqual(result, mock_conductor_event_handler.return_value)

    @mock.patch.object(conductor_client, 'ConductorClient')
    def test__rpc_conductor_client(self, mock_cond_client):
        result = self.server._rpc_conductor_client
        mock_cond_client.assert_called_once()
        self.assertEqual(result, mock_cond_client.return_value)

    @mock.patch.object(conductor_client, 'ConductorClient')
    def test__rpc_conductor_client_instantiated(self, mock_cond_client):
        self.server._rpc_conductor_client_instance = mock.sentinel.cond_client
        mock_cond_client.assert_not_called()

    @mock.patch.object(conductor_client, 'ConductorClient')
    @mock.patch.object(cond_rpc_utils, 'check_create_registration_for_service')
    @mock.patch.object(server.WorkerServerEndpoint, 'get_service_status')
    @mock.patch.object(context, 'RequestContext')
    @mock.patch.object(utils, 'get_binary_name')
    @mock.patch.object(utils, 'get_hostname')
    def test__register_worker_service(
            self, mock_hostname, mock_binary, mock_context,
            mock_get_service_status, mock_check_create_service,
            mock_cond_client):
        result = self.server._register_worker_service()

        mock_hostname.assert_called_once()
        mock_binary.assert_called_once()
        mock_context.assert_called_once_with('coriolis', 'admin')
        mock_get_service_status.assert_called_once_with(
            mock_context.return_value)
        mock_check_create_service.assert_called_once_with(
            mock_cond_client.return_value, mock_context.return_value,
            mock_hostname.return_value, mock_binary.return_value,
            constants.WORKER_MAIN_MESSAGING_TOPIC, enabled=True,
            providers=mock_get_service_status.return_value['providers'],
            specs=mock_get_service_status.return_value['specs'])

        self.assertEqual(result, mock_check_create_service.return_value)
        self.assertEqual(result, self.server._service_registration)

    def test__check_remove_dir(self):
        tmp = tempfile.mkdtemp()
        self.server._check_remove_dir(tmp)
        self.assertFalse(os.path.exists(tmp))

    @mock.patch.object(shutil, 'rmtree')
    def test__check_remove_dir_fails(self, mock_rmtree):
        tmp = tempfile.mkdtemp()
        mock_rmtree.side_effect = Exception('YOLO')
        self.server._check_remove_dir(tmp)
        self.assertLogs(server.LOG, level=logging.ERROR)
        os.rmdir(tmp)

    @mock.patch.object(server.WorkerServerEndpoint, 'get_available_providers')
    @mock.patch.object(server.WorkerServerEndpoint, 'get_diagnostics')
    def test_get_service_status(self, mock_get_diagnostics,
                                mock_get_available_providers):
        expected_result = {
            "host": mock_get_diagnostics.return_value['hostname'],
            "binary": mock_get_diagnostics.return_value['application'],
            "topic": constants.WORKER_MAIN_MESSAGING_TOPIC,
            "providers": mock_get_available_providers.return_value,
            "specs": mock_get_diagnostics.return_value,
        }
        result = self.server.get_service_status(mock.sentinel.ctxt)
        mock_get_available_providers.assert_called_once_with(
            mock.sentinel.ctxt)
        mock_get_diagnostics.assert_called_once()
        self.assertEqual(result, expected_result)

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

    @mock.patch.object(logging, 'getLogger')
    def test__handle_mp_log_events(self, mock_get_logger):
        mock_mp_log_q = mock.MagicMock()
        mock_p = mock.MagicMock()
        mock_mp_log_q.get.side_effect = [
            mock.sentinel.record, queue.Empty, None]
        mock_p.is_alive.return_value = True

        result = self.server._handle_mp_log_events(mock_p, mock_mp_log_q)
        mock_get_logger.assert_called_once_with(mock.sentinel.record.name)
        mock_get_logger.return_value.logger.handle.assert_called_once_with(
            mock.sentinel.record)
        self.assertIsNone(result)

    def test__handle_mp_log_events_dead_process(self):
        mock_mp_log_q = mock.MagicMock()
        mock_p = mock.MagicMock()
        mock_mp_log_q.get.side_effect = queue.Empty
        mock_p.is_alive.return_value = False

        result = self.server._handle_mp_log_events(mock_p, mock_mp_log_q)
        self.assertIsNone(result)

    @ddt.file_data('data/get_custom_ld_path.yml')
    @ddt.unpack
    def test__get_custom_ld_path(self, config):
        original_ld_path = config.get("original_ld_path", "")
        extra_library_paths = config.get("extra_library_paths", [])
        exception_expected = config.get("exception_expected", False)
        expected_result = config.get("expected_result")

        if exception_expected:
            self.assertRaises(
                TypeError, self.server._get_custom_ld_path,
                original_ld_path, extra_library_paths)
            return

        result = self.server._get_custom_ld_path(
            original_ld_path, extra_library_paths)
        self.assertEqual(result, expected_result)

    @mock.patch.object(server.WorkerServerEndpoint, '_get_custom_ld_path')
    def test__start_process_with_custom_library_paths(
            self, mock_get_custom_ld_path):
        original_ld_path = os.environ.get('LD_LIBRARY_PATH', '')
        # NOTE(dvincze): Return value is required to be string here, as this
        # value will be assigned to environment variable LD_LIBRARY_PATH
        mock_get_custom_ld_path.return_value = "custom_ld_path"
        process = mock.MagicMock()

        self.server._start_process_with_custom_library_paths(
            process, mock.sentinel.extra_library_paths)
        process.start.assert_called_once()
        mock_get_custom_ld_path.assert_called_once_with(
            original_ld_path, mock.sentinel.extra_library_paths)
        self.assertEqual(original_ld_path, os.environ['LD_LIBRARY_PATH'])

    @mock.patch.object(server.WorkerServerEndpoint, '_get_custom_ld_path')
    def test__start_process_with_custom_library_paths_raises(
            self, mock_custom_ld_path):
        original_ld_path = os.environ.get('LD_LIBRARY_PATH', '')
        mock_custom_ld_path.side_effect = TypeError()
        process = mock.MagicMock()

        self.server._start_process_with_custom_library_paths(
            process, mock.sentinel.extra_library_paths)
        process.assert_not_called()
        self.assertLogs(server.LOG, logging.WARNING)
        self.assertEqual(original_ld_path, os.environ['LD_LIBRARY_PATH'])

    @mock.patch.object(task_runners_factory, 'get_task_runner_class')
    @mock.patch.object(server, '_get_event_handler_for_task_type')
    def test__get_extra_library_paths_for_providers(
            self, mock_get_event_handler, mock_get_task_runner):
        result = self.server._get_extra_library_paths_for_providers(
            mock.sentinel.ctxt, mock.sentinel.task_id, mock.sentinel.task_type,
            mock.sentinel.origin, mock.sentinel.destination)
        mock_get_event_handler.assert_called_once_with(mock.sentinel.task_type,
                                                       mock.sentinel.ctxt,
                                                       mock.sentinel.task_id)
        mock_get_task_runner.assert_called_once_with(mock.sentinel.task_type)
        mock_task_runner = (
            mock_get_task_runner.return_value.return_value)
        mock_task_runner.get_shared_libs_for_providers.assert_called_once_with(
            mock.sentinel.ctxt, mock.sentinel.origin,
            mock.sentinel.destination, mock_get_event_handler.return_value)
        self.assertEqual(
            result,
            mock_task_runner.get_shared_libs_for_providers.return_value)

    def test__wait_for_process(self):
        p = mock.MagicMock()
        mp_q = mock.MagicMock()

        mp_q.get.side_effect = ["result", "result"]
        p.is_alive.side_effect = [True, False]

        result = self.server._wait_for_process(p, mp_q)

        self.assertEqual(result, "result")

    def test__wait_for_process_empty_queue_dead_process(self):
        p = mock.MagicMock()
        mp_q = mock.MagicMock()

        mp_q.get.side_effect = [queue.Empty]
        p.is_alive.side_effect = [False]

        result = self.server._wait_for_process(p, mp_q)
        self.assertEqual(result, None)

    def test__wait_for_process_dead_process(self):
        p = mock.MagicMock()
        mp_q = mock.MagicMock()

        mp_q.get.side_effect = [None, "result"]
        p.is_alive.side_effect = [False]

        result = self.server._wait_for_process(p, mp_q)
        self.assertEqual(result, "result")

    def test__wait_for_process_dead_process_quere_raise(self):
        p = mock.MagicMock()
        mp_q = mock.MagicMock()

        mp_q.get.side_effect = [None, Exception]
        p.is_alive.side_effect = [False]

        result = self.server._wait_for_process(p, mp_q)
        self.assertEqual(result, None)

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

    @mock.patch('coriolis.service.get_worker_count_from_args')
    @mock.patch('sys.argv')
    @mock.patch('oslo_config.cfg.CONF')
    @mock.patch('coriolis.utils.setup_logging')
    @mock.patch('oslo_log.log.getLogger')
    @mock.patch('logging.handlers.QueueHandler')
    def test__setup_task_process(self, mock_queue_handler, mock_get_logger,
                                 mock_setup_logging, mock_conf, mock_argv,
                                 mock_get_worker_count_from_args):
        mock_get_worker_count_from_args.return_value = (None, "args")
        mock_logger = mock_get_logger.return_value.logger
        mock_logger.handlers = [mock.sentinel.handler]
        server._setup_task_process(mock.sentinel.mp_log_q)

        mock_get_worker_count_from_args.assert_called_once_with(mock_argv)
        mock_conf.assert_called_once_with(
            mock_get_worker_count_from_args.return_value[1][1:],
            project='coriolis', version='1.0.0')
        mock_setup_logging.assert_called_once_with()
        mock_get_logger.assert_called_once_with(None)
        mock_logger.removeHandler.assert_called_once_with(
            mock.sentinel.handler)
        mock_queue_handler.assert_called_once_with(
            mock.sentinel.mp_log_q)
        mock_logger.addHandler.assert_called_once_with(
            mock_queue_handler.return_value)

    @mock.patch.object(server, '_setup_task_process')
    @mock.patch.object(task_runners_factory, 'get_task_runner_class')
    @mock.patch.object(server, '_get_event_handler_for_task_type')
    @mock.patch('coriolis.utils.is_serializable')
    def test__task_process(self, mock_is_serializable,
                           mock_get_event_handler, mock_get_task_runner_class,
                           mock_setup_task_process):
        mp_q = mock.MagicMock()
        mp_log_q = mock.MagicMock()
        task_info = {}
        mock_task_runner = mock_get_task_runner_class.return_value.return_value
        mock_task_result = mock_task_runner.run.return_value

        server._task_process(mock.sentinel.ctxt, mock.sentinel.task_id,
                             mock.sentinel.task_type, mock.sentinel.origin,
                             mock.sentinel.destination, mock.sentinel.instance,
                             task_info, mp_q, mp_log_q)
        mock_setup_task_process.assert_called_once_with(mp_log_q)
        mock_get_task_runner_class.assert_called_once_with(
            mock.sentinel.task_type)
        mock_get_event_handler.assert_called_once_with(mock.sentinel.task_type,
                                                       mock.sentinel.ctxt,
                                                       mock.sentinel.task_id)
        mock_task_runner.run.assert_called_once_with(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            mock.sentinel.destination, task_info,
            mock_get_event_handler.return_value)
        mock_is_serializable.assert_called_once_with(mock_task_result)
        mp_q.put.assert_called_once_with(mock_task_result)
        mp_log_q.put.assert_called_once_with(None)

    @mock.patch.object(server, '_setup_task_process')
    def test__task_process_raise(self, mock_setup_task_process):
        mock_setup_task_process.side_effect = Exception('YOLO')
        mp_q = mock.MagicMock()
        mp_log_q = mock.MagicMock()

        server._task_process(mock.sentinel.ctxt, mock.sentinel.task_id,
                             mock.sentinel.task_type, mock.sentinel.origin,
                             mock.sentinel.destination, mock.sentinel.instance,
                             mock.sentinel.task_info, mp_q, mp_log_q)
        mp_q.put.assert_called_once_with("YOLO")
        mp_log_q.put.assert_called_once_with(None)
