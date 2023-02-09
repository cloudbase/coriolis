# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import copy
import ddt
import uuid
from unittest import mock

from coriolis import constants, exception
from coriolis.conductor.rpc import server
from coriolis.db import api as db_api
from coriolis.db.sqlalchemy import models
from coriolis.licensing import client as licensing_client
from coriolis.tests import test_base, testutils
from coriolis.worker.rpc import client as rpc_worker_client
from oslo_concurrency import lockutils


@ddt.ddt
class ConductorServerEndpointTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Conductor RPC server."""

    def setUp(self):
        super(ConductorServerEndpointTestCase, self).setUp()
        self.server = server.ConductorServerEndpoint()

    @mock.patch.object(
        rpc_worker_client.WorkerClient, "from_service_definition"
    )
    @mock.patch.object(server.ConductorServerEndpoint, "_scheduler_client")
    def test_get_all_diagnostics(self, mock_scheduler_client, _):
        mock_scheduler_client.get_workers_for_specs.side_effect = Exception()
        self.assertRaises(
            Exception,
            lambda: self.server.get_all_diagnostics(mock.sentinel.context),
        )
        mock_scheduler_client.get_workers_for_specs.side_effect = None

        diagnostics = self.server.get_all_diagnostics(mock.sentinel.context)
        assert (
            mock_scheduler_client.get_diagnostics.return_value in diagnostics
        )

    @mock.patch.object(
        rpc_worker_client.WorkerClient, "from_service_definition"
    )
    @mock.patch.object(db_api, "get_service")
    @mock.patch.object(server.ConductorServerEndpoint, "_scheduler_client")
    def test_get_worker_service_rpc_for_specs(
            self,
            mock_scheduler_client,
            mock_get_service,
            mock_from_service_definition,
    ):
        # returns dictionary with id and rpc
        mock_scheduler_client.get_worker_service_for_specs.return_value = {
            "id": mock.sentinel.worker_id
        }
        result = self.server._get_worker_service_rpc_for_specs(
            mock.sentinel.context
        )

        worker_service = mock_scheduler_client.get_worker_service_for_specs
        worker_service.assert_called_once_with(
            mock.sentinel.context,
            provider_requirements=None,
            region_sets=None,
            enabled=True,
            random_choice=False,
            raise_on_no_matches=True,
        )
        mock_get_service.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.worker_id
        )

        self.assertEqual(result, mock_from_service_definition.return_value)

    @mock.patch.object(server.ConductorServerEndpoint, "get_endpoint")
    @mock.patch.object(db_api, "delete_endpoint")
    @mock.patch.object(db_api, "update_endpoint")
    @mock.patch.object(db_api, "add_endpoint")
    @mock.patch.object(models, "Endpoint")
    @mock.patch.object(uuid, "uuid4")
    def test_create_endpoint(
            self,
            mock_uuid4,
            mock_endpoint_model,
            mock_add_endpoint,
            mock_update_endpoint,
            mock_delete_endpoint,
            mock_get_endpoint,
    ):
        endpoint = self.server.create_endpoint(
            mock.sentinel.context,
            mock.sentinel.name,
            mock.sentinel.endpoint_type,
            mock.sentinel.description,
            mock.sentinel.connection_info,
        )
        self.assertEqual(
            mock_endpoint_model.return_value.name, mock.sentinel.name
        )
        self.assertEqual(
            mock_endpoint_model.return_value.type, mock.sentinel.endpoint_type
        )
        self.assertEqual(
            mock_endpoint_model.return_value.description,
            mock.sentinel.description,
        )
        self.assertEqual(
            mock_endpoint_model.return_value.connection_info,
            mock.sentinel.connection_info,
        )

        mock_add_endpoint.assert_called_once_with(
            mock.sentinel.context, mock_endpoint_model.return_value
        )
        mock_update_endpoint.assert_not_called()
        mock_delete_endpoint.assert_not_called()
        self.assertEqual(endpoint, mock_get_endpoint.return_value)

        # mapped_regions exist
        self.server.create_endpoint(
            mock.sentinel.context,
            mock.sentinel.name,
            mock.sentinel.endpoint_type,
            mock.sentinel.description,
            mock.sentinel.connection_info,
            mock.sentinel.mapped_regions,
        )

        mock_update_endpoint.assert_called_once_with(
            mock.sentinel.context,
            str(mock_uuid4.return_value),
            {"mapped_regions": mock.sentinel.mapped_regions},
        )
        mock_delete_endpoint.assert_not_called()

        # mapped_regions exist and there's an error updating the endpoint
        mock_update_endpoint.side_effect = Exception()
        self.assertRaises(
            Exception,
            lambda: self.server.create_endpoint(
                mock.sentinel.context,
                mock.sentinel.name,
                mock.sentinel.endpoint_type,
                mock.sentinel.description,
                mock.sentinel.connection_info,
                mock.sentinel.mapped_regions,
            ),
        )
        mock_delete_endpoint.assert_called_once_with(
            mock.sentinel.context, str(mock_uuid4.return_value)
        )

    @mock.patch.object(db_api, "get_endpoint")
    @mock.patch.object(db_api, "update_endpoint")
    def test_update_endpoint(self, mock_update_endpoint, mock_get_endpoint):
        endpoint = testutils.get_wrapped_function(self.server.update_endpoint)(
            self,
            mock.sentinel.context,
            mock.sentinel.endpoint_id,
            mock.sentinel.updated_values,  # type: ignore
        )

        mock_update_endpoint.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.endpoint_id,
            mock.sentinel.updated_values,
        )
        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id
        )
        self.assertEqual(endpoint, mock_get_endpoint.return_value)

    @mock.patch.object(db_api, "get_endpoints")
    def test_get_endpoints(self, mock_get_endpoints):
        endpoints = self.server.get_endpoints(mock.sentinel.context)
        mock_get_endpoints.assert_called_once_with(mock.sentinel.context)
        self.assertEqual(endpoints, mock_get_endpoints.return_value)

    @mock.patch.object(db_api, "get_endpoint")
    def test_get_endpoint(self, mock_get_endpoint):
        def call_get_endpoint():
            return testutils.get_wrapped_function(self.server.get_endpoint)(
                self, mock.sentinel.context,
                mock.sentinel.endpoint_id  # type: ignore
            )

        endpoint = call_get_endpoint()
        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id
        )
        self.assertEqual(endpoint, mock_get_endpoint.return_value)

        # endpoint not found
        mock_get_endpoint.side_effect = exception.NotFound()
        self.assertRaises(exception.NotFound, call_get_endpoint)

    @mock.patch.object(db_api, "delete_endpoint")
    @mock.patch.object(db_api, "get_endpoint_replicas_count")
    def test_delete_endpoint(
            self, mock_get_endpoint_replicas_count, mock_delete_endpoint
    ):
        def call_delete_endpoint():
            return testutils.get_wrapped_function(self.server.delete_endpoint)(
                self, mock.sentinel.context,
                mock.sentinel.endpoint_id  # type: ignore
            )

        mock_get_endpoint_replicas_count.return_value = 0
        call_delete_endpoint()
        mock_delete_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id
        )

        # endpoint has replicas
        mock_get_endpoint_replicas_count.return_value = 1
        self.assertRaises(exception.NotAuthorized, call_delete_endpoint)

    @mock.patch.object(
        server.ConductorServerEndpoint, "_get_worker_service_rpc_for_specs"
    )
    @mock.patch.object(server.ConductorServerEndpoint, "get_endpoint")
    def test_get_endpoint_instances(
            self, mock_get_endpoint, mock_get_worker_service_rpc_for_specs
    ):
        instances = self.server.get_endpoint_instances(
            mock.sentinel.context,
            mock.sentinel.endpoint_id,
            mock.sentinel.source_environment,
            mock.sentinel.marker,
            mock.sentinel.limit,
            mock.sentinel.instance_name_pattern,
        )

        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id
        )

        mock_get_worker_service_rpc_for_specs.assert_called_once_with(
            mock.sentinel.context,
            enabled=True,
            region_sets=[[]],
            provider_requirements={
                mock_get_endpoint.return_value.type: [
                    constants.PROVIDER_TYPE_ENDPOINT_INSTANCES
                ]
            },
        )
        rpc_return_value = mock_get_worker_service_rpc_for_specs.return_value
        self.assertEqual(
            instances, rpc_return_value.get_endpoint_instances.return_value
        )

    @mock.patch.object(
        server.ConductorServerEndpoint, "_get_worker_service_rpc_for_specs"
    )
    @mock.patch.object(server.ConductorServerEndpoint, "get_endpoint")
    def test_get_endpoint_instance(
            self, mock_get_endpoint, mock_get_worker_service_rpc_for_specs
    ):
        instance = self.server.get_endpoint_instance(
            mock.sentinel.context,
            mock.sentinel.endpoint_id,
            mock.sentinel.source_environment,
            mock.sentinel.instance_name,
        )

        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id
        )

        mock_get_worker_service_rpc_for_specs.assert_called_once_with(
            mock.sentinel.context,
            enabled=True,
            region_sets=[[]],
            provider_requirements={
                mock_get_endpoint.return_value.type: [
                    constants.PROVIDER_TYPE_ENDPOINT_INSTANCES
                ]
            },
        )

        rpc_return_value = mock_get_worker_service_rpc_for_specs.return_value
        self.assertEqual(
            instance, rpc_return_value.get_endpoint_instance.return_value
        )

    @mock.patch.object(
        server.ConductorServerEndpoint, "_get_worker_service_rpc_for_specs"
    )
    @mock.patch.object(server.ConductorServerEndpoint, "get_endpoint")
    def test_get_endpoint_source_options(
            self, mock_get_endpoint, mock_get_worker_service_rpc_for_specs
    ):
        options = self.server.get_endpoint_source_options(
            mock.sentinel.context,
            mock.sentinel.endpoint_id,
            mock.sentinel.environment,
            mock.sentinel.option_names,
        )

        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id
        )

        mock_get_worker_service_rpc_for_specs.assert_called_once_with(
            mock.sentinel.context,
            enabled=True,
            region_sets=[[]],
            provider_requirements={
                mock_get_endpoint.return_value.type: [
                    constants.PROVIDER_TYPE_SOURCE_ENDPOINT_OPTIONS
                ]
            },
        )

        rpc_return_value = mock_get_worker_service_rpc_for_specs.return_value
        rpc_return_value.get_endpoint_source_options.assert_called_once_with(
            mock.sentinel.context,
            mock_get_endpoint.return_value.type,
            mock_get_endpoint.return_value.connection_info,
            mock.sentinel.environment,
            mock.sentinel.option_names,
        )

        self.assertEqual(
            options, rpc_return_value.get_endpoint_source_options.return_value
        )

    @mock.patch.object(
        server.ConductorServerEndpoint, "_get_worker_service_rpc_for_specs"
    )
    @mock.patch.object(server.ConductorServerEndpoint, "get_endpoint")
    def test_get_endpoint_destination_options(
            self, mock_get_endpoint, mock_get_worker_service_rpc_for_specs
    ):
        options = self.server.get_endpoint_destination_options(
            mock.sentinel.context,
            mock.sentinel.endpoint_id,
            mock.sentinel.environment,
            mock.sentinel.option_names,
        )

        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id
        )

        mock_get_worker_service_rpc_for_specs.assert_called_once_with(
            mock.sentinel.context,
            enabled=True,
            region_sets=[[]],
            provider_requirements={
                mock_get_endpoint.return_value.type: [
                    constants.PROVIDER_TYPE_DESTINATION_ENDPOINT_OPTIONS
                ]
            },
        )

        rpc_return_value = mock_get_worker_service_rpc_for_specs.return_value
        rpc_return_value.get_endpoint_destination_options\
            .assert_called_once_with(
                mock.sentinel.context,
                mock_get_endpoint.return_value.type,
                mock_get_endpoint.return_value.connection_info,
                mock.sentinel.environment,
                mock.sentinel.option_names,
                )

        self.assertEqual(
            options,
            rpc_return_value.get_endpoint_destination_options.return_value,
        )

    @mock.patch.object(
        server.ConductorServerEndpoint, "_get_worker_service_rpc_for_specs"
    )
    @mock.patch.object(server.ConductorServerEndpoint, "get_endpoint")
    def test_get_endpoint_networks(
            self, mock_get_endpoint, mock_get_worker_service_rpc_for_specs
    ):
        networks = self.server.get_endpoint_networks(
            mock.sentinel.context,
            mock.sentinel.endpoint_id,
            mock.sentinel.environment,
        )

        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id
        )

        mock_get_worker_service_rpc_for_specs.assert_called_once_with(
            mock.sentinel.context,
            enabled=True,
            region_sets=[[]],
            provider_requirements={
                mock_get_endpoint.return_value.type: [
                    constants.PROVIDER_TYPE_ENDPOINT_NETWORKS
                ]
            },
        )

        rpc_return_value = mock_get_worker_service_rpc_for_specs.return_value
        rpc_return_value.get_endpoint_networks.assert_called_once_with(
            mock.sentinel.context,
            mock_get_endpoint.return_value.type,
            mock_get_endpoint.return_value.connection_info,
            mock.sentinel.environment,
        )

        self.assertEqual(
            networks, rpc_return_value.get_endpoint_networks.return_value
        )

    @mock.patch.object(
        server.ConductorServerEndpoint, "_get_worker_service_rpc_for_specs"
    )
    @mock.patch.object(server.ConductorServerEndpoint, "get_endpoint")
    def test_get_endpoint_storage_pools(
            self, mock_get_endpoint, mock_get_worker_service_rpc_for_specs
    ):
        storage_pools = self.server.get_endpoint_storage(
            mock.sentinel.context,
            mock.sentinel.endpoint_id,
            mock.sentinel.environment,
        )

        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id
        )

        mock_get_worker_service_rpc_for_specs.assert_called_once_with(
            mock.sentinel.context,
            enabled=True,
            region_sets=[[]],
            provider_requirements={
                mock_get_endpoint.return_value.type: [
                    constants.PROVIDER_TYPE_ENDPOINT_STORAGE
                ]
            },
        )

        rpc_return_value = mock_get_worker_service_rpc_for_specs.return_value
        rpc_return_value.get_endpoint_storage.assert_called_once_with(
            mock.sentinel.context,
            mock_get_endpoint.return_value.type,
            mock_get_endpoint.return_value.connection_info,
            mock.sentinel.environment,
        )

        self.assertEqual(
            storage_pools, rpc_return_value.get_endpoint_storage.return_value
        )

    @mock.patch.object(
        server.ConductorServerEndpoint, "_get_worker_service_rpc_for_specs"
    )
    @mock.patch.object(server.ConductorServerEndpoint, "get_endpoint")
    def test_validate_endpoint_connection(
            self, mock_get_endpoint, mock_get_worker_service_rpc_for_specs
    ):
        self.server.validate_endpoint_connection(
            mock.sentinel.context, mock.sentinel.endpoint_id
        )

        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id
        )

        mock_get_worker_service_rpc_for_specs.assert_called_once_with(
            mock.sentinel.context,
            enabled=True,
            region_sets=[[]],
            provider_requirements={
                mock_get_endpoint.return_value.type: [
                    constants.PROVIDER_TYPE_ENDPOINT
                ]
            },
        )

        rpc_return_value = mock_get_worker_service_rpc_for_specs.return_value
        rpc_return_value.validate_endpoint_connection.assert_called_once_with(
            mock.sentinel.context,
            mock_get_endpoint.return_value.type,
            mock_get_endpoint.return_value.connection_info,
        )

    @mock.patch.object(
        server.ConductorServerEndpoint, "_get_worker_service_rpc_for_specs"
    )
    @mock.patch.object(server.ConductorServerEndpoint, "get_endpoint")
    def test_validate_endpoint_target_environment(
            self, mock_get_endpoint, mock_get_worker_service_rpc_for_specs
    ):
        self.server.validate_endpoint_target_environment(
            mock.sentinel.context,
            mock.sentinel.endpoint_id,
            mock.sentinel.target_env,
        )

        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id
        )

        mock_get_worker_service_rpc_for_specs.assert_called_once_with(
            mock.sentinel.context,
            enabled=True,
            region_sets=[[]],
            provider_requirements={
                mock_get_endpoint.return_value.type: [
                    constants.PROVIDER_TYPE_ENDPOINT
                ]
            },
        )

        mock_get_worker_service_rpc_for_specs.return_value\
            .validate_endpoint_target_environment.assert_called_once_with(
                mock.sentinel.context,
                mock_get_endpoint.return_value.type,
                mock.sentinel.target_env,
                )

    @mock.patch.object(
        server.ConductorServerEndpoint, "_get_worker_service_rpc_for_specs"
    )
    @mock.patch.object(server.ConductorServerEndpoint, "get_endpoint")
    def test_validate_endpoint_source_environment(
            self, mock_get_endpoint, mock_get_worker_service_rpc_for_specs
    ):
        self.server.validate_endpoint_source_environment(
            mock.sentinel.context,
            mock.sentinel.endpoint_id,
            mock.sentinel.source_env,
        )

        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id
        )

        mock_get_worker_service_rpc_for_specs.assert_called_once_with(
            mock.sentinel.context,
            enabled=True,
            region_sets=[[]],
            provider_requirements={
                mock_get_endpoint.return_value.type: [
                    constants.PROVIDER_TYPE_ENDPOINT
                ]
            },
        )

        mock_get_worker_service_rpc_for_specs.return_value\
            .validate_endpoint_source_environment.assert_called_once_with(
                mock.sentinel.context,
                mock_get_endpoint.return_value.type,
                mock.sentinel.source_env,
                )

    @mock.patch.object(
        rpc_worker_client.WorkerClient, "from_service_definition"
    )
    @mock.patch.object(server.ConductorServerEndpoint, "_scheduler_client")
    def test_get_available_providers(
            self, mock_scheduler_client, mock_service_definition
    ):
        providers = self.server.get_available_providers(mock.sentinel.context)
        mock_service_definition.assert_called_once_with(
            mock_scheduler_client.get_any_worker_service(mock.sentinel.context)
        )
        mock_service_definition.return_value\
            .get_available_providers.assert_called_once_with(
                mock.sentinel.context
                )
        self.assertEqual(
            providers,
            mock_service_definition
            .return_value.get_available_providers.return_value,
        )

    @mock.patch.object(server.ConductorServerEndpoint, "_scheduler_client")
    @mock.patch.object(
        rpc_worker_client.WorkerClient, "from_service_definition"
    )
    def test_get_provider_schemas(
            self, mock_service_definition, mock_scheduler_client
    ):
        schemas = self.server.get_provider_schemas(
            mock.sentinel.context,
            mock.sentinel.platform_name,
            mock.sentinel.provider_type,
        )
        mock_service_definition.assert_called_once_with(
            mock_scheduler_client.get_any_worker_service(mock.sentinel.context)
        )
        mock_service_definition.return_value\
            .get_provider_schemas.assert_called_once_with(
                mock.sentinel.context,
                mock.sentinel.platform_name,
                mock.sentinel.provider_type,
                )
        self.assertEqual(
            schemas,
            mock_service_definition.return_value
            .get_provider_schemas.return_value,
        )

    @mock.patch.object(models, "Task")
    @mock.patch.object(uuid, "uuid4", return_value="task_id")
    def test_create_task(
            self, mock_uuid4, mock_task_model
    ):  # pylint: disable=unused-argument
        task1 = mock.sentinel.task1
        task1.id = mock.sentinel.task1_id
        task2 = mock.sentinel.task2
        task2.id = "task_id"
        task2.status = constants.TASK_STATUS_SCHEDULED
        execution = mock.sentinel.executions
        execution.tasks = [task1, task2]

        task = self.server._create_task(
            mock.sentinel.instance, mock.sentinel.task_type, execution
        )

        self.assertEqual(task.index, 3)
        self.assertEqual(task.instance, mock.sentinel.instance)
        self.assertEqual(task.status, constants.TASK_STATUS_SCHEDULED)

        # Handles depends_on
        task = self.server._create_task(
            mock.sentinel.instance,
            mock.sentinel.task_type,
            execution,
            depends_on=["task_id"],
            on_error=True,
        )
        self.assertEqual(task.status, constants.TASK_STATUS_SCHEDULED)
        self.assertEqual(task.on_error, True)
        task = self.server._create_task(
            mock.sentinel.instance,
            mock.sentinel.task_type,
            execution,
            depends_on=["other_task"],
            on_error=True,
        )
        self.assertEqual(task.status, constants.TASK_STATUS_ON_ERROR_ONLY)

        # Handles on_error_only
        task = self.server._create_task(
            mock.sentinel.instance,
            mock.sentinel.task_type,
            execution,
            depends_on=["task_id"],
            on_error_only=True,
        )
        self.assertEqual(task.on_error, True)
        self.assertEqual(task.status, constants.TASK_STATUS_ON_ERROR_ONLY)

        # Handles on_error with no depends_on
        task = self.server._create_task(
            mock.sentinel.instance,
            mock.sentinel.task_type,
            execution,
            on_error=True,
        )
        self.assertEqual(task.on_error, True)
        self.assertEqual(task.status, constants.TASK_STATUS_SCHEDULED)

    @mock.patch.object(
        rpc_worker_client.WorkerClient, "from_service_definition"
    )
    @mock.patch.object(db_api, "set_task_status")
    @mock.patch.object(server.ConductorServerEndpoint, "_scheduler_client")
    def test_get_worker_service_rpc_for_task(
            self,
            mock_scheduler_client,
            mock_set_task_status,
            mock_service_definition,
    ):
        task_mock = mock.Mock()
        service = self.server._get_worker_service_rpc_for_task(
            mock.sentinel.context,
            task_mock,
            mock.sentinel.origin_endpoint,
            mock.sentinel.destination_endpoint,
        )
        mock_scheduler_client.get_worker_service_for_task\
            .assert_called_once_with(
                mock.sentinel.context,
                {"id": task_mock.id, "task_type": task_mock.task_type},
                mock.sentinel.origin_endpoint,
                mock.sentinel.destination_endpoint,
                retry_count=5,
                retry_period=2,
                random_choice=True,
                )
        mock_service_definition.assert_called_once_with(
            mock_scheduler_client.get_worker_service_for_task.return_value
        )
        self.assertEqual(service, mock_service_definition.return_value)
        mock_set_task_status.assert_not_called()

        # Handles exception
        mock_scheduler_client.get_worker_service_for_task.side_effect = (
            Exception("test")
        )
        self.assertRaises(
            Exception,
            self.server._get_worker_service_rpc_for_task,
            mock.sentinel.context,
            task_mock,
            mock.sentinel.origin_endpoint,
            mock.sentinel.destination_endpoint,
        )
        mock_set_task_status.assert_called_once_with(
            mock.sentinel.context,
            task_mock.id,
            constants.TASK_STATUS_FAILED_TO_SCHEDULE,
            exception_details="test",
        )

    @mock.patch.object(server.ConductorServerEndpoint, "_create_task")
    @mock.patch.object(
        server.ConductorServerEndpoint, "_check_replica_running_executions"
    )
    @mock.patch.object(server.ConductorServerEndpoint, "_get_replica")
    def test_delete_replica_disks_invalid_state(
            self, mock_get_replica,
            mock_check_replica_running, mock_create_task
    ):
        mock_replica = mock_get_replica.return_value
        mock_replica.instances = [mock.sentinel.instance]
        mock_replica.info = {}
        delete_replica_disks = testutils.get_wrapped_function(
            self.server.delete_replica_disks
        )

        self.assertRaises(
            exception.InvalidReplicaState,
            delete_replica_disks,
            self.server,
            mock.sentinel.context,
            mock.sentinel.replica_id,
        )

        mock_get_replica.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.replica_id,
            include_task_info=True,
        )
        mock_check_replica_running.assert_called_once_with(
            mock.sentinel.context, mock_replica
        )
        mock_create_task.assert_not_called()

    @ddt.file_data("data/execution_tasks_config.yml")
    @ddt.unpack
    def test_check_execution_tasks_sanity(
            self, tasks_config, init_task_info, expected_result
    ):
        def convert_to_task(task_config):
            instance_task = mock.Mock()
            instance_task.instance = task_config.get(
                "instance", mock.sentinel.instance
            )
            instance_task.id = task_config.get("id", str(uuid.uuid4()))
            instance_task.status = task_config.get(
                "status", constants.TASK_STATUS_SCHEDULED
            )
            instance_task.depends_on = task_config.get("depends_on", None)
            instance_task.task_type = task_config.get(
                "task_type",
                constants.TASK_TYPE_DEPLOY_MIGRATION_SOURCE_RESOURCES,
            )
            return instance_task

        execution = mock.sentinel.execution
        execution.id = str(uuid.uuid4())
        execution.type = mock.sentinel.execution_type

        execution.tasks = [convert_to_task(t) for t in tasks_config]

        if init_task_info is not None:
            initial_task_info = init_task_info
        else:
            initial_task_info = {
                mock.sentinel.instance: {
                    "source_environment": mock.sentinel.source_environment,
                    "export_info": mock.sentinel.export_info,
                }
            }

        if not expected_result:
            self.server._check_execution_tasks_sanity(
                execution, initial_task_info
            )
        else:
            exception_mappings = {
                "INVALID_STATE": exception.InvalidTaskState,
                "MISSING_PARAMS": exception.TaskParametersException,
                "MISSING_DEPENDENCIES": exception.TaskDependencyException,
                "FIELDS_CONFLICT": exception.TaskFieldsConflict,
                "DEADLOCK": exception.ExecutionDeadlockException,
            }
            with self.assertRaisesRegex(
                exception_mappings[expected_result["type"]],
                expected_result.get("message", ""),
            ):
                self.server._check_execution_tasks_sanity(
                    execution, initial_task_info
                )

    @mock.patch.object(copy, "deepcopy")
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "get_replica_tasks_execution"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_begin_tasks"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_set_tasks_execution_status"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_minion_manager_client"
    )
    @mock.patch.object(db_api, "add_replica_tasks_execution")
    @mock.patch.object(db_api, "update_transfer_action_info_for_instance")
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_check_execution_tasks_sanity"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_create_task"
    )
    @mock.patch.object(uuid, "uuid4")
    @mock.patch.object(models, "TasksExecution")
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_check_minion_pools_for_action"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_check_replica_running_executions"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_check_reservation_for_transfer"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_get_replica"
    )
    @ddt.file_data("data/execute_replica_tasks_config.yml")
    @ddt.unpack
    def test_execute_replica_tasks(
            self,
            mock_get_replica,
            mock_check_reservation,
            mock_check_replica_running_executions,
            mock_check_minion_pools_for_action,
            mock_tasks_execution,
            mock_uuid4,  # pylint: disable=unused-argument
            mock_create_task,
            mock_check_execution_tasks_sanity,
            mock_update_transfer_action_info_for_instance,
            mock_add_replica_tasks_execution,
            mock_minion_manager_client,
            mock_set_tasks_execution_status,
            mock_begin_tasks,
            mock_get_replica_tasks_execution,
            mock_deepcopy,
            config,
            expected_tasks,
    ):
        has_origin_minion_pool = config.get("origin_minion_pool", False)
        has_target_minion_pool = config.get("target_minion_pool", False)
        shutdown_instances = config.get("shutdown_instances", False)

        def call_execute_replica_tasks():
            return testutils\
                .get_wrapped_function(self.server.execute_replica_tasks)(
                    self.server,
                    mock.sentinel.context,
                    mock.sentinel.replica_id,
                    shutdown_instances,  # type: ignore
                )

        instances = [mock.sentinel.instance1, mock.sentinel.instance2]
        mock_replica = mock.Mock(
            instances=instances,
            network_map=mock.sentinel.network_map,
            info={},
            origin_minion_pool_id=mock.sentinel.origin_minion_pool_id
            if has_origin_minion_pool else None,
            destination_minion_pool_id=mock.sentinel.destination_minion_pool_id
            if has_target_minion_pool else None,
        )
        mock_get_replica.return_value = mock_replica

        def create_task_side_effect(
                instance,
                task_type,
                execution,
                depends_on=None,
                on_error=False,
                on_error_only=False
        ):
            return mock.Mock(
                id=task_type,
                type=task_type,
                instance=instance,
                execution=execution,
                depends_on=depends_on,
                on_error=on_error,
                on_error_only=on_error_only,
            )

        mock_create_task.side_effect = create_task_side_effect

        result = call_execute_replica_tasks()
        mock_get_replica.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.replica_id,
            include_task_info=True,
        )
        mock_check_reservation.assert_called_once_with(
            mock_replica,
            licensing_client.RESERVATION_TYPE_REPLICA
        )
        mock_check_replica_running_executions.assert_called_once_with(
            mock.sentinel.context, mock_replica)
        mock_check_minion_pools_for_action.assert_called_once_with(
            mock.sentinel.context, mock_replica)

        mock_deepcopy.assert_called_once_with(
            mock_replica.destination_environment)

        for instance in instances:
            assert instance in mock_replica.info

            self.assertEqual(
                mock_replica.info[instance]['source_environment'],
                mock_replica.source_environment)

            self.assertEqual(
                mock_replica.info[instance]['target_environment'],
                mock_deepcopy.return_value)

            # generic tasks
            mock_create_task.assert_has_calls([
                mock.call(
                    instance,
                    constants.TASK_TYPE_VALIDATE_REPLICA_SOURCE_INPUTS,
                    mock_tasks_execution.return_value),
                mock.call(
                    instance,
                    constants.TASK_TYPE_GET_INSTANCE_INFO,
                    mock_tasks_execution.return_value),
                mock.call(
                    instance,
                    constants.TASK_TYPE_VALIDATE_REPLICA_DESTINATION_INPUTS,
                    mock_tasks_execution.return_value,
                    depends_on=[constants.TASK_TYPE_GET_INSTANCE_INFO]),
            ])

            # tasks defined in the yaml config
            for task in expected_tasks:
                kwargs = {}
                if 'on_error' in task:
                    kwargs = {'on_error': task['on_error']}
                mock_create_task.assert_has_calls([
                    mock.call(
                        instance,
                        task['type'],
                        mock_tasks_execution.return_value,
                        depends_on=task['depends_on'],
                        **kwargs,
                    )
                ])

            mock_update_transfer_action_info_for_instance.assert_has_calls([
                mock.call(
                    mock.sentinel.context,
                    mock_replica.id,
                    instance,
                    mock_replica.info[instance],
                )
            ])

        mock_check_execution_tasks_sanity.assert_called_once_with(
            mock_tasks_execution.return_value,
            mock_replica.info)

        mock_add_replica_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock_tasks_execution.return_value)

        if any([has_origin_minion_pool, has_target_minion_pool]):
            mock_minion_manager_client\
                .allocate_minion_machines_for_replica.assert_called_once_with(
                    mock.sentinel.context,
                    mock_replica,
                )
            mock_set_tasks_execution_status.assert_called_once_with(
                mock.sentinel.context,
                mock_tasks_execution.return_value,
                constants.EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS,
            )
        else:
            mock_begin_tasks.assert_called_once_with(
                mock.sentinel.context,
                mock_replica,
                mock_tasks_execution.return_value,
            )

        mock_get_replica_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.replica_id,
            mock_tasks_execution.return_value.id)

        self.assertEqual(
            mock_tasks_execution.return_value.status,
            constants.EXECUTION_STATUS_UNEXECUTED)
        self.assertEqual(
            mock_tasks_execution.return_value.type,
            constants.EXECUTION_TYPE_REPLICA_EXECUTION)
        self.assertEqual(result, mock_get_replica_tasks_execution.return_value)

    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_get_replica_tasks_execution'
    )
    @mock.patch.object(db_api, 'delete_replica_tasks_execution')
    def test_delete_replica_tasks_execution(
            self,
            mock_delete_replica_tasks_execution,
            mock_get_replica_tasks_execution
    ):
        def call_delete_replica_tasks_execution():
            return testutils.get_wrapped_function(
                self.server.delete_replica_tasks_execution)(
                self.server,
                mock.sentinel.context,
                mock.sentinel.replica_id,
                mock.sentinel.execution_id,  # type: ignore
            )
        call_delete_replica_tasks_execution()
        mock_get_replica_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.replica_id,
            mock.sentinel.execution_id)
        mock_delete_replica_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.execution_id)

        # raises exception if status is active
        mock_get_replica_tasks_execution.return_value.status = constants\
            .EXECUTION_STATUS_RUNNING

        self.assertRaises(
            exception.InvalidMigrationState,
            call_delete_replica_tasks_execution)

    @mock.patch.object(
        server.ConductorServerEndpoint,
        'get_replica_tasks_execution'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_begin_tasks'
    )
    @mock.patch.object(db_api, "add_replica_tasks_execution")
    @mock.patch.object(db_api, "update_transfer_action_info_for_instance")
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_check_execution_tasks_sanity'
    )
    @mock.patch.object(copy, "deepcopy")
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_create_task'
    )
    @mock.patch.object(uuid, "uuid4")
    @mock.patch.object(models, "TasksExecution")
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_check_replica_running_executions'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_get_replica'
    )
    def test_delete_replica_disks(
            self,
            mock_get_replica,
            mock_check_replica_running_executions,
            mock_tasks_execution,
            mock_uuid4,  # pylint: disable=unused-argument
            mock_create_task,
            mock_deepcopy,
            mock_check_execution_tasks_sanity,
            mock_update_transfer_action_info_for_instance,
            mock_add_replica_tasks_execution,
            mock_begin_tasks,
            mock_get_replica_tasks_execution,
    ):
        def call_delete_replica_disks():
            return testutils.get_wrapped_function(
                self.server.delete_replica_disks)(
                self.server,
                mock.sentinel.context,
                mock.sentinel.replica_id,  # type: ignore
            )
        instances = [mock.Mock(), mock.Mock()]
        mock_replica = mock.Mock(
            instances=instances,
            id=mock.sentinel.replica_id,
            network_map=mock.sentinel.network_map,
            info={
                instance: instance
                for instance in instances
            }
        )

        def create_task_side_effect(
                instance,
                task_type,
                execution,
                depends_on=None,
        ):
            return mock.Mock(
                id=task_type,
                type=task_type,
                instance=instance,
                execution=execution,
                depends_on=depends_on,
            )

        mock_create_task.side_effect = create_task_side_effect

        mock_get_replica.return_value = mock_replica
        result = call_delete_replica_disks()

        mock_get_replica.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.replica_id,
            include_task_info=True
        )
        mock_check_replica_running_executions.assert_called_once_with(
            mock.sentinel.context,
            mock_replica
        )

        self.assertEqual(
            mock_tasks_execution.return_value.status,
            constants.EXECUTION_STATUS_UNEXECUTED
        )
        self.assertEqual(
            mock_tasks_execution.return_value.type,
            constants.EXECUTION_TYPE_REPLICA_DISKS_DELETE
        )

        for instance in instances:
            assert instance in mock_replica.info

            mock_create_task.assert_has_calls([
                mock.call(
                    instance,
                    constants.TASK_TYPE_DELETE_REPLICA_SOURCE_DISK_SNAPSHOTS,
                    mock_tasks_execution.return_value,
                ),
                mock.call(
                    instance,
                    constants.TASK_TYPE_DELETE_REPLICA_DISKS,
                    mock_tasks_execution.return_value,
                    depends_on=[
                        constants
                        .TASK_TYPE_DELETE_REPLICA_SOURCE_DISK_SNAPSHOTS
                    ],
                ),
            ])

            mock_update_transfer_action_info_for_instance\
                .assert_has_calls([mock.call(
                    mock.sentinel.context,
                    mock_replica.id,
                    instance,
                    mock_replica.info[instance],
                )])

        mock_deepcopy.assert_called_once_with(
            mock_replica.destination_environment)
        mock_check_execution_tasks_sanity.assert_called_once_with(
            mock_tasks_execution.return_value,
            mock_replica.info,
        )
        mock_add_replica_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock_tasks_execution.return_value
        )
        mock_begin_tasks.assert_called_once_with(
            mock.sentinel.context,
            mock_replica,
            mock_tasks_execution.return_value
        )
        mock_get_replica_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock_replica.id,
            mock_tasks_execution.return_value.id
        )

        self.assertEqual(result, mock_get_replica_tasks_execution.return_value)

        # raises exception if instances have no volumes info
        instances[0].get.return_value = None
        instances[1].get.return_value = None

        self.assertRaises(
            exception.InvalidReplicaState,
            call_delete_replica_disks
        )

        # raises exception if instance not in replica.info
        instances[0].get.return_value = mock.sentinel.volume_info
        instances[1].get.return_value = mock.sentinel.volume_info
        mock_replica.info = {}

        self.assertRaises(
            exception.InvalidReplicaState,
            call_delete_replica_disks
        )

    def test_check_valid_replica_tasks_execution(self):
        execution1 = mock.Mock(
            number=1,
            type=constants.EXECUTION_TYPE_REPLICA_EXECUTION,
            status=constants.EXECUTION_STATUS_COMPLETED,
        )
        execution2 = mock.Mock(
            number=2,
            type=constants.EXECUTION_TYPE_REPLICA_EXECUTION,
            status=constants.EXECUTION_STATUS_COMPLETED,
        )
        mock_replica = mock.Mock(
            executions=[execution1, execution2]
        )
        self.server._check_valid_replica_tasks_execution(
            mock_replica
        )

        # raises exception if all executions are incomplete
        execution1.status = constants.EXECUTION_STATUS_UNEXECUTED
        execution2.status = constants.EXECUTION_STATUS_UNEXECUTED

        self.assertRaises(
            exception.InvalidReplicaState,
            self.server._check_valid_replica_tasks_execution,
            mock_replica
        )

        # doesn't raise exception if all executions are incomplete
        # and is forced
        self.server._check_valid_replica_tasks_execution(
            mock_replica,
            True
        )

        # doesn't raise exception if only one execution is completed
        execution1.status = constants.EXECUTION_STATUS_COMPLETED
        execution2.status = constants.EXECUTION_STATUS_UNEXECUTED

        self.server._check_valid_replica_tasks_execution(
            mock_replica
        )

    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_get_replica'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_check_reservation_for_transfer'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_check_replica_running_executions'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_check_valid_replica_tasks_execution'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        'get_endpoint'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_get_provider_types'
    )
    @mock.patch.object(models, "Migration")
    @mock.patch.object(uuid, "uuid4", return_value="migration_id")
    @mock.patch.object(copy, "deepcopy")
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_check_minion_pools_for_action'
    )
    @mock.patch.object(models, "TasksExecution")
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_get_instance_scripts'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_create_task'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_check_execution_tasks_sanity'
    )
    @mock.patch.object(db_api, 'add_migration')
    @mock.patch.object(lockutils, 'lock')
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_minion_manager_client"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_set_tasks_execution_status"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_begin_tasks"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "get_migration"
    )
    @ddt.file_data("data/deploy_replica_instance_config.yml")
    @ddt.unpack
    def test_deploy_replica_instance(
            self,
            mock_get_migration,
            mock_begin_tasks,
            mock_set_tasks_execution_status,
            mock_minion_manager_client,
            mock_lock,
            mock_add_migration,
            mock_check_execution_tasks_sanity,
            mock_create_task,
            mock_get_instance_scripts,
            mock_tasks_execution,
            mock_check_minion_pools_for_action,
            mock_deepcopy,  # pylint: disable=unused-argument
            mock_uuid4,  # pylint: disable=unused-argument
            mock_migration,
            mock_get_provider_types,
            mock_get_endpoint,
            mock_check_valid_replica_tasks_execution,
            mock_check_replica_running_executions,
            mock_check_reservation_for_transfer,
            mock_get_replica,
            config,
            expected_tasks,
    ):
        skip_os_morphing = config.get('skip_os_morphing', False)
        has_os_morphing_minion = config.get('has_os_morphing_minion', False)
        get_optimal_flavor = config.get('get_optimal_flavor', False)
        clone_disks = config.get('clone_disks', False)

        if get_optimal_flavor:
            mock_get_provider_types.return_value = [
                constants.PROVIDER_TYPE_INSTANCE_FLAVOR
            ]

        instance_osmorphing_minion_pool_mappings = None
        if not skip_os_morphing and has_os_morphing_minion:
            instance_osmorphing_minion_pool_mappings = {
                mock.sentinel.instance1: mock.sentinel.pool1,
                mock.sentinel.instance2: mock.sentinel.pool2,
            }

        mock_get_replica.return_value = mock.Mock(
            instances=[mock.sentinel.instance1, mock.sentinel.instance2],
            info={
                mock.sentinel.instance1: {
                    'volumes_info': mock.sentinel.volumes_info1
                },
                mock.sentinel.instance2: {
                    'volumes_info': {}
                },
            },
            instance_osmorphing_minion_pool_mappings={}
        )

        def call_deploy_replica_instance():
            return self.server.deploy_replica_instances(
                mock.sentinel.context,
                mock.sentinel.replica_id,
                clone_disks=clone_disks,
                force=False,
                instance_osmorphing_minion_pool_mappings=(
                    instance_osmorphing_minion_pool_mappings),
                skip_os_morphing=skip_os_morphing,
                user_scripts=mock.sentinel.user_scripts,
            )

        # One of the instances has no volumes info
        self.assertRaises(
            exception.InvalidReplicaState,
            call_deploy_replica_instance,
        )

        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context,
            mock_get_replica.return_value.destination_endpoint_id
        )

        mock_get_replica.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.replica_id,
            include_task_info=True,
        )
        mock_check_reservation_for_transfer.assert_called_once_with(
            mock_get_replica.return_value,
            licensing_client.RESERVATION_TYPE_REPLICA
        )
        mock_check_replica_running_executions.assert_called_once_with(
            mock.sentinel.context,
            mock_get_replica.return_value
        )
        mock_check_valid_replica_tasks_execution.assert_called_once_with(
            mock_get_replica.return_value,
            False
        )
        mock_get_provider_types.assert_called_once_with(
            mock.sentinel.context,
            mock_get_endpoint.return_value
        )

        # add the missing volumes info
        mock_get_replica.return_value.info[mock.sentinel.instance2] = {
            'volumes_info': mock.sentinel.volumes_info2
        }

        def create_task_side_effect(
                instance,
                task_type,
                execution,
                depends_on=None,
                on_error=False,
                on_error_only=False
        ):
            return mock.Mock(
                id=task_type,
                type=task_type,
                instance=instance,
                execution=execution,
                depends_on=depends_on,
                on_error=on_error,
                on_error_only=on_error_only,
            )

        mock_create_task.side_effect = create_task_side_effect

        # no longer raises exception
        migration = call_deploy_replica_instance()

        mock_check_minion_pools_for_action.assert_called_once_with(
            mock.sentinel.context,
            mock_migration.return_value
        )

        self.assertEqual(
            mock_tasks_execution.return_value.status,
            constants.EXECUTION_STATUS_UNEXECUTED
        )
        self.assertEqual(
            mock_tasks_execution.return_value.type,
            constants.EXECUTION_TYPE_REPLICA_DEPLOY
        )

        for instance in mock_get_replica.return_value.instances:
            mock_get_instance_scripts.assert_any_call(
                mock.sentinel.user_scripts,
                instance,
            )
            mock_create_task.assert_any_call(
                instance,
                constants.TASK_TYPE_VALIDATE_REPLICA_DEPLOYMENT_INPUTS,
                mock_tasks_execution.return_value,
            )

            # tasks defined in the yaml config
            for task in expected_tasks:
                kwargs = {}
                if 'on_error' in task:
                    kwargs = {'on_error': task['on_error']}
                if 'on_error_only' in task:
                    kwargs = {'on_error_only': task['on_error_only']}
                mock_create_task.assert_has_calls([
                    mock.call(
                        instance,
                        task['type'],
                        mock_tasks_execution.return_value,
                        depends_on=task['depends_on'],
                        **kwargs,
                    )
                ])

        mock_check_execution_tasks_sanity.assert_called_once_with(
            mock_tasks_execution.return_value,
            mock_migration.return_value.info,
        )

        mock_add_migration.assert_called_once_with(
            mock.sentinel.context,
            mock_migration.return_value,
        )

        if not skip_os_morphing and has_os_morphing_minion:
            mock_lock.assert_any_call(
                constants.MIGRATION_LOCK_NAME_FORMAT
                % mock_migration.return_value.id,
                external=True,
            )
            mock_minion_manager_client\
                .allocate_minion_machines_for_migration\
                .assert_called_once_with(
                    mock.sentinel.context,
                    mock_migration.return_value,
                    include_transfer_minions=False,
                    include_osmorphing_minions=True
                )
            mock_set_tasks_execution_status.assert_called_once_with(
                mock.sentinel.context,
                mock_tasks_execution.return_value,
                constants.EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS
            )
        else:
            mock_begin_tasks.assert_called_once_with(
                mock.sentinel.context,
                mock_migration.return_value,
                mock_tasks_execution.return_value,
            )

        mock_get_migration.assert_called_once_with(
            mock.sentinel.context,
            mock_migration.return_value.id,
        )

        self.assertEqual(
            migration,
            mock_get_migration.return_value
        )
