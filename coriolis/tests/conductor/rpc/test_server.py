# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import copy
import logging
from unittest import mock
import uuid

import ddt
from oslo_concurrency import lockutils
from oslo_config import cfg

from coriolis.conductor.rpc import server
from coriolis import constants
from coriolis import context
from coriolis.db import api as db_api
from coriolis.db.sqlalchemy import models
from coriolis import exception
from coriolis import keystone
from coriolis import schemas
from coriolis.tests import test_base
from coriolis.tests import testutils
from coriolis import utils
from coriolis.worker.rpc import client as rpc_worker_client


class CoriolisTestException(Exception):
    pass


@ddt.ddt
class ConductorServerEndpointTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Conductor RPC server."""

    def setUp(self):
        super(ConductorServerEndpointTestCase, self).setUp()
        self.server = server.ConductorServerEndpoint()
        self._licensing_client = mock.Mock()
        self.server._licensing_client = self._licensing_client

    @mock.patch.object(
        rpc_worker_client.WorkerClient, "from_service_definition"
    )
    @mock.patch.object(server.ConductorServerEndpoint, "_scheduler_client")
    def test_get_all_diagnostics(
        self,
        mock_scheduler_client,
        mock_from_service_definition
    ):
        mock_scheduler_client.get_workers_for_specs.side_effect = (
            CoriolisTestException())
        self.assertRaises(
            CoriolisTestException,
            lambda: self.server.get_all_diagnostics(mock.sentinel.context),
        )
        mock_scheduler_client.get_workers_for_specs.side_effect = None

        diagnostics = self.server.get_all_diagnostics(mock.sentinel.context)
        assert (
            mock_scheduler_client.get_diagnostics.return_value in diagnostics
        )
        mock_scheduler_client.get_workers_for_specs.return_value = {
            mock.sentinel.diagnostics
        }
        diagnostics = self.server.get_all_diagnostics(mock.sentinel.context)
        assert (
            mock_scheduler_client.get_diagnostics.return_value in diagnostics
        )
        mock_scheduler_client.get_workers_for_specs.return_value = [{
            'host': mock.sentinel.host
        }]
        mock_from_service_definition.side_effect = (
            mock.sentinel.rpc_worker, CoriolisTestException())
        self.assertRaises(
            CoriolisTestException,
            lambda: self.server.get_all_diagnostics(mock.sentinel.context),
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

    def test_check_delete_reservation_for_transfer(self):
        transfer_action = mock.Mock()
        self.server._check_delete_reservation_for_transfer(transfer_action)
        self._licensing_client.delete_reservation.assert_called_once_with(
            transfer_action.reservation_id
        )

    def test_check_delete_reservation_for_transfer_no_licensing_client(self):
        transfer_action = mock.Mock()
        self.server._licensing_client = None
        with self.assertLogs(
            'coriolis.conductor.rpc.server', level=logging.WARNING):
            self.server._check_delete_reservation_for_transfer(transfer_action)

    def test_check_delete_reservation_for_transfer_delete_fails(self):
        transfer_action = mock.Mock()
        self._licensing_client.delete_reservation.side_effect = \
            CoriolisTestException()
        with self.assertLogs(
            'coriolis.conductor.rpc.server', level=logging.WARNING):
            self.server._check_delete_reservation_for_transfer(transfer_action)
        self._licensing_client.delete_reservation.assert_called_once_with(
            transfer_action.reservation_id
        )

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
        mock_update_endpoint.side_effect = CoriolisTestException()
        self.assertRaises(
            CoriolisTestException,
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

    @mock.patch.object(db_api, "get_endpoint")
    def test_get_endpoint_not_found(self, mock_get_endpoint):
        def call_get_endpoint():
            return testutils.get_wrapped_function(self.server.get_endpoint)(
                self, mock.sentinel.context,
                mock.sentinel.endpoint_id  # type: ignore
            )

        mock_get_endpoint.return_value = None
        self.assertRaises(exception.NotFound, call_get_endpoint)

    @mock.patch.object(db_api, "delete_endpoint")
    @mock.patch.object(db_api, "get_endpoint_transfers_count")
    def test_delete_endpoint(
            self, mock_get_endpoint_transfers_count, mock_delete_endpoint
    ):
        def call_delete_endpoint():
            return testutils.get_wrapped_function(self.server.delete_endpoint)(
                self, mock.sentinel.context,
                mock.sentinel.endpoint_id  # type: ignore
            )

        mock_get_endpoint_transfers_count.return_value = 0
        call_delete_endpoint()
        mock_delete_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.endpoint_id
        )

        # endpoint has transfers
        mock_get_endpoint_transfers_count.return_value = 1
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
            mock_scheduler_client.get_any_worker_service(
                mock.sentinel.context))
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
        provider_schemas = self.server.get_provider_schemas(
            mock.sentinel.context,
            mock.sentinel.platform_name,
            mock.sentinel.provider_type,
        )
        mock_service_definition.assert_called_once_with(
            mock_scheduler_client.get_any_worker_service(
                mock.sentinel.context))
        mock_service_definition.return_value\
            .get_provider_schemas.assert_called_once_with(
                mock.sentinel.context,
                mock.sentinel.platform_name,
                mock.sentinel.provider_type,
            )
        self.assertEqual(
            provider_schemas,
            mock_service_definition.return_value
            .get_provider_schemas.return_value,
        )

    @mock.patch.object(models, "Task")
    @mock.patch.object(uuid, "uuid4", return_value="task_id")
    def test_create_task(
            self, mock_uuid4, mock_task_model
    ):
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

    @mock.patch.object(server.ConductorServerEndpoint, "get_endpoint")
    def test_get_task_origin(self, mock_get_endpoint):
        mock_endpoint = mock.Mock()
        mock_endpoint.connection_info = mock.sentinel.connection_info
        mock_endpoint.type = mock.sentinel.type
        mock_get_endpoint.return_value = mock_endpoint
        action = mock.Mock()
        action.origin_endpoint_id = mock.sentinel.origin_endpoint_id
        action.source_environment = mock.sentinel.source_environment

        expected_result = {
            "connection_info": mock.sentinel.connection_info,
            "type": mock.sentinel.type,
            "source_environment": mock.sentinel.source_environment
        }

        result = self.server._get_task_origin(
            mock.sentinel.context,
            action
        )

        self.assertEqual(
            expected_result,
            result
        )

        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin_endpoint_id)

    @mock.patch.object(server.ConductorServerEndpoint, "get_endpoint")
    def test_get_task_destination(self, mock_get_endpoint):
        mock_endpoint = mock.Mock()
        mock_endpoint.connection_info = mock.sentinel.connection_info
        mock_endpoint.type = mock.sentinel.type
        mock_get_endpoint.return_value = mock_endpoint
        action = mock.Mock()
        action.destination_endpoint_id = mock.sentinel.destination_endpoint_id
        action.destination_environment = mock.sentinel.destination_environment

        expected_result = {
            "connection_info": mock.sentinel.connection_info,
            "type": mock.sentinel.type,
            "target_environment": mock.sentinel.destination_environment
        }

        result = self.server._get_task_destination(
            mock.sentinel.context,
            action
        )

        self.assertEqual(
            expected_result,
            result
        )

        mock_get_endpoint.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.destination_endpoint_id)

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
            CoriolisTestException("test")
        )
        self.assertRaises(
            CoriolisTestException,
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

    @mock.patch.object(server.ConductorServerEndpoint,
                       "_set_tasks_execution_status")
    @mock.patch.object(server.ConductorServerEndpoint,
                       "_get_worker_service_rpc_for_task")
    @mock.patch.object(db_api, "set_task_status")
    @mock.patch.object(db_api, "get_endpoint")
    @mock.patch.object(server.ConductorServerEndpoint, "_get_task_destination")
    @mock.patch.object(server.ConductorServerEndpoint, "_get_task_origin")
    @mock.patch.object(keystone, 'create_trust')
    def test_begin_tasks(
        self,
        mock_create_trust,
        mock_get_task_origin,
        mock_get_task_destination,
        mock_get_endpoint,
        mock_set_task_status,
        mock_get_worker_service_rpc_for_task,
        mock_set_tasks_execution_status,
    ):
        mock_context = mock.Mock()
        mock_action = mock.Mock()
        task_info = {'instance_1': "mock_instance_1"}
        expected_task_instance = "mock_instance_1"
        mock_action.info = task_info
        mock_context.trust_id = None
        mock_get_endpoint.side_effect = [
            mock.sentinel.origin_endpoint_id,
            mock.sentinel.destination_endpoint_id]

        task = mock.Mock(
            id=mock.sentinel.task_id,
            index=mock.sentinel.task_index,
            status=constants.TASK_STATUS_SCHEDULED,
            instance='instance_1',
            depends_on=None,
            on_error=None,
        )
        execution = mock.Mock(
            id=mock.sentinel.execution_id,
            tasks=[task]
        )

        self.server._begin_tasks(
            mock_context,
            mock_action,
            execution,
            task_info_override=task_info,
            scheduling_retry_count=mock.sentinel.scheduling_retry_count,
            scheduling_retry_period=mock.sentinel.scheduling_retry_period,
        )

        mock_create_trust.assert_called_once_with(mock_context)
        mock_get_task_origin.assert_called_once_with(mock_context, mock_action)
        mock_get_task_destination.assert_called_once_with(
            mock_context, mock_action)
        mock_get_endpoint.assert_has_calls([
            mock.call(mock_context, mock_action.origin_endpoint_id),
            mock.call(mock_context, mock_action.destination_endpoint_id)
        ])
        mock_set_task_status.assert_called_once_with(
            mock_context, mock.sentinel.task_id, constants.TASK_STATUS_PENDING)
        mock_get_worker_service_rpc_for_task.assert_called_once_with(
            mock_context,
            task,
            mock.sentinel.origin_endpoint_id,
            mock.sentinel.destination_endpoint_id,
            retry_count=mock.sentinel.scheduling_retry_count,
            retry_period=mock.sentinel.scheduling_retry_period
        )
        (mock_get_worker_service_rpc_for_task.return_value.begin_task.
         assert_called_once_with)(
            mock_context,
            task_id=mock.sentinel.task_id,
            task_type=task.task_type,
            origin=mock_get_task_origin.return_value,
            destination=mock_get_task_destination.return_value,
            instance=task.instance,
            task_info=expected_task_instance
        )
        mock_set_tasks_execution_status.assert_called_once_with(
            mock_context, execution, constants.TASK_STATUS_RUNNING
        )

    @mock.patch.object(server.ConductorServerEndpoint,
                       "_cancel_tasks_execution")
    @mock.patch.object(server.ConductorServerEndpoint,
                       "_get_worker_service_rpc_for_task")
    @mock.patch.object(db_api, "set_task_status")
    @mock.patch.object(db_api, "get_endpoint")
    @mock.patch.object(server.ConductorServerEndpoint, "_get_task_destination")
    @mock.patch.object(server.ConductorServerEndpoint, "_get_task_origin")
    @mock.patch.object(keystone, 'create_trust')
    def test_begin_tasks_begin_task_raises(
        self,
        mock_create_trust,
        mock_get_task_origin,
        mock_get_task_destination,
        mock_get_endpoint,
        mock_set_task_status,
        mock_get_worker_service_rpc_for_task,
        mock_cancel_tasks_execution
    ):
        mock_context = mock.Mock()
        task_info = {'instance_1': "mock_instance_1"}
        expected_task_instance = "mock_instance_1"
        mock_action = mock.Mock()
        mock_action.info = task_info
        mock_context.trust_id = None
        mock_get_endpoint.side_effect = [
            mock.sentinel.origin_endpoint_id,
            mock.sentinel.destination_endpoint_id]

        task = mock.Mock(
            id=mock.sentinel.task_id,
            index=mock.sentinel.task_index,
            status=constants.TASK_STATUS_SCHEDULED,
            instance='instance_1',
            depends_on=None,
            on_error=None,
        )
        execution = mock.Mock(
            id=mock.sentinel.execution_id,
            tasks=[task]
        )
        (mock_get_worker_service_rpc_for_task.return_value.begin_task
            .side_effect) = CoriolisTestException()

        self.assertRaises(
            CoriolisTestException,
            self.server._begin_tasks,
            mock_context,
            mock_action,
            execution,
            task_info_override=task_info,
            scheduling_retry_count=mock.sentinel.scheduling_retry_count,
            scheduling_retry_period=mock.sentinel.scheduling_retry_period,
        )

        mock_create_trust.assert_called_once_with(mock_context)
        mock_get_task_origin.assert_called_once_with(mock_context, mock_action)
        mock_get_task_destination.assert_called_once_with(
            mock_context, mock_action)
        mock_get_endpoint.assert_has_calls([
            mock.call(mock_context, mock_action.origin_endpoint_id),
            mock.call(mock_context, mock_action.destination_endpoint_id)
        ])
        mock_set_task_status.assert_called_once_with(
            mock_context, mock.sentinel.task_id, constants.TASK_STATUS_PENDING)
        mock_get_worker_service_rpc_for_task.assert_called_once_with(
            mock_context,
            task,
            mock.sentinel.origin_endpoint_id,
            mock.sentinel.destination_endpoint_id,
            retry_count=mock.sentinel.scheduling_retry_count,
            retry_period=mock.sentinel.scheduling_retry_period
        )
        (mock_get_worker_service_rpc_for_task.return_value.begin_task.
         assert_called_once_with)(
            mock_context,
            task_id=mock.sentinel.task_id,
            task_type=task.task_type,
            origin=mock_get_task_origin.return_value,
            destination=mock_get_task_destination.return_value,
            instance=task.instance,
            task_info=expected_task_instance
        )
        mock_cancel_tasks_execution.assert_called_once_with(
            mock_context, execution, requery=True)

    @mock.patch.object(server.ConductorServerEndpoint,
                       "_cancel_tasks_execution")
    @mock.patch.object(server.ConductorServerEndpoint,
                       "_get_worker_service_rpc_for_task")
    @mock.patch.object(db_api, "set_task_status")
    @mock.patch.object(db_api, "get_endpoint")
    @mock.patch.object(server.ConductorServerEndpoint, "_get_task_destination")
    @mock.patch.object(server.ConductorServerEndpoint, "_get_task_origin")
    @mock.patch.object(keystone, 'create_trust')
    def test_begin_tasks_no_newly_started_tasks(
        self,
        mock_create_trust,
        mock_get_task_origin,
        mock_get_task_destination,
        mock_get_endpoint,
        mock_set_task_status,
        mock_get_worker_service_rpc_for_task,
        mock_cancel_tasks_execution
    ):
        mock_context = mock.Mock()
        task_info = mock.Mock()
        mock_action = mock.Mock()
        task_info.instance = {}
        mock_action.info = task_info
        mock_context.trust_id = None
        mock_get_endpoint.side_effect = [
            mock.sentinel.origin_endpoint_id,
            mock.sentinel.destination_endpoint_id]

        task = mock.Mock(
            id=mock.sentinel.task_id,
            index=mock.sentinel.task_index,
            status=constants.TASK_STATUS_PENDING,
            depends_on=None,
            on_error=None,
        )
        execution = mock.Mock(
            id=mock.sentinel.execution_id,
            tasks=[task]
        )

        self.assertRaises(
            exception.InvalidActionTasksExecutionState,
            self.server._begin_tasks,
            mock_context,
            mock_action,
            execution,
            task_info,
            mock.sentinel.scheduling_retry_count,
            mock.sentinel.scheduling_retry_period,
        )

        mock_create_trust.assert_called_once_with(mock_context)
        mock_get_task_origin.assert_called_once_with(mock_context, mock_action)
        mock_get_task_destination.assert_called_once_with(
            mock_context, mock_action)
        mock_get_endpoint.assert_has_calls([
            mock.call(mock_context, mock_action.origin_endpoint_id),
            mock.call(mock_context, mock_action.destination_endpoint_id)
        ])
        mock_set_task_status.assert_not_called()
        mock_get_worker_service_rpc_for_task.assert_not_called()
        (mock_get_worker_service_rpc_for_task.return_value.begin_task.
         assert_not_called)()
        mock_cancel_tasks_execution.assert_not_called()

    @mock.patch.object(server.ConductorServerEndpoint, "_create_task")
    @mock.patch.object(
        server.ConductorServerEndpoint, "_check_transfer_running_executions"
    )
    @mock.patch.object(server.ConductorServerEndpoint, "_get_transfer")
    def test_delete_transfer_disks_invalid_state(
            self, mock_get_transfer,
            mock_check_transfer_running, mock_create_task
    ):
        mock_transfer = mock_get_transfer.return_value
        mock_transfer.instances = [mock.sentinel.instance]
        mock_transfer.info = {}
        delete_transfer_disks = testutils.get_wrapped_function(
            self.server.delete_transfer_disks
        )

        self.assertRaises(
            exception.InvalidTransferState,
            delete_transfer_disks,
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
        )

        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            include_task_info=True,
        )
        mock_check_transfer_running.assert_called_once_with(
            mock.sentinel.context, mock_transfer
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
                constants.TASK_TYPE_DEPLOY_TRANSFER_SOURCE_RESOURCES,
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
        "get_transfer_tasks_execution"
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
    @mock.patch.object(db_api, "add_transfer_tasks_execution")
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
        "_check_transfer_running_executions"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_check_reservation_for_transfer"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_get_transfer"
    )
    @ddt.file_data("data/execute_transfer_tasks_config.yml")
    @ddt.unpack
    def test_execute_transfer_tasks(
            self,
            mock_get_transfer,
            mock_check_reservation,
            mock_check_transfer_running_executions,
            mock_check_minion_pools_for_action,
            mock_tasks_execution,
            mock_uuid4,
            mock_create_task,
            mock_check_execution_tasks_sanity,
            mock_update_transfer_action_info_for_instance,
            mock_add_transfer_tasks_execution,
            mock_minion_manager_client,
            mock_set_tasks_execution_status,
            mock_begin_tasks,
            mock_get_transfer_tasks_execution,
            mock_deepcopy,
            config,
            expected_tasks,
    ):
        has_origin_minion_pool = config.get("origin_minion_pool", False)
        has_target_minion_pool = config.get("target_minion_pool", False)
        shutdown_instances = config.get("shutdown_instances", False)

        def call_execute_transfer_tasks():
            return testutils\
                .get_wrapped_function(self.server.execute_transfer_tasks)(
                    self.server,
                    mock.sentinel.context,
                    mock.sentinel.transfer_id,
                    shutdown_instances,  # type: ignore
                )

        instances = [mock.sentinel.instance1, mock.sentinel.instance2]
        mock_transfer = mock.Mock(
            instances=instances,
            network_map=mock.sentinel.network_map,
            info={mock.sentinel.instance1: {'volume_info': None}},
            origin_minion_pool_id=mock.sentinel.origin_minion_pool_id
            if has_origin_minion_pool else None,
            destination_minion_pool_id=mock.sentinel.destination_minion_pool_id
            if has_target_minion_pool else None,
        )
        mock_get_transfer.return_value = mock_transfer

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

        result = call_execute_transfer_tasks()
        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            include_task_info=True,
        )
        mock_check_reservation.assert_called_once_with(mock_transfer)
        mock_check_transfer_running_executions.assert_called_once_with(
            mock.sentinel.context, mock_transfer)
        mock_check_minion_pools_for_action.assert_called_once_with(
            mock.sentinel.context, mock_transfer)

        mock_deepcopy.assert_called_once_with(
            mock_transfer.destination_environment)

        for instance in instances:
            assert instance in mock_transfer.info

            self.assertEqual(
                mock_transfer.info[instance]['source_environment'],
                mock_transfer.source_environment)

            self.assertEqual(
                mock_transfer.info[instance]['target_environment'],
                mock_deepcopy.return_value)

            # generic tasks
            mock_create_task.assert_has_calls([
                mock.call(
                    instance,
                    constants.TASK_TYPE_VALIDATE_TRANSFER_SOURCE_INPUTS,
                    mock_tasks_execution.return_value),
                mock.call(
                    instance,
                    constants.TASK_TYPE_GET_INSTANCE_INFO,
                    mock_tasks_execution.return_value),
                mock.call(
                    instance,
                    constants.TASK_TYPE_VALIDATE_TRANSFER_DESTINATION_INPUTS,
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
                    mock_transfer.id,
                    instance,
                    mock_transfer.info[instance],
                )
            ])

        mock_check_execution_tasks_sanity.assert_called_once_with(
            mock_tasks_execution.return_value,
            mock_transfer.info)

        mock_add_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock_tasks_execution.return_value)

        if any([has_origin_minion_pool, has_target_minion_pool]):
            mock_minion_manager_client\
                .allocate_minion_machines_for_transfer.assert_called_once_with(
                    mock.sentinel.context,
                    mock_transfer,
                )
            mock_set_tasks_execution_status.assert_called_once_with(
                mock.sentinel.context,
                mock_tasks_execution.return_value,
                constants.EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS,
            )
        else:
            mock_begin_tasks.assert_called_once_with(
                mock.sentinel.context,
                mock_transfer,
                mock_tasks_execution.return_value,
                delete_trust_id=True
            )

        mock_get_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock_tasks_execution.return_value.id)

        self.assertEqual(
            mock_tasks_execution.return_value.status,
            constants.EXECUTION_STATUS_UNEXECUTED)
        self.assertEqual(
            mock_tasks_execution.return_value.type,
            constants.EXECUTION_TYPE_TRANSFER_EXECUTION)
        self.assertEqual(
            result, mock_get_transfer_tasks_execution.return_value)

    @mock.patch.object(db_api, "get_transfer_tasks_executions")
    def test_get_transfer_tasks_executions(
        self,
        mock_get_transfer_tasks_executions
    ):
        result = testutils.get_wrapped_function(
            self.server.get_transfer_tasks_executions)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id,
            include_task_info=False
        )

        self.assertEqual(
            mock_get_transfer_tasks_executions.return_value,
            result
        )
        mock_get_transfer_tasks_executions.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id,
            include_task_info=False,
            to_dict=True
        )

    @mock.patch.object(db_api, "get_transfer_tasks_execution")
    def test_get_transfer_tasks_execution(
        self,
        mock_get_transfer_tasks_execution
    ):
        result = testutils.get_wrapped_function(
            self.server.get_transfer_tasks_execution)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id,
            include_task_info=False
        )

        self.assertEqual(
            mock_get_transfer_tasks_execution.return_value,
            result
        )
        mock_get_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id,
            include_task_info=False,
            to_dict=True
        )

    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_get_transfer_tasks_execution'
    )
    @mock.patch.object(db_api, 'delete_transfer_tasks_execution')
    def test_delete_transfer_tasks_execution(
            self,
            mock_delete_transfer_tasks_execution,
            mock_get_transfer_tasks_execution
    ):
        def call_delete_transfer_tasks_execution():
            return testutils.get_wrapped_function(
                self.server.delete_transfer_tasks_execution)(
                self.server,
                mock.sentinel.context,
                mock.sentinel.transfer_id,
                mock.sentinel.execution_id,  # type: ignore
            )
        call_delete_transfer_tasks_execution()
        mock_get_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id)
        mock_delete_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.execution_id)

        # raises exception if status is active
        mock_get_transfer_tasks_execution.return_value.status = (
            constants.EXECUTION_STATUS_RUNNING)

        self.assertRaises(
            exception.InvalidActionTasksExecutionState,
            call_delete_transfer_tasks_execution)

    @mock.patch.object(server.ConductorServerEndpoint,
                       '_get_transfer_tasks_execution')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_cancel_tasks_execution')
    def test_cancel_transfer_tasks_execution(
            self,
            mock_cancel_transfer_tasks_execution,
            mock_get_transfer_tasks_execution
    ):
        mock_get_transfer_tasks_execution.return_value.status = constants\
            .EXECUTION_STATUS_RUNNING
        testutils.get_wrapped_function(
            self.server.cancel_transfer_tasks_execution)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id,
            False
        )
        mock_get_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id)
        mock_cancel_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock_get_transfer_tasks_execution.return_value,
            force=False)

        mock_get_transfer_tasks_execution.reset_mock()
        mock_cancel_transfer_tasks_execution.reset_mock()
        mock_get_transfer_tasks_execution.return_value.status = constants\
            .EXECUTION_STATUS_CANCELLING
        testutils.get_wrapped_function(
            self.server.cancel_transfer_tasks_execution)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id,
            True
        )
        mock_get_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id)
        mock_cancel_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock_get_transfer_tasks_execution.return_value,
            force=True)

    @mock.patch.object(server.ConductorServerEndpoint,
                       '_get_transfer_tasks_execution')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_cancel_tasks_execution')
    def test_cancel_transfer_tasks_execution_status_not_active(
            self,
            mock_cancel_transfer_tasks_execution,
            mock_get_transfer_tasks_execution
    ):
        self.assertRaises(
            exception.InvalidTransferState,
            testutils.get_wrapped_function(
                self.server.cancel_transfer_tasks_execution),
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id,
            False
        )
        mock_get_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id)
        mock_cancel_transfer_tasks_execution.assert_not_called()

    @mock.patch.object(server.ConductorServerEndpoint,
                       '_get_transfer_tasks_execution')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_cancel_tasks_execution')
    def test_cancel_transfer_tasks_execution_status_cancelling_no_force(
            self,
            mock_cancel_transfer_tasks_execution,
            mock_get_transfer_tasks_execution
    ):
        mock_get_transfer_tasks_execution.return_value.status = constants\
            .EXECUTION_STATUS_CANCELLING
        self.assertRaises(
            exception.InvalidTransferState,
            testutils.get_wrapped_function(
                self.server.cancel_transfer_tasks_execution),
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id,
            False
        )
        mock_get_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id)
        mock_cancel_transfer_tasks_execution.assert_not_called()

    @mock.patch.object(db_api, 'get_transfer_tasks_execution')
    def test__get_transfer_tasks_execution(
            self,
            mock_get_transfer_tasks_execution
    ):
        result = self.server._get_transfer_tasks_execution(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id,
            include_task_info=False,
            to_dict=False,
        )
        self.assertEqual(
            mock_get_transfer_tasks_execution.return_value,
            result
        )
        mock_get_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id,
            include_task_info=False,
            to_dict=False)

    @mock.patch.object(db_api, 'get_transfer_tasks_execution')
    def test__get_transfer_tasks_execution_no_execution(
            self,
            mock_get_transfer_tasks_execution
    ):
        mock_get_transfer_tasks_execution.return_value = None
        self.assertRaises(
            exception.NotFound,
            self.server._get_transfer_tasks_execution,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id,
            include_task_info=False,
            to_dict=False,
        )

        mock_get_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.execution_id,
            include_task_info=False,
            to_dict=False)

    @mock.patch.object(db_api, 'get_transfers')
    def test_get_transfers(self, mock_get_transfers):
        result = self.server.get_transfers(
            mock.sentinel.context,
            include_tasks_executions=False,
            include_task_info=False
        )

        self.assertEqual(
            mock_get_transfers.return_value,
            result
        )
        mock_get_transfers.assert_called_once_with(
            mock.sentinel.context,
            False,
            include_task_info=False,
            to_dict=True
        )

    @mock.patch.object(server.ConductorServerEndpoint, '_get_transfer')
    def test_get_transfer(self, mock_get_transfer):
        result = testutils.get_wrapped_function(self.server.get_transfer)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            include_task_info=False
        )

        self.assertEqual(
            mock_get_transfer.return_value,
            result
        )
        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            include_task_info=False,
            to_dict=True
        )

    @mock.patch.object(db_api, 'delete_transfer')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_check_delete_reservation_for_transfer')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_check_transfer_running_executions')
    @mock.patch.object(server.ConductorServerEndpoint, '_get_transfer')
    def test_delete_transfer(
        self,
        mock_get_transfer,
        mock_check_transfer_running_executions,
        mock_check_delete_reservation_for_transfer,
        mock_delete_transfer,
    ):
        testutils.get_wrapped_function(self.server.delete_transfer)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id
        )
        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.transfer_id)
        mock_check_transfer_running_executions.assert_called_once_with(
            mock.sentinel.context, mock_get_transfer.return_value)
        mock_check_delete_reservation_for_transfer.assert_called_once_with(
            mock_get_transfer.return_value)
        mock_delete_transfer.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.transfer_id)

    @mock.patch.object(
        server.ConductorServerEndpoint,
        'get_transfer_tasks_execution'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_begin_tasks'
    )
    @mock.patch.object(db_api, "add_transfer_tasks_execution")
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
        '_check_transfer_running_executions'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_get_transfer'
    )
    def test_delete_transfer_disks(
            self,
            mock_get_transfer,
            mock_check_transfer_running_executions,
            mock_tasks_execution,
            mock_uuid4,
            mock_create_task,
            mock_deepcopy,
            mock_check_execution_tasks_sanity,
            mock_update_transfer_action_info_for_instance,
            mock_add_transfer_tasks_execution,
            mock_begin_tasks,
            mock_get_transfer_tasks_execution,
    ):
        def call_delete_transfer_disks():
            return testutils.get_wrapped_function(
                self.server.delete_transfer_disks)(
                self.server,
                mock.sentinel.context,
                mock.sentinel.transfer_id,  # type: ignore
            )
        instances = [mock.Mock(), mock.Mock()]
        mock_transfer = mock.Mock(
            instances=instances,
            id=mock.sentinel.transfer_id,
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

        mock_get_transfer.return_value = mock_transfer
        result = call_delete_transfer_disks()

        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            include_task_info=True
        )
        mock_check_transfer_running_executions.assert_called_once_with(
            mock.sentinel.context,
            mock_transfer
        )

        self.assertEqual(
            mock_tasks_execution.return_value.status,
            constants.EXECUTION_STATUS_UNEXECUTED
        )
        self.assertEqual(
            mock_tasks_execution.return_value.type,
            constants.EXECUTION_TYPE_TRANSFER_DISKS_DELETE
        )

        for instance in instances:
            assert instance in mock_transfer.info

            mock_create_task.assert_has_calls([
                mock.call(
                    instance,
                    constants.TASK_TYPE_DELETE_TRANSFER_SOURCE_DISK_SNAPSHOTS,
                    mock_tasks_execution.return_value,
                ),
                mock.call(
                    instance,
                    constants.TASK_TYPE_DELETE_TRANSFER_DISKS,
                    mock_tasks_execution.return_value,
                    depends_on=[
                        constants
                        .TASK_TYPE_DELETE_TRANSFER_SOURCE_DISK_SNAPSHOTS
                    ],
                ),
            ])

            mock_update_transfer_action_info_for_instance\
                .assert_has_calls([mock.call(
                    mock.sentinel.context,
                    mock_transfer.id,
                    instance,
                    mock_transfer.info[instance],
                )])

        mock_deepcopy.assert_called_once_with(
            mock_transfer.destination_environment)
        mock_check_execution_tasks_sanity.assert_called_once_with(
            mock_tasks_execution.return_value,
            mock_transfer.info,
        )
        mock_add_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock_tasks_execution.return_value
        )
        mock_begin_tasks.assert_called_once_with(
            mock.sentinel.context,
            mock_transfer,
            mock_tasks_execution.return_value
        )
        mock_get_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock_transfer.id,
            mock_tasks_execution.return_value.id
        )

        self.assertEqual(
            result, mock_get_transfer_tasks_execution.return_value)

        # raises exception if instances have no volumes info
        instances[0].get.return_value = None
        instances[1].get.return_value = None

        self.assertRaises(
            exception.InvalidTransferState,
            call_delete_transfer_disks
        )

        # raises exception if instance not in transfer.info
        instances[0].get.return_value = mock.sentinel.volume_info
        instances[1].get.return_value = mock.sentinel.volume_info
        mock_transfer.info = {}

        self.assertRaises(
            exception.InvalidTransferState,
            call_delete_transfer_disks
        )

    def test_check_endpoints(self):
        origin_endpoint = mock.Mock()
        destination_endpoint = mock.Mock()
        self.server._check_endpoints(
            mock.sentinel.context, origin_endpoint, destination_endpoint)

    def test_check_endpoints_same_destination_id(self):
        origin_endpoint = mock.Mock()
        destination_endpoint = mock.Mock()
        origin_endpoint.id = mock.sentinel.origin_endpoint_id
        destination_endpoint.id = mock.sentinel.origin_endpoint_id
        self.assertRaises(
            exception.SameDestination,
            self.server._check_endpoints,
            mock.sentinel.context,
            origin_endpoint,
            destination_endpoint
        )

    def test_check_endpoints_same_destination_connection_info(self):
        origin_endpoint = mock.Mock()
        destination_endpoint = mock.Mock()
        origin_endpoint.connection_info = \
            mock.sentinel.origin_endpoint_connection_info
        destination_endpoint.connection_info = \
            mock.sentinel.origin_endpoint_connection_info
        self.assertRaises(
            exception.SameDestination,
            self.server._check_endpoints,
            mock.sentinel.context,
            origin_endpoint,
            destination_endpoint
        )

    @mock.patch.object(server.ConductorServerEndpoint, 'get_transfer')
    @mock.patch.object(db_api, 'add_transfer')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_create_reservation_for_transfer')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_check_minion_pools_for_action')
    @mock.patch.object(models, 'Transfer')
    @mock.patch.object(server.ConductorServerEndpoint, '_check_endpoints')
    @mock.patch.object(server.ConductorServerEndpoint, 'get_endpoint')
    def test_create_instances_transfer(
        self,
        mock_get_endpoint,
        mock_check_endpoints,
        mock_transfer,
        mock_check_minion_pools_for_action,
        mock_create_reservation_for_transfer,
        mock_add_transfer,
        mock_get_transfer
    ):
        mock_get_endpoint.side_effect = mock.sentinel.origin_endpoint_id, \
            mock.sentinel.destination_endpoint_id
        mock_transfer.return_value = mock.Mock()
        result = self.server.create_instances_transfer(
            mock.sentinel.context,
            constants.TRANSFER_SCENARIO_REPLICA,
            mock.sentinel.origin_endpoint_id,
            mock.sentinel.destination_endpoint_id,
            mock.sentinel.origin_minion_pool_id,
            mock.sentinel.destination_minion_pool_id,
            mock.sentinel.instance_osmorphing_minion_pool_mappings,
            mock.sentinel.source_environment,
            mock.sentinel.destination_environment,
            [mock.sentinel.instance_1, mock.sentinel.instance_2],
            mock.sentinel.network_map,
            mock.sentinel.storage_mappings,
            notes=None,
            user_scripts=None
        )
        self.assertEqual(
            mock_get_transfer.return_value,
            result
        )
        mock_get_endpoint.assert_has_calls([
            mock.call(mock.sentinel.context, mock.sentinel.origin_endpoint_id),
            mock.call(mock.sentinel.context,
                      mock.sentinel.destination_endpoint_id)])
        mock_check_endpoints.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.origin_endpoint_id,
            mock.sentinel.destination_endpoint_id
        )
        self.assertEqual(
            (
                mock_transfer.return_value.origin_endpoint_id,
                mock_transfer.return_value.destination_endpoint_id,
                mock_transfer.return_value.destination_endpoint_id,
                mock_transfer.return_value.origin_minion_pool_id,
                mock_transfer.return_value.destination_minion_pool_id,
                (mock_transfer.return_value.
                 instance_osmorphing_minion_pool_mappings),
                mock_transfer.return_value.source_environment,
                mock_transfer.return_value.destination_environment,
                mock_transfer.return_value.info,
                mock_transfer.return_value.notes,
                mock_transfer.return_value.user_scripts),
            (
                mock.sentinel.origin_endpoint_id,
                mock.sentinel.destination_endpoint_id,
                mock.sentinel.destination_endpoint_id,
                mock.sentinel.origin_minion_pool_id,
                mock.sentinel.destination_minion_pool_id,
                (mock.sentinel.
                    instance_osmorphing_minion_pool_mappings),
                mock.sentinel.source_environment,
                mock.sentinel.destination_environment,
                {mock.sentinel.instance_1: {'volumes_info': []},
                 mock.sentinel.instance_2: {'volumes_info': []}},
                None,
                {})
        )
        mock_check_minion_pools_for_action.assert_called_once_with(
            mock.sentinel.context, mock_transfer.return_value)
        mock_create_reservation_for_transfer.assert_called_once_with(
            mock_transfer.return_value)
        mock_add_transfer.assert_called_once_with(
            mock.sentinel.context, mock_transfer.return_value)
        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context, mock_transfer.return_value.id)

    @mock.patch.object(db_api, 'get_transfer')
    def test__get_transfer(self, mock_get_transfer):
        result = self.server._get_transfer(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            include_task_info=False,
            to_dict=False
        )
        self.assertEqual(
            mock_get_transfer.return_value,
            result
        )
        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            include_task_info=False,
            to_dict=False
        )

    @mock.patch.object(db_api, 'get_transfer')
    def test__get_transfer_not_found(self, mock_get_transfer):
        mock_get_transfer.return_value = None
        self.assertRaises(
            exception.NotFound,
            self.server._get_transfer,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            include_task_info=False,
            to_dict=False
        )
        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            include_task_info=False,
            to_dict=False
        )

    def test_normalize_user_scripts(self):
        user_scripts = {
            'instances': {
                "mock_instance_1": "mock_value_1",
                "mock_instance_2": "mock_value_2"
            }
        }
        instances = ["mock_instance_2", "mock_instance_3"]

        expected_result = {
            'instances': {
                "mock_instance_2": "mock_value_2"
            }
        }

        with self.assertLogs('coriolis.conductor.rpc.server', level='WARNING'):
            result = self.server._normalize_user_scripts(user_scripts,
                                                         instances)

        self.assertEqual(expected_result, result)

    def test_normalize_user_scripts_none(self):
        result = self.server._normalize_user_scripts(None, None)
        self.assertEqual({}, result)

    @mock.patch.object(server.ConductorServerEndpoint, '_get_deployment')
    def test_get_deployment(self, mock_get_deployment):
        result = testutils.get_wrapped_function(self.server.get_deployment)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.deployment_id,
            include_task_info=False
        )
        self.assertEqual(
            mock_get_deployment.return_value,
            result
        )
        mock_get_deployment.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.deployment_id,
            include_task_info=False,
            to_dict=True
        )

    @mock.patch.object(db_api, 'get_transfer_deployments')
    def test_check_running_transfer_deployments(
        self,
        mock_get_transfer_deployments
    ):
        deployment_1 = mock.Mock()
        deployment_2 = mock.Mock()
        deployment_1.executions = [mock.Mock()]
        deployment_1.executions[0].status = \
            constants.EXECUTION_STATUS_COMPLETED
        deployment_2.executions = [mock.Mock()]
        deployment_2.executions[0].status = \
            constants.EXECUTION_STATUS_ERROR
        deployments = [deployment_1, deployment_2]
        mock_get_transfer_deployments.return_value = deployments
        self.server._check_running_transfer_deployments(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
        )
        mock_get_transfer_deployments.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
        )

    @mock.patch.object(db_api, 'get_transfer_deployments')
    def test_check_running_transfer_deployments_invalid_transfer_state(
        self,
        mock_get_transfer_deployments
    ):
        deployment_1 = mock.Mock()
        deployment_2 = mock.Mock()
        deployment_1.executions = [mock.Mock()]
        deployment_1.executions[0].status = constants.EXECUTION_STATUS_RUNNING
        deployment_2.executions = [mock.Mock()]
        deployment_2.executions[0].status = \
            constants.EXECUTION_STATUS_COMPLETED
        deployments = [deployment_1, deployment_2]
        mock_get_transfer_deployments.return_value = deployments
        self.assertRaises(
            exception.InvalidTransferState,
            self.server._check_running_transfer_deployments,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
        )
        mock_get_transfer_deployments.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
        )

    def test_check_running_executions(self):
        execution_1 = mock.Mock()
        execution_2 = mock.Mock()
        action = mock.Mock()
        execution_1.status = constants.EXECUTION_STATUS_COMPLETED
        execution_2.status = constants.EXECUTION_STATUS_COMPLETED
        action.executions = [execution_1, execution_2]
        self.server._check_running_executions(action)

    def test_check_running_executions_invalid_state(self):
        execution_1 = mock.Mock()
        execution_2 = mock.Mock()
        action = mock.Mock()
        execution_1.status = constants.EXECUTION_STATUS_COMPLETED
        execution_2.status = constants.EXECUTION_STATUS_RUNNING
        action.executions = [execution_1, execution_2]
        self.assertRaises(
            exception.InvalidActionTasksExecutionState,
            self.server._check_running_executions,
            action
        )

    @mock.patch.object(server.ConductorServerEndpoint,
                       '_check_running_transfer_deployments')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_check_running_executions')
    def test_check_transfer_running_executions(
        self,
        mock_check_running_executions,
        mock_check_running_transfer_deployments
    ):
        transfer = mock.Mock()
        self.server._check_transfer_running_executions(
            mock.sentinel.context,
            transfer
        )

        mock_check_running_executions.assert_called_once_with(transfer)
        mock_check_running_transfer_deployments.assert_called_once_with(
            mock.sentinel.context,
            transfer.id
        )

    def test_check_valid_transfer_tasks_execution(self):
        execution1 = mock.Mock(
            number=1,
            type=constants.EXECUTION_TYPE_TRANSFER_EXECUTION,
            status=constants.EXECUTION_STATUS_COMPLETED,
        )
        execution2 = mock.Mock(
            number=2,
            type=constants.EXECUTION_TYPE_TRANSFER_EXECUTION,
            status=constants.EXECUTION_STATUS_COMPLETED,
        )
        mock_transfer = mock.Mock(
            executions=[execution1, execution2]
        )
        self.server._check_valid_transfer_tasks_execution(
            mock_transfer
        )

        # raises exception if all executions are incomplete
        execution1.status = constants.EXECUTION_STATUS_UNEXECUTED
        execution2.status = constants.EXECUTION_STATUS_UNEXECUTED

        self.assertRaises(
            exception.InvalidTransferState,
            self.server._check_valid_transfer_tasks_execution,
            mock_transfer
        )

        # doesn't raise exception if all executions are incomplete
        # and is forced
        self.server._check_valid_transfer_tasks_execution(
            mock_transfer,
            True
        )

        # doesn't raise exception if only one execution is completed
        execution1.status = constants.EXECUTION_STATUS_COMPLETED
        execution2.status = constants.EXECUTION_STATUS_UNEXECUTED

        self.server._check_valid_transfer_tasks_execution(
            mock_transfer
        )

        mock_transfer.executions = []

        self.assertRaises(
            exception.InvalidTransferState,
            self.server._check_valid_transfer_tasks_execution,
            mock_transfer
        )

    @mock.patch.object(server.ConductorServerEndpoint,
                       "get_available_providers")
    def test_get_provider_types(self, mock_get_available_providers):
        endpoint = mock.Mock()
        result = self.server._get_provider_types(
            mock.sentinel.context,
            endpoint
        )

        self.assertEqual(
            (mock_get_available_providers.return_value.
                get(endpoint.type)["types"]),
            result
        )

        mock_get_available_providers.assert_called_once_with(
            mock.sentinel.context)

        mock_get_available_providers.reset_mock()
        endpoint.type = "mock_type"
        mock_get_available_providers.return_value = {"mock_type": None}
        self.assertRaises(
            exception.NotFound,
            self.server._get_provider_types,
            mock.sentinel.context,
            endpoint
        )

        mock_get_available_providers.assert_called_once_with(
            mock.sentinel.context)

    @mock.patch.object(server.ConductorServerEndpoint,
                       '_normalize_user_scripts')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_validate_deployment_inputs')
    @mock.patch.object(models, 'Deployment')
    @mock.patch.object(db_api, 'add_deployment')
    @mock.patch.object(server.ConductorServerEndpoint, '_get_transfer')
    @mock.patch.object(server.ConductorServerEndpoint, '_execute_deployment')
    @mock.patch.object(server.ConductorServerEndpoint, 'get_deployment')
    def test_deploy_transfer_instances(
            self, mock_get_deployment, mock_execute_deployment,
            mock_get_transfer, mock_add_deployment, mock_deployment_model,
            mock_validate_deployment_inputs, mock_normalize_user_scripts):

        mock_normalize_user_scripts.return_value = {'instances': {}}

        transfer_mock = mock.MagicMock()
        transfer_mock.instance_osmorphing_minion_pool_mappings = {
            mock.sentinel.instance1: mock.sentinel.pool1}
        transfer_mock.user_scripts = {}
        transfer_mock.clone_disks = False
        transfer_mock.skip_os_morphing = True
        transfer_mock.instances = [mock.sentinel.instance1]
        mock_get_transfer.return_value = transfer_mock
        osm_pool_mappings = {mock.sentinel.instance1: mock.sentinel.pool2}
        deployment = mock_deployment_model.return_value
        force = False

        result = testutils.get_wrapped_function(
            self.server.deploy_transfer_instances)(
                self.server,
                mock.sentinel.ctxt,
                mock.sentinel.transfer_id,
                force=force,
                clone_disks=True,
                instance_osmorphing_minion_pool_mappings=osm_pool_mappings,
                skip_os_morphing=False,
                user_scripts=mock.sentinel.user_scripts,
                wait_for_execution=None,
                trust_id=None)

        self.assertEqual(mock_get_deployment.return_value, result)
        self.assertEqual({'instances': {}}, deployment.user_scripts)
        self.assertEqual(True, deployment.clone_disks)
        self.assertEqual(False, deployment.skip_os_morphing)
        self.assertEqual(
            mock.sentinel.pool2,
            deployment.instance_osmorphing_minion_pool_mappings[
                mock.sentinel.instance1])
        self.assertTrue(
            transfer_mock.destination_environment is not
            deployment.destination_environment)
        mock_normalize_user_scripts.assert_called_once_with(
            mock.sentinel.user_scripts, transfer_mock.instances)
        mock_get_transfer.assert_called_once_with(
            mock.sentinel.ctxt, mock.sentinel.transfer_id,
            include_task_info=True)
        mock_add_deployment.assert_called_once_with(
            mock.sentinel.ctxt, deployment)
        mock_execute_deployment.assert_called_once_with(
            mock.sentinel.ctxt, deployment, False)
        mock_get_deployment.assert_called_once_with(
            mock.sentinel.ctxt, deployment.id)
        mock_validate_deployment_inputs.assert_called_once_with(
            mock.sentinel.ctxt, deployment, transfer_mock, force)

    @mock.patch.object(models, 'Deployment')
    @mock.patch.object(db_api, 'add_deployment')
    @mock.patch.object(server.ConductorServerEndpoint, '_get_transfer')
    @mock.patch.object(server.ConductorServerEndpoint, '_execute_deployment')
    @mock.patch.object(server.ConductorServerEndpoint, 'get_deployment')
    def test_deploy_transfer_instances_wait_for_execution(
            self, mock_get_deployment, mock_execute_deployment,
            mock_get_transfer, mock_add_deployment, mock_deployment_model):
        transfer_mock = mock.MagicMock()
        transfer_mock.instance_osmorphing_minion_pool_mappings = {
            mock.sentinel.instance1: mock.sentinel.pool1}
        transfer_mock.user_scripts = {}
        transfer_mock.clone_disks = False
        transfer_mock.skip_os_morphing = True
        transfer_mock.instances = [mock.sentinel.instance1]
        mock_get_transfer.return_value = transfer_mock
        deployment = mock_deployment_model.return_value

        result = testutils.get_wrapped_function(
            self.server.deploy_transfer_instances)(
                self.server,
                mock.sentinel.ctxt,
                mock.sentinel.transfer_id,
                force=False,
                clone_disks=None,
                instance_osmorphing_minion_pool_mappings=None,
                skip_os_morphing=None,
                user_scripts=None,
                wait_for_execution=mock.sentinel.wait_for_execution,
                trust_id=mock.sentinel.trust_id)

        self.assertEqual(mock_get_deployment.return_value, result)
        self.assertEqual(transfer_mock.user_scripts, deployment.user_scripts)
        self.assertEqual(transfer_mock.clone_disks, deployment.clone_disks)
        self.assertEqual(
            transfer_mock.skip_os_morphing, deployment.skip_os_morphing)
        self.assertEqual(
            transfer_mock.instance_osmorphing_minion_pool_mappings,
            deployment.instance_osmorphing_minion_pool_mappings)
        self.assertTrue(
            transfer_mock.destination_environment is not
            deployment.destination_environment)
        mock_get_transfer.assert_called_once_with(
            mock.sentinel.ctxt, mock.sentinel.transfer_id,
            include_task_info=True)
        mock_add_deployment.assert_called_once_with(
            mock.sentinel.ctxt, deployment)
        mock_execute_deployment.assert_not_called()
        mock_get_deployment.assert_called_once_with(
            mock.sentinel.ctxt, deployment.id)

    def test_get_instance_scripts(
        self
    ):
        user_scripts = {
            "global": 'mock_user_scripts',
            "instances": {
                mock.sentinel.instance_1: mock.sentinel.scripts_1,
                mock.sentinel.instance_2: mock.sentinel.scripts_2,
            }
        }
        expected_result = {
            'global': 'mock_user_scripts',
            'instances': {mock.sentinel.instance_1: mock.sentinel.scripts_1}
        }
        result = self.server._get_instance_scripts(
            user_scripts, mock.sentinel.instance_1
        )
        self.assertEqual(
            expected_result,
            result
        )

    def test_get_instance_scripts_no_instance_script(
        self
    ):
        user_scripts = {
            "global": 'mock_user_scripts',
            "instances": {
                mock.sentinel.instance_1: None,
                mock.sentinel.instance_2: mock.sentinel.scripts_2,
            }
        }
        expected_result = {
            'global': 'mock_user_scripts',
            'instances': {}
        }
        result = self.server._get_instance_scripts(
            user_scripts, mock.sentinel.instance_1
        )
        self.assertEqual(
            expected_result,
            result
        )

    def test_get_instance_scripts_no_user_scripts(
        self
    ):
        user_scripts = None
        expected_result = {
            'global': {},
            'instances': {}
        }
        result = self.server._get_instance_scripts(
            user_scripts, mock.sentinel.instance_1
        )
        self.assertEqual(
            expected_result,
            result
        )

    @mock.patch.object(server.ConductorServerEndpoint,
                       "_minion_manager_client")
    def test_deallocate_minion_machines_for_action(
        self,
        mock_minion_manager_client
    ):
        action = mock.Mock()
        result = self.server._deallocate_minion_machines_for_action(
            mock.sentinel.context,
            action
        )

        self.assertEqual(
            (mock_minion_manager_client.deallocate_minion_machines_for_action.
                return_value),
            result
        )
        (mock_minion_manager_client.deallocate_minion_machines_for_action.
            assert_called_once_with(
                mock.sentinel.context,
                action.base_id
            ))

    @mock.patch.object(server.ConductorServerEndpoint,
                       "_minion_manager_client")
    def test_check_minion_pools_for_action(
        self,
        mock_minion_manager_client
    ):
        action = mock.Mock()
        self.server._check_minion_pools_for_action(
            mock.sentinel.context,
            action
        )

        (mock_minion_manager_client.
            validate_minion_pool_selections_for_action.assert_called_once_with(
                mock.sentinel.context,
                action
            ))

    @mock.patch.object(db_api, "update_transfer_action_info_for_instance")
    def test_update_task_info_for_minion_allocations(
        self,
        mock_update_transfer_action_info_for_instance
    ):
        action = mock.Mock()
        action.id = mock.sentinel.action_id
        action.instances = [mock.sentinel.instance1, mock.sentinel.instance2]
        action.info = {
            mock.sentinel.instance1: {},
            mock.sentinel.instance2: {
                'origin_minion_machine_id':
                mock.sentinel.origin_minion_id,
                'origin_minion_provider_properties':
                mock.sentinel.origin_minion_provider_properties,
                'origin_minion_connection_info':
                mock.sentinel.origin_minion_connection_info,
                'destination_minion_machine_id':
                mock.sentinel.destination_minion_id,
                'destination_minion_provider_properties':
                mock.sentinel.destination_minion_provider_properties,
                'destination_minion_connection_info':
                mock.sentinel.destination_minion_connection_info,
                'destination_minion_backup_writer_connection_info':
                mock.sentinel.destination_minion_backup_writer_connection_info,
                'osmorphing_minion_machine_id':
                mock.sentinel.osmorphing_minion_id,
                'osmorphing_minion_provider_properties':
                mock.sentinel.osmorphing_minion_provider_properties,
                'osmorphing_minion_connection_info':
                mock.sentinel.osmorphing_minion_connection_info,
            },
            mock.sentinel.instance3: {}
        }
        minion_machine_allocations = {
            mock.sentinel.instance1: {
                'origin_minion': {
                    'id': mock.sentinel.updated_origin_minion_id,
                    "provider_properties":
                    mock.sentinel.updated_origin_minion_provider_properties,
                    "connection_info":
                    mock.sentinel.updated_origin_minion_connection_info,
                },
                'destination_minion': {
                    'id': mock.sentinel.updated_destination_minion_id,
                    "provider_properties":
                    (mock.sentinel.
                     updated_destination_minion_provider_properties),
                    "connection_info":
                    mock.sentinel.updated_destination_minion_connection_info,
                    "backup_writer_connection_info":
                    (mock.sentinel.
                     updated_destination_minion_backup_writer_connection_info)
                },
                'osmorphing_minion': {
                    'id': mock.sentinel.updated_osmorphing_minion_id,
                    "provider_properties":
                    (mock.sentinel.
                     updated_osmorphing_minion_provider_properties),
                    "connection_info":
                    mock.sentinel.updated_osmorphing_minion_connection_info,
                }
            },
        }

        expected_action_info = {
            mock.sentinel.instance1: {
                'origin_minion_machine_id':
                mock.sentinel.updated_origin_minion_id,
                'origin_minion_provider_properties':
                mock.sentinel.updated_origin_minion_provider_properties,
                'origin_minion_connection_info':
                mock.sentinel.updated_origin_minion_connection_info,
                'destination_minion_machine_id':
                mock.sentinel.updated_destination_minion_id,
                'destination_minion_provider_properties':
                mock.sentinel.updated_destination_minion_provider_properties,
                'destination_minion_connection_info':
                mock.sentinel.updated_destination_minion_connection_info,
                'destination_minion_backup_writer_connection_info':
                (mock.sentinel.
                 updated_destination_minion_backup_writer_connection_info),
                'osmorphing_minion_machine_id':
                mock.sentinel.updated_osmorphing_minion_id,
                'osmorphing_minion_provider_properties':
                mock.sentinel.updated_osmorphing_minion_provider_properties,
                'osmorphing_minion_connection_info':
                mock.sentinel.updated_osmorphing_minion_connection_info,
            },
        }
        self.server._update_task_info_for_minion_allocations(
            mock.sentinel.context,
            action,
            minion_machine_allocations
        )
        mock_update_transfer_action_info_for_instance.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.action_id,
            mock.sentinel.instance1,
            expected_action_info[mock.sentinel.instance1]
        )
        minion_machine_allocations = {
            mock.sentinel.instance2: {},
            mock.sentinel.instance4: {},
        }
        expected_action_info = {
            mock.sentinel.instance2: {
                'origin_minion_machine_id':
                mock.sentinel.origin_minion_id,
                'origin_minion_provider_properties':
                mock.sentinel.origin_minion_provider_properties,
                'origin_minion_connection_info':
                mock.sentinel.origin_minion_connection_info,
                'destination_minion_machine_id':
                mock.sentinel.destination_minion_id,
                'destination_minion_provider_properties':
                mock.sentinel.destination_minion_provider_properties,
                'destination_minion_connection_info':
                mock.sentinel.destination_minion_connection_info,
                'destination_minion_backup_writer_connection_info':
                mock.sentinel.destination_minion_backup_writer_connection_info,
                'osmorphing_minion_machine_id':
                mock.sentinel.osmorphing_minion_id,
                'osmorphing_minion_provider_properties':
                mock.sentinel.osmorphing_minion_provider_properties,
                'osmorphing_minion_connection_info':
                mock.sentinel.osmorphing_minion_connection_info,
            },
        }
        mock_update_transfer_action_info_for_instance.reset_mock()
        self.server._update_task_info_for_minion_allocations(
            mock.sentinel.context,
            action,
            minion_machine_allocations
        )
        mock_update_transfer_action_info_for_instance.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.action_id,
            mock.sentinel.instance2,
            expected_action_info[mock.sentinel.instance2]
        )

    @mock.patch.object(server.ConductorServerEndpoint, '_get_transfer')
    def test_get_last_execution_for_transfer(
        self,
        mock_get_transfer
    ):
        transfer = mock.Mock()
        transfer.id = mock.sentinel.id
        execution1 = mock.Mock(id=mock.sentinel.execution_id1, number=1)
        execution2 = mock.Mock(id=mock.sentinel.execution_id2, number=3)
        execution3 = mock.Mock(id=mock.sentinel.execution_id3, number=2)
        transfer.executions = [execution1, execution2, execution3]
        mock_get_transfer.return_value = transfer
        result = self.server._get_last_execution_for_transfer(
            mock.sentinel.context,
            transfer,
            requery=False
        )
        self.assertEqual(
            execution2,
            result
        )
        mock_get_transfer.assert_not_called()
        transfer.executions = None
        self.assertRaises(
            exception.InvalidTransferState,
            self.server._get_last_execution_for_transfer,
            mock.sentinel.context,
            transfer,
            requery=True
        )
        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.id)

    @mock.patch.object(server.ConductorServerEndpoint, '_get_deployment')
    def test_get_execution_for_deployment(
        self,
        mock_get_deployment
    ):
        deployment = mock.Mock()
        deployment.id = mock.sentinel.id
        execution1 = mock.Mock(id=mock.sentinel.execution_id1)
        execution2 = mock.Mock(id=mock.sentinel.execution_id2)
        deployment.executions = [execution1]
        mock_get_deployment.return_value = deployment
        result = self.server._get_execution_for_deployment(
            mock.sentinel.context,
            deployment,
            requery=False
        )
        self.assertEqual(
            execution1,
            result
        )
        mock_get_deployment.assert_not_called()
        deployment.executions = [execution1, execution2]
        self.assertRaises(
            exception.InvalidDeploymentState,
            self.server._get_execution_for_deployment,
            mock.sentinel.context,
            deployment,
            requery=True
        )
        mock_get_deployment.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.id)
        deployment.executions = []
        self.assertRaises(
            exception.InvalidDeploymentState,
            self.server._get_execution_for_deployment,
            mock.sentinel.context,
            deployment,
            requery=False
        )

    @mock.patch.object(server.ConductorServerEndpoint, '_begin_tasks')
    @mock.patch.object(db_api, 'get_transfer_tasks_execution')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_update_task_info_for_minion_allocations')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_get_last_execution_for_transfer')
    @mock.patch.object(server.ConductorServerEndpoint, '_get_transfer')
    def test_confirm_transfer_minions_allocation(
        self,
        mock_get_transfer,
        mock_get_last_execution_for_transfer,
        mock_update_task_info_for_minion_allocations,
        mock_get_transfer_tasks_execution,
        mock_begin_tasks
    ):
        mock_get_transfer.return_value.last_execution_status = \
            constants.EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS

        testutils.get_wrapped_function(
            self.server.confirm_transfer_minions_allocation)(
                self.server,
                mock.sentinel.context,
                mock.sentinel.transfer_id,
                mock.sentinel.minion_machine_allocations
        )

        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            include_task_info=True
        )
        mock_get_last_execution_for_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock_get_transfer.return_value,
            requery=False
        )
        mock_update_task_info_for_minion_allocations.assert_called_once_with(
            mock.sentinel.context,
            mock_get_transfer.return_value,
            mock.sentinel.minion_machine_allocations
        )
        mock_get_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock_get_transfer.return_value.id,
            mock_get_last_execution_for_transfer.return_value.id
        )
        mock_begin_tasks.assert_called_once_with(
            mock.sentinel.context,
            mock_get_transfer.return_value,
            mock_get_transfer_tasks_execution.return_value
        )

    @mock.patch.object(server.ConductorServerEndpoint, '_begin_tasks')
    @mock.patch.object(db_api, 'get_transfer_tasks_execution')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_update_task_info_for_minion_allocations')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_get_last_execution_for_transfer')
    @mock.patch.object(server.ConductorServerEndpoint, '_get_transfer')
    def test_confirm_transfer_minions_allocation_unexpected_status(
        self,
        mock_get_transfer,
        mock_get_last_execution_for_transfer,
        mock_update_task_info_for_minion_allocations,
        mock_get_transfer_tasks_execution,
        mock_begin_tasks
    ):
        mock_get_transfer.return_value.last_execution_status = \
            constants.EXECUTION_STATUS_CANCELED

        self.assertRaises(
            exception.InvalidTransferState,
            testutils.get_wrapped_function(
                self.server.confirm_transfer_minions_allocation),
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.minion_machine_allocations
        )

        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            include_task_info=True
        )
        mock_get_last_execution_for_transfer.assert_not_called()
        mock_update_task_info_for_minion_allocations.assert_not_called()
        mock_get_transfer_tasks_execution.assert_not_called()
        mock_begin_tasks.assert_not_called()

    @mock.patch.object(server.ConductorServerEndpoint,
                       '_set_tasks_execution_status')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_cancel_tasks_execution')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_get_last_execution_for_transfer')
    @mock.patch.object(server.ConductorServerEndpoint, '_get_transfer')
    def test_report_transfer_minions_allocation_error(
        self,
        mock_get_transfer,
        mock_get_last_execution_for_transfer,
        mock_cancel_tasks_execution,
        mock_set_tasks_execution_status
    ):
        mock_get_transfer.return_value.last_execution_status = \
            constants.EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS

        testutils.get_wrapped_function(
            self.server.report_transfer_minions_allocation_error)(
                self.server,
                mock.sentinel.context,
                mock.sentinel.transfer_id,
                mock.sentinel.minion_allocation_error_details
        )

        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id
        )
        mock_get_last_execution_for_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock_get_transfer.return_value,
            requery=False
        )
        mock_cancel_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock_get_last_execution_for_transfer.return_value,
            requery=True
        )
        mock_set_tasks_execution_status.assert_called_once_with(
            mock.sentinel.context,
            mock_get_last_execution_for_transfer.return_value,
            constants.EXECUTION_STATUS_ERROR_ALLOCATING_MINIONS
        )

    @mock.patch.object(server.ConductorServerEndpoint,
                       '_set_tasks_execution_status')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_cancel_tasks_execution')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_get_last_execution_for_transfer')
    @mock.patch.object(server.ConductorServerEndpoint, '_get_transfer')
    def test_report_transfer_minions_allocation_error_unexpected_status(
        self,
        mock_get_transfer,
        mock_get_last_execution_for_transfer,
        mock_cancel_tasks_execution,
        mock_set_tasks_execution_status
    ):
        mock_get_transfer.return_value.last_execution_status = \
            constants.EXECUTION_STATUS_CANCELED

        self.assertRaises(
            exception.InvalidTransferState,
            testutils.get_wrapped_function(
                self.server.report_transfer_minions_allocation_error),
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.minion_allocation_error_details
        )

        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id
        )
        mock_get_last_execution_for_transfer.assert_not_called()
        mock_cancel_tasks_execution.assert_not_called()
        mock_set_tasks_execution_status.assert_not_called()

    @mock.patch.object(server.ConductorServerEndpoint, '_begin_tasks')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_update_task_info_for_minion_allocations')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_get_execution_for_deployment')
    @mock.patch.object(server.ConductorServerEndpoint, '_get_deployment')
    def test_confirm_deployment_minions_allocation(
        self,
        mock_get_deployment,
        mock_get_execution_for_deployment,
        mock_update_task_info_for_minion_allocations,
        mock_begin_tasks
    ):
        mock_get_deployment.return_value.last_execution_status = \
            constants.EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS

        testutils.get_wrapped_function(
            self.server.confirm_deployment_minions_allocation)(
                self.server,
                mock.sentinel.context,
                mock.sentinel.transfer_id,
                mock.sentinel.minion_machine_allocations
        )

        mock_get_deployment.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            include_task_info=True
        )
        mock_get_execution_for_deployment.assert_called_once_with(
            mock.sentinel.context,
            mock_get_deployment.return_value,
            requery=False
        )
        mock_update_task_info_for_minion_allocations.assert_called_once_with(
            mock.sentinel.context,
            mock_get_deployment.return_value,
            mock.sentinel.minion_machine_allocations
        )
        mock_begin_tasks.assert_called_once_with(
            mock.sentinel.context,
            mock_get_deployment.return_value,
            mock_get_execution_for_deployment.return_value
        )

    @mock.patch.object(server.ConductorServerEndpoint, '_begin_tasks')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_update_task_info_for_minion_allocations')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_get_execution_for_deployment')
    @mock.patch.object(server.ConductorServerEndpoint, '_get_deployment')
    def test_confirm_deployment_minions_allocation_unexpected_status(
        self,
        mock_get_deployment,
        mock_get_execution_for_deployment,
        mock_update_task_info_for_minion_allocations,
        mock_begin_tasks
    ):
        mock_get_deployment.return_value.last_execution_status = \
            constants.EXECUTION_STATUS_CANCELED

        self.assertRaises(
            exception.InvalidDeploymentState,
            testutils.get_wrapped_function(
                self.server.confirm_deployment_minions_allocation),
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.minion_machine_allocations
        )

        mock_get_deployment.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            include_task_info=True
        )
        mock_get_execution_for_deployment.assert_not_called()
        mock_update_task_info_for_minion_allocations.assert_not_called()

        mock_begin_tasks.assert_not_called()

    @mock.patch.object(server.ConductorServerEndpoint,
                       '_set_tasks_execution_status')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_cancel_tasks_execution')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_get_execution_for_deployment')
    @mock.patch.object(server.ConductorServerEndpoint, '_get_deployment')
    def test_report_deployment_minions_allocation_error(
        self,
        mock_get_deployment,
        mock_get_execution_for_deployment,
        mock_cancel_tasks_execution,
        mock_set_tasks_execution_status
    ):
        mock_get_deployment.return_value.last_execution_status = \
            constants.EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS

        testutils.get_wrapped_function(
            self.server.report_deployment_minions_allocation_error)(
                self.server,
                mock.sentinel.context,
                mock.sentinel.transfer_id,
                mock.sentinel.minion_allocation_error_details
        )

        mock_get_deployment.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id
        )
        mock_get_execution_for_deployment.assert_called_once_with(
            mock.sentinel.context,
            mock_get_deployment.return_value,
            requery=False
        )
        mock_cancel_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock_get_execution_for_deployment.return_value,
            requery=True
        )
        mock_set_tasks_execution_status.assert_called_once_with(
            mock.sentinel.context,
            mock_get_execution_for_deployment.return_value,
            constants.EXECUTION_STATUS_ERROR_ALLOCATING_MINIONS
        )

    @mock.patch.object(server.ConductorServerEndpoint,
                       '_set_tasks_execution_status')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_cancel_tasks_execution')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_get_execution_for_deployment')
    @mock.patch.object(server.ConductorServerEndpoint, '_get_deployment')
    def test_report_deployment_minions_allocation_error_unexpected_status(
        self,
        mock_get_deployment,
        mock_get_execution_for_deployment,
        mock_cancel_tasks_execution,
        mock_set_tasks_execution_status
    ):
        mock_get_deployment.return_value.last_execution_status = \
            constants.EXECUTION_STATUS_CANCELED

        self.assertRaises(
            exception.InvalidDeploymentState,
            testutils.get_wrapped_function(
                self.server.report_deployment_minions_allocation_error),
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.minion_allocation_error_details
        )

        mock_get_deployment.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id
        )
        mock_get_execution_for_deployment.assert_not_called()
        mock_cancel_tasks_execution.assert_not_called()
        mock_set_tasks_execution_status.assert_not_called()

    @mock.patch.object(db_api, 'get_tasks_execution')
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_set_tasks_execution_status',
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_advance_execution_state',
    )
    @mock.patch.object(db_api, 'set_task_status')
    @mock.patch.object(
        rpc_worker_client,
        'WorkerClient',
    )
    def test_cancel_tasks_execution_no_config(
            self,
            mock_worker_client,
            mock_set_task_status,
            mock_advance_execution_state,
            mock_set_tasks_execution_status,
            mock_get_tasks_execution
    ):
        execution = mock.Mock(
            id=mock.sentinel.execution_id,
            tasks=[],
        )

        def call_cancel_tasks_execution(
                requery=False,
                force=False,
        ):
            self.server._cancel_tasks_execution(
                mock.sentinel.context,
                execution,
                requery=requery,
                force=force,
            )

        call_cancel_tasks_execution(requery=True)
        mock_get_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            execution.id,
        )

        mock_get_tasks_execution.reset_mock()
        execution.status = constants.EXECUTION_STATUS_RUNNING

        call_cancel_tasks_execution()
        # no requery
        mock_get_tasks_execution.assert_not_called()
        mock_set_tasks_execution_status.assert_called_once_with(
            mock.sentinel.context,
            execution,
            constants.EXECUTION_STATUS_CANCELLING,
        )

        mock_advance_execution_state.reset_mock()
        execution.status = constants.EXECUTION_STATUS_CANCELLING
        call_cancel_tasks_execution()
        mock_advance_execution_state.assert_called_once_with(
            mock.sentinel.context,
            execution,
            requery=True,
        )

        # execution is in finalized state
        mock_advance_execution_state.reset_mock()
        execution.status = constants.TASK_STATUS_COMPLETED
        call_cancel_tasks_execution()
        mock_advance_execution_state.assert_not_called()

        # for a RUNNING task with no on_error and not forced
        # worker_rpc.cancel_task should be called
        mock_set_task_status.reset_mock()
        mock_worker_client.return_value.cancel_task.reset_mock()
        execution.status = constants.EXECUTION_STATUS_RUNNING
        execution.tasks = [
            mock.Mock(
                index=1,
                status=constants.TASK_STATUS_RUNNING,
                depends_on=None,
                on_error=False,
            )
        ]
        call_cancel_tasks_execution()
        mock_worker_client.return_value.cancel_task\
            .assert_called_once_with(
                mock.sentinel.context,
                execution.tasks[0].id,
                execution.tasks[0].process_id,
                False
            )

        # if worker_rpc.cancel_task fails
        # _set_task_status should be called with FAILED_TO_CANCEL
        mock_set_task_status.reset_mock()
        mock_worker_client.return_value.cancel_task\
            .side_effect = Exception()
        call_cancel_tasks_execution()
        mock_set_task_status.assert_any_call(
            mock.sentinel.context,
            execution.tasks[0].id,
            constants.TASK_STATUS_FAILED_TO_CANCEL,
            exception_details=mock.ANY,
        )

    @mock.patch.object(db_api, 'get_deployment')
    def test__get_deployment(
        self,
        mock_get_deployment
    ):
        result = self.server._get_deployment(
            mock.sentinel.context,
            mock.sentinel.deployment_id,
            include_task_info=False,
            to_dict=False
        )
        self.assertEqual(
            mock_get_deployment.return_value,
            result
        )

        mock_get_deployment.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.deployment_id,
            include_task_info=False,
            to_dict=False
        )
        mock_get_deployment.reset_mock()
        mock_get_deployment.return_value = None

        self.assertRaises(
            exception.NotFound,
            self.server._get_deployment,
            mock.sentinel.context,
            mock.sentinel.deployment_id,
            include_task_info=False,
            to_dict=False
        )

        mock_get_deployment.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.deployment_id,
            include_task_info=False,
            to_dict=False
        )

    @mock.patch.object(db_api, 'get_tasks_execution')
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_set_tasks_execution_status',
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_advance_execution_state',
    )
    @mock.patch.object(db_api, 'set_task_status')
    @mock.patch.object(
        rpc_worker_client,
        'WorkerClient',
    )
    @ddt.file_data("data/cancel_tasks_execution_config.yml")
    @ddt.unpack
    def test_cancel_tasks_execution(
            self,
            mock_worker_client,
            mock_set_task_status,
            mock_advance_execution_state,
            mock_set_tasks_execution_status,
            mock_get_tasks_execution,
            config,
            expected_status,
    ):
        force = config.get('force', False)
        tasks = config.get('tasks', [])
        on_error = config.get('on_error')
        hides_exception_details = config.get('hides_exception_details', False)
        depends_on = config.get('depends_on') and [
            mock.sentinel.depends_on
        ]
        execution = mock.Mock(
            id=mock.sentinel.execution_id,
            status=constants.EXECUTION_STATUS_RUNNING,
            tasks=[
                mock.Mock(
                    id=f"task-{t}",
                    index=i,
                    status=t,
                    depends_on=depends_on,
                    on_error=on_error,
                ) for i, t in enumerate(tasks)
            ]
        )

        self.server._cancel_tasks_execution(
            mock.sentinel.context,
            execution,
            requery=False,
            force=force,
        )

        if not expected_status:
            mock_set_task_status.assert_not_called()
            return

        for execution_task in execution.tasks:
            kwargs = {'exception_details': mock.ANY}
            if hides_exception_details:
                kwargs = {}
            mock_set_task_status.assert_has_calls([
                mock.call(
                    mock.sentinel.context,
                    execution_task.id,
                    expected_status,
                    **kwargs
                )
            ])

    @mock.patch.object(server.ConductorServerEndpoint,
                       '_deallocate_minion_machines_for_action')
    @mock.patch.object(db_api, 'get_action')
    @mock.patch.object(keystone, 'delete_trust')
    @mock.patch.object(db_api, 'set_execution_status')
    def test_set_tasks_execution_status(
        self,
        mock_set_execution_status,
        mock_delete_trust,
        mock_get_action,
        mock_deallocate_minion_machines_for_action
    ):
        context = mock.Mock()
        execution = mock.Mock()

        def call_set_tasks_execution_status(new_execution_status):
            self.server._set_tasks_execution_status(
                context,
                execution,
                new_execution_status
            )

        context.delete_trust_id = mock.sentinel.delete_trust_id
        execution.status = (constants.
                            EXECUTION_STATUS_AWAITING_MINION_ALLOCATIONS)
        call_set_tasks_execution_status(constants.EXECUTION_STATUS_COMPLETED)

        mock_set_execution_status.assert_called_once_with(
            context,
            execution.id,
            constants.EXECUTION_STATUS_COMPLETED
        )
        mock_delete_trust.assert_called_once_with(
            context
        )
        mock_get_action.assert_not_called()

        mock_set_execution_status.reset_mock()
        mock_delete_trust.reset_mock()
        context.delete_trust_id = None
        execution.status = constants.EXECUTION_STATUS_RUNNING
        call_set_tasks_execution_status(constants.EXECUTION_STATUS_CANCELED)

        mock_set_execution_status.assert_called_once_with(
            context,
            execution.id,
            constants.EXECUTION_STATUS_CANCELED
        )
        mock_delete_trust.assert_not_called()
        mock_get_action.assert_called_once_with(
            context, execution.action_id)
        mock_deallocate_minion_machines_for_action.assert_called_once_with(
            context, mock_get_action.return_value)

        mock_set_execution_status.reset_mock()
        mock_get_action.reset_mock()
        call_set_tasks_execution_status(constants.EXECUTION_STATUS_RUNNING)
        mock_set_execution_status.assert_called_once_with(
            context,
            execution.id,
            constants.EXECUTION_STATUS_RUNNING
        )
        mock_delete_trust.assert_not_called()
        mock_get_action.assert_not_called()

    @mock.patch.object(db_api, 'set_task_status')
    @mock.patch.object(db_api, 'set_task_host_properties')
    @mock.patch.object(db_api, 'get_task')
    def test_set_task_host(
            self,
            mock_get_task,
            mock_set_task_host_properties,
            mock_set_task_status,
    ):
        def call_set_task_host():
            testutils.get_wrapped_function(
                self.server.set_task_host
            )(
                self.server,
                mock.sentinel.context,
                mock.sentinel.task_id,
                mock.sentinel.host,  # type: ignore
            )

        # task status is not expected
        self.assertRaises(
            exception.InvalidTaskState,
            call_set_task_host,
        )

        mock_get_task.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
        )

        mock_get_task.return_value.status = constants.TASK_STATUS_CANCELLING
        self.assertRaises(
            exception.TaskIsCancelling,
            call_set_task_host
        )

        mock_get_task.return_value.status = constants\
            .TASK_STATUS_CANCELLING_AFTER_COMPLETION
        call_set_task_host()
        mock_set_task_host_properties.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
            host=mock.sentinel.host
        )
        mock_set_task_status.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
            constants.TASK_STATUS_CANCELLING_AFTER_COMPLETION,
            exception_details=mock.ANY,
        )

        mock_set_task_status.reset_mock()
        mock_get_task.return_value.status = constants.TASK_STATUS_PENDING
        call_set_task_host()
        mock_set_task_status.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
            constants.TASK_STATUS_STARTING,
            exception_details=None,
        )

    @mock.patch.object(db_api, 'set_task_status')
    @mock.patch.object(db_api, 'set_task_host_properties')
    @mock.patch.object(db_api, 'get_task')
    def test_set_task_process(
            self,
            mock_get_task,
            mock_set_task_host_properties,
            mock_set_task_status,
    ):
        def call_set_task_host():
            testutils.get_wrapped_function(
                self.server.set_task_process
            )(
                self.server,
                mock.sentinel.context,
                mock.sentinel.task_id,
                mock.sentinel.process_id,  # type: ignore
            )

        # task status is not in accepted state
        with self.assertRaisesRegex(
                exception.InvalidTaskState,
                "expected statuses"):
            call_set_task_host()

        mock_get_task.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
        )

        # task is in acceptable state
        mock_get_task.return_value.status = constants.TASK_STATUS_STARTING
        call_set_task_host()
        mock_set_task_host_properties.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
            process_id=mock.sentinel.process_id
        )
        mock_set_task_status.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
            constants.TASK_STATUS_RUNNING,
        )

        # task has no host
        mock_get_task.return_value = mock.Mock(
            host=None,
        )
        with self.assertRaisesRegex(
                exception.InvalidTaskState,
                "has no host"):
            call_set_task_host()

    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_set_tasks_execution_status'
    )
    @mock.patch.object(db_api, 'set_task_status')
    @mock.patch.object(db_api, 'get_tasks_execution')
    def test_check_clean_execution_deadlock(
            self,
            mock_get_tasks_execution,
            mock_set_task_status,
            mock_set_tasks_execution_status,
    ):
        execution = mock.Mock(
            id=mock.sentinel.execution_id,
        )

        def call_check_clean_execution_deadlock(
                task_statuses=None,
                requery=False,
        ):
            return self.server._check_clean_execution_deadlock(
                mock.sentinel.context,
                execution,
                task_statuses=task_statuses,
                requery=requery,
            )

        # requery with default task_statuses
        determined_state = call_check_clean_execution_deadlock(requery=True)
        mock_get_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.execution_id,
        )
        # RUNNING is default state
        self.assertEqual(
            determined_state,
            constants.EXECUTION_STATUS_RUNNING
        )

        # is deadlocked with 2 tasks that should be stranded
        task_statuses = {
            mock.sentinel.task_1: constants.TASK_STATUS_SCHEDULED,
            mock.sentinel.task_2: constants.TASK_STATUS_ON_ERROR_ONLY,
        }
        determined_state = call_check_clean_execution_deadlock(
            task_statuses=task_statuses,
        )
        mock_set_task_status.assert_has_calls([
            mock.call(
                mock.sentinel.context,
                mock.sentinel.task_1,
                constants.TASK_STATUS_CANCELED_FROM_DEADLOCK,
                exception_details=mock.ANY,
            ),
            mock.call(
                mock.sentinel.context,
                mock.sentinel.task_2,
                constants.TASK_STATUS_CANCELED_FROM_DEADLOCK,
                exception_details=mock.ANY,
            ),
        ])
        mock_set_tasks_execution_status.assert_called_once_with(
            mock.sentinel.context,
            execution,
            constants.EXECUTION_STATUS_DEADLOCKED,
        )
        self.assertEqual(
            determined_state,
            constants.EXECUTION_STATUS_DEADLOCKED
        )

        # has a pending task, not deadlocked
        task_statuses = {
            mock.sentinel.task_1: constants.TASK_STATUS_SCHEDULED,
            mock.sentinel.task_2: constants.TASK_STATUS_PENDING,
        }
        mock_set_task_status.reset_mock()
        mock_set_tasks_execution_status.reset_mock()
        determined_state = call_check_clean_execution_deadlock(
            task_statuses=task_statuses,
        )
        mock_set_task_status.assert_not_called()
        mock_set_tasks_execution_status.assert_not_called()
        self.assertEqual(
            determined_state,
            constants.EXECUTION_STATUS_RUNNING
        )

        # deadlocked with 2 tasks but one is not stranded
        task_statuses = {
            mock.sentinel.task_1: constants.TASK_STATUS_SCHEDULED,
            mock.sentinel.task_2: constants.TASK_STATUS_ERROR,
        }
        determined_state = call_check_clean_execution_deadlock(
            task_statuses=task_statuses,
        )
        mock_set_task_status.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_1,
            constants.TASK_STATUS_CANCELED_FROM_DEADLOCK,
            exception_details=mock.ANY,
        )
        mock_set_tasks_execution_status.assert_called_once_with(
            mock.sentinel.context,
            execution,
            constants.EXECUTION_STATUS_DEADLOCKED,
        )
        self.assertEqual(
            determined_state,
            constants.EXECUTION_STATUS_DEADLOCKED
        )

    @mock.patch.object(db_api, 'get_tasks_execution')
    def test_get_execution_status_no_config(
            self,
            mock_get_tasks_execution,
    ):
        execution = mock.Mock(
            id=mock.sentinel.execution_id,
        )

        def call_get_execution_status(
                requery=False,
        ):
            return self.server._get_execution_status(
                mock.sentinel.context,
                execution,
                requery=requery,
            )

        # task is requeried
        status = call_get_execution_status(requery=True)
        mock_get_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.execution_id,
        )
        # default state is COMPLETED
        self.assertEqual(
            status,
            constants.EXECUTION_STATUS_COMPLETED
        )

    @ddt.file_data("data/get_execution_status_config.yml")
    @ddt.unpack
    def test_get_execution_status(
            self,
            config,
            expected_status
    ):
        tasks = config.get('tasks', [])
        execution = mock.Mock(
            id=mock.sentinel.execution_id,
            tasks=[
                mock.Mock(
                    id=f'task_{status}',
                    status=status,
                ) for status in tasks
            ],
        )
        status = self.server._get_execution_status(
            mock.sentinel.context,
            execution,
            requery=False,
        )
        self.assertEqual(status, expected_status)

    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_set_tasks_execution_status'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_get_execution_status'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_cancel_tasks_execution'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_get_worker_service_rpc_for_task'
    )
    @mock.patch.object(db_api, 'set_task_status')
    @mock.patch.object(db_api, 'get_endpoint')
    @mock.patch.object(db_api, 'get_action')
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_get_task_destination'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_get_task_origin'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_check_clean_execution_deadlock'
    )
    @mock.patch.object(db_api, 'get_tasks_execution')
    def test_advance_execution_state_no_config(
            self,
            mock_get_tasks_execution,
            mock_check_clean_execution_deadlock,
            mock_get_task_origin,
            mock_get_task_destination,
            mock_get_action,
            mock_get_endpoint,
            mock_set_task_status,
            mock_get_worker_service_rpc_for_task,
            mock_cancel_tasks_execution,
            mock_get_execution_status,
            mock_set_tasks_execution_status,
    ):
        # no active status and requery
        started_tasks = self.server._advance_execution_state(
            mock.sentinel.context,
            mock.Mock(
                id=mock.sentinel.execution_id,
            ),
        )
        mock_get_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.execution_id,
        )
        mock_check_clean_execution_deadlock.assert_called_once_with(
            mock.sentinel.context,
            mock_get_tasks_execution.return_value,
            task_statuses=None,
            requery=False,
        )
        self.assertEqual(started_tasks, [])

        execution = mock.Mock(
            status=constants.EXECUTION_STATUS_RUNNING,
            tasks=[],
        )

        def call_advance_execution_state(
                requery=False,
        ):
            return self.server._advance_execution_state(
                mock.sentinel.context,
                execution,
                requery=requery,
                instance=None,
            )

        # call with no tasks
        with self.assertRaisesRegex(
            exception.InvalidActionTasksExecutionState,
            "no tasks"
        ):
            call_advance_execution_state()

        # test the flow with 1 non-scheduled task
        execution.tasks = [
            mock.Mock(
                id=mock.sentinel.task_1,
                status=constants.TASK_STATUS_ERROR,
                depends_on=[],
            ),
        ]
        started_tasks = call_advance_execution_state()

        mock_get_task_origin.assert_called_once_with(
            mock.sentinel.context,
            execution.action
        )
        mock_get_task_destination.assert_called_once_with(
            mock.sentinel.context,
            execution.action
        )
        mock_get_action.assert_called_once_with(
            mock.sentinel.context,
            execution.action_id,
            include_task_info=True,
        )
        mock_get_endpoint.assert_has_calls([
            mock.call(
                mock.sentinel.context,
                execution.action.origin_endpoint_id,
            ),
            mock.call(
                mock.sentinel.context,
                execution.action.destination_endpoint_id,
            ),
        ])
        mock_check_clean_execution_deadlock.assert_called_with(
            mock.sentinel.context,
            execution,
            task_statuses={mock.sentinel.task_1: constants.TASK_STATUS_ERROR},
        )
        mock_get_execution_status.assert_called_once_with(
            mock.sentinel.context,
            execution,
            requery=True,
        )
        mock_set_tasks_execution_status.assert_called_once_with(
            mock.sentinel.context,
            execution,
            mock_get_execution_status.return_value,
        )
        self.assertEqual(started_tasks, [])

        # execution is deadlocked
        mock_check_clean_execution_deadlock\
            .return_value = constants.EXECUTION_STATUS_DEADLOCKED
        mock_get_execution_status.reset_mock()
        started_tasks = call_advance_execution_state()
        mock_get_execution_status.assert_not_called()
        self.assertEqual(started_tasks, [])

        # last execution status is the execution status
        mock_check_clean_execution_deadlock.reset_mock(return_value=True)
        mock_get_execution_status\
            .return_value = constants.EXECUTION_STATUS_RUNNING
        mock_set_tasks_execution_status.reset_mock()
        call_advance_execution_state()
        mock_set_tasks_execution_status.assert_not_called()

        # task info is set from action info of the instance
        task = mock.Mock(
            id=mock.sentinel.task_1,
            status=constants.TASK_STATUS_SCHEDULED,
            instance=mock.sentinel.instance,
            task_type=mock.sentinel.task_type,
            depends_on=[],
        )
        execution.tasks = [task]
        task_info = {
            mock.sentinel.instance: {
                'test': 'info',
            },
        }
        mock_get_action.return_value = mock.Mock(
            info=task_info
        )
        started_tasks = call_advance_execution_state()
        mock_get_worker_service_rpc_for_task.assert_called_once_with(
            mock.sentinel.context,
            task,
            mock.ANY,
            mock.ANY,
        )
        mock_get_worker_service_rpc_for_task.return_value\
            .begin_task.assert_called_once_with(
                mock.sentinel.context,
                task_id=mock.sentinel.task_1,
                task_type=mock.sentinel.task_type,
                origin=mock_get_task_origin.return_value,
                destination=mock_get_task_destination.return_value,
                instance=mock.sentinel.instance,
                task_info=task_info[mock.sentinel.instance],
            )
        self.assertEqual(started_tasks, [task.id])

        # handles worker service rpc error
        mock_get_worker_service_rpc_for_task.side_effect = (
            CoriolisTestException())
        self.assertRaises(
            CoriolisTestException,
            call_advance_execution_state,
        )
        mock_cancel_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            execution,
            requery=True,
        )

    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_set_tasks_execution_status'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_get_execution_status'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_cancel_tasks_execution'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_get_worker_service_rpc_for_task'
    )
    @mock.patch.object(db_api, 'set_task_status')
    @mock.patch.object(db_api, 'get_endpoint')
    @mock.patch.object(db_api, 'get_action')
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_get_task_destination'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_get_task_origin'
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_check_clean_execution_deadlock'
    )
    @mock.patch.object(db_api, 'get_tasks_execution')
    @ddt.file_data("data/advance_execution_state.yml")
    @ddt.unpack
    def test_advance_execution_state_scheduled_tasks(
            self,
            mock_get_tasks_execution,
            mock_check_clean_execution_deadlock,
            mock_get_task_origin,
            mock_get_task_destination,
            mock_get_action,
            mock_get_endpoint,
            mock_set_task_status,
            mock_get_worker_service_rpc_for_task,
            mock_cancel_tasks_execution,
            mock_get_execution_status,
            mock_set_tasks_execution_status,
            config):
        tasks = config.get('tasks', [])
        execution = mock.Mock(
            status=constants.EXECUTION_STATUS_RUNNING,
            tasks=[
                mock.Mock(
                    index=i,
                    id=t['id'],
                    status=t['status'],
                    on_error=t.get('on_error', None),
                    depends_on=t.get('depends_on', []),
                ) for i, t in enumerate(tasks)
            ],
        )

        started_tasks = self.server._advance_execution_state(
            mock.sentinel.context,
            execution,
            requery=False,
            instance=None,
        )

        for task in tasks:
            if 'expected_status' not in task:
                continue
            kwargs = {'exception_details': mock.ANY}
            if task['expected_status'] == constants.TASK_STATUS_PENDING:
                kwargs = {}
            mock_set_task_status.assert_has_calls([
                mock.call(
                    mock.sentinel.context,
                    task['id'],
                    task['expected_status'],
                    **kwargs
                )
            ])

        self.assertEqual(
            started_tasks,
            [t['id'] for t in tasks
             if 'expected_status' in t and t['expected_status'] ==
                constants.TASK_STATUS_PENDING]
        )

    @mock.patch.object(db_api, 'update_transfer_action_info_for_instance')
    def test_update_transfer_volumes_info(
        self,
        mock_update_transfer_action_info_for_instance
    ):
        self.server._update_transfer_volumes_info(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.instance,
            mock.sentinel.updated_task_info
        )

        mock_update_transfer_action_info_for_instance.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.instance,
            mock.sentinel.updated_task_info
        )

    @mock.patch.object(server.ConductorServerEndpoint,
                       '_update_transfer_volumes_info')
    @mock.patch.object(lockutils, 'lock')
    @mock.patch.object(db_api, 'get_deployment')
    def test_update_volumes_info_for_deployment_parent_transfer(
        self,
        mock_get_deployment,
        mock_lock,
        mock_update_transfer_volumes_info
    ):
        deployment = mock.Mock()
        mock_get_deployment.return_value = deployment

        self.server._update_volumes_info_for_deployment_parent_transfer(
            mock.sentinel.context,
            mock.sentinel.deployment_id,
            mock.sentinel.instance,
            mock.sentinel.updated_task_info
        )

        mock_get_deployment.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.deployment_id
        )
        mock_lock.assert_called_once_with(
            constants.TRANSFER_LOCK_NAME_FORMAT %
            mock_get_deployment.return_value.transfer_id,
            external=True
        )
        mock_update_transfer_volumes_info.assert_called_once_with(
            mock.sentinel.context,
            mock_get_deployment.return_value.transfer_id,
            mock.sentinel.instance,
            mock.sentinel.updated_task_info
        )

    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_minion_manager_client'
    )
    @mock.patch.object(db_api, 'update_minion_machine')
    @mock.patch.object(db_api, 'update_transfer')
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_update_transfer_volumes_info'
    )
    @mock.patch.object(db_api, 'set_transfer_action_result')
    @mock.patch.object(schemas, 'validate_value')
    @mock.patch.object(
        server.ConductorServerEndpoint,
        '_update_volumes_info_for_deployment_parent_transfer'
    )
    def test_handle_post_task_actions(
            self,
            mock_update_volumes_info_for_deployment_parent_transfer,
            mock_validate_value,
            mock_set_transfer_action_result,
            mock_update_transfer_volumes_info,
            mock_update_transfer,
            mock_update_minion_machine,
            mock_minion_manager_client,
    ):
        task = mock.Mock(
            task_type=constants.TASK_TYPE_RESTORE_TRANSFER_DISK_SNAPSHOTS,
            instance=mock.sentinel.instance,
        )
        execution = mock.Mock(
            action_id=mock.sentinel.action_id,
        )
        task_info = {
            'volumes_info': [],
        }

        def call_handle_post_task_actions():
            self.server._handle_post_task_actions(
                mock.sentinel.context,
                task,
                execution,
                task_info,
            )
        call_handle_post_task_actions()

        # no volumes_info
        mock_update_volumes_info_for_deployment_parent_transfer\
            .assert_not_called()

        # has volumes_info
        task_info["volumes_info"] = [
            {
                "id": "volume_id",
            }
        ]
        call_handle_post_task_actions()
        mock_update_volumes_info_for_deployment_parent_transfer\
            .assert_called_once_with(
                mock.sentinel.context,
                mock.sentinel.action_id,
                mock.sentinel.instance,
                {"volumes_info": task_info["volumes_info"]},
            )

        task.task_type = constants\
            .TASK_TYPE_DELETE_TRANSFER_TARGET_DISK_SNAPSHOTS
        call_handle_post_task_actions()
        # no clone_disks, reset volumes_info
        mock_update_volumes_info_for_deployment_parent_transfer\
            .assert_called_with(
                mock.sentinel.context,
                mock.sentinel.action_id,
                mock.sentinel.instance,
                {"volumes_info": []},
            )

        # has clone_disks
        task_info['clone_disks'] = [
            {
                'id': 'clone_disk_id',
            }
        ]
        mock_update_volumes_info_for_deployment_parent_transfer\
            .reset_mock()
        call_handle_post_task_actions()
        mock_update_volumes_info_for_deployment_parent_transfer\
            .assert_not_called()

        types = [
            constants.TASK_TYPE_FINALIZE_INSTANCE_DEPLOYMENT,
        ]
        for task_type in types:
            task.task_type = task_type

            # no transfer_result
            task_info.pop('transfer_result', None)
            call_handle_post_task_actions()
            mock_validate_value.assert_not_called()
            mock_set_transfer_action_result.assert_not_called()

            # has transfer_result
            task_info['transfer_result'] = [
                {
                    'result_1': 'value_1',
                }
            ]
            call_handle_post_task_actions()
            mock_validate_value.assert_called_once_with(
                task_info['transfer_result'],
                schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA
            )
            mock_set_transfer_action_result.assert_called_once_with(
                mock.sentinel.context,
                mock.sentinel.action_id,
                mock.sentinel.instance,
                task_info['transfer_result'],
            )

            # handles schema validation error
            mock_set_transfer_action_result.reset_mock()
            mock_validate_value.side_effect = (
                exception.SchemaValidationException()
            )
            call_handle_post_task_actions()
            mock_set_transfer_action_result.assert_not_called()
            mock_validate_value.side_effect = None
            mock_validate_value.reset_mock()
            mock_set_transfer_action_result.reset_mock()

        types = [
            constants.TASK_TYPE_UPDATE_SOURCE_TRANSFER,
            constants.TASK_TYPE_UPDATE_DESTINATION_TRANSFER,
        ]
        execution.tasks = [
            mock.Mock(
                id='task_id_1',
                status=constants.TASK_STATUS_PENDING,
            )
        ]
        for task_type in types:
            task.task_type = task_type
            mock_update_transfer_volumes_info.reset_mock()
            call_handle_post_task_actions()
            mock_update_transfer_volumes_info.assert_called_once_with(
                mock.sentinel.context,
                mock.sentinel.action_id,
                mock.sentinel.instance,
                {"volumes_info": task_info["volumes_info"]},
            )

        # execution has active tasks
        task.type = constants.TASK_TYPE_UPDATE_DESTINATION_TRANSFER
        call_handle_post_task_actions()
        mock_update_transfer.assert_not_called()

        # execution has no active tasks
        execution.tasks = [
            mock.Mock(
                id='task_id_1',
                status=constants.TASK_STATUS_ERROR,
            )
        ]
        call_handle_post_task_actions()
        mock_update_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.action_id,
            task_info
        )
        mock_update_transfer.reset_mock()

        # TASK_TYPE_ATTACH_VOLUMES_TO_SOURCE_MINION
        # TASK_TYPE_DETACH_VOLUMES_FROM_SOURCE_MINION
        types = [
            constants.TASK_TYPE_ATTACH_VOLUMES_TO_SOURCE_MINION,
            constants.TASK_TYPE_DETACH_VOLUMES_FROM_SOURCE_MINION,
        ]
        task_info['origin_minion_machine_id'] = ['minion_machine_id']
        task_info['origin_minion_provider_properties'] = [
            {'minion_provider_properties': 'value'}
        ]
        for task_type in types:
            task.task_type = task_type
            call_handle_post_task_actions()
            mock_update_minion_machine.assert_called_once_with(
                mock.sentinel.context,
                task_info['origin_minion_machine_id'],
                {'provider_properties': [
                    {'minion_provider_properties': 'value'}]},
            )
            mock_update_minion_machine.reset_mock()

        # TASK_TYPE_ATTACH_VOLUMES_TO_DESTINATION_MINION
        # TASK_TYPE_DETACH_VOLUMES_FROM_DESTINATION_MINION
        types = [
            constants.TASK_TYPE_ATTACH_VOLUMES_TO_DESTINATION_MINION,
            constants.TASK_TYPE_DETACH_VOLUMES_FROM_DESTINATION_MINION,
        ]
        task_info['destination_minion_machine_id'] = ['minion_machine_id']
        task_info['destination_minion_provider_properties'] = [
            {'minion_provider_properties': 'value'}
        ]
        for task_type in types:
            task.task_type = task_type
            call_handle_post_task_actions()
            mock_update_minion_machine.assert_called_once_with(
                mock.sentinel.context,
                task_info['destination_minion_machine_id'],
                {'provider_properties': [
                    {'minion_provider_properties': 'value'}]},
            )
            mock_update_minion_machine.reset_mock()

        # TASK_TYPE_ATTACH_VOLUMES_TO_OSMORPHING_MINION
        # TASK_TYPE_DETACH_VOLUMES_FROM_OSMORPHING_MINION
        task_info['osmorphing_minion_machine_id'] = ['minion_machine_id']
        task_info['osmorphing_minion_provider_properties'] = [
            {'minion_provider_properties': 'value'}
        ]
        types = [
            constants.TASK_TYPE_ATTACH_VOLUMES_TO_OSMORPHING_MINION,
            constants.TASK_TYPE_DETACH_VOLUMES_FROM_OSMORPHING_MINION,
        ]
        for task_type in types:
            task.task_type = task_type
            call_handle_post_task_actions()
            mock_update_minion_machine.assert_called_once_with(
                mock.sentinel.context,
                task_info['osmorphing_minion_machine_id'],
                {'provider_properties': [
                    {'minion_provider_properties': 'value'}]},
            )
            mock_update_minion_machine.reset_mock()

        # TASK_TYPE_RELEASE_SOURCE_MINION
        task.task_type = constants.TASK_TYPE_RELEASE_SOURCE_MINION
        call_handle_post_task_actions()
        mock_minion_manager_client.deallocate_minion_machine\
            .assert_called_once_with(
                mock.sentinel.context,
                task_info['origin_minion_machine_id'],
            )
        mock_minion_manager_client.deallocate_minion_machine.reset_mock()

        # TASK_TYPE_RELEASE_DESTINATION_MINION
        task.task_type = constants.TASK_TYPE_RELEASE_DESTINATION_MINION

        # destination minion machine is the same as osmorphing minion machine
        task_info['destination_minion_machine_id'] = ['other id']
        call_handle_post_task_actions()
        mock_minion_manager_client.deallocate_minion_machine\
            .assert_called_once_with(
                mock.sentinel.context,
                task_info['destination_minion_machine_id'],
            )
        mock_minion_manager_client.deallocate_minion_machine.reset_mock()

        # destination minion machine is different
        # from osmorphing minion machine
        task_info['destination_minion_machine_id'] = ['minion_machine_id']
        call_handle_post_task_actions()
        mock_minion_manager_client.deallocate_minion_machine\
            .assert_not_called()
        mock_minion_manager_client.deallocate_minion_machine.reset_mock()

        # TASK_TYPE_RELEASE_OSMORPHING_MINION
        task.task_type = constants.TASK_TYPE_RELEASE_OSMORPHING_MINION
        call_handle_post_task_actions()
        mock_minion_manager_client.deallocate_minion_machine\
            .assert_called_once_with(
                mock.sentinel.context,
                task_info['osmorphing_minion_machine_id'],
            )
        mock_minion_manager_client.deallocate_minion_machine.reset_mock()

        # for any other type of task nothing is called
        task.task_type = constants.TASK_TYPE_COLLECT_OSMORPHING_INFO
        call_handle_post_task_actions()
        mock_update_transfer.assert_not_called()
        mock_update_minion_machine.assert_not_called()
        mock_minion_manager_client.deallocate_minion_machine\
            .assert_not_called()

    @mock.patch.object(utils, "sanitize_task_info")
    @mock.patch.object(db_api, "get_task")
    @mock.patch.object(db_api, "set_task_status")
    @mock.patch.object(db_api, "get_tasks_execution")
    @mock.patch.object(db_api, "get_action")
    @mock.patch.object(db_api, "update_transfer_action_info_for_instance")
    @mock.patch.object(lockutils, "lock")
    @ddt.file_data("data/task_completed_config.yml")
    @ddt.unpack
    def test_task_completed(
            self,
            mock_lock,
            mock_update_transfer_action_info,
            mock_get_action,
            mock_get_tasks_execution,
            mock_set_task_status,
            mock_get_task,
            mock_sanitize_task_info,
            config,
            expected_status,
    ):
        task_status = config['task_status']
        has_exception_details = config.get('has_exception_details', True)
        mock_get_task.return_value = mock.Mock(
            status=task_status,
            instance=mock.sentinel.instance,
        )

        mock_get_tasks_execution.return_value = mock.Mock(
            id=mock.sentinel.execution_id,
            type=constants.EXECUTION_TYPE_DEPLOYMENT,
            action_id=mock.sentinel.action_id,
            tasks=[
                mock.Mock(
                    id=mock.sentinel.task_id,
                    status=constants.TASK_STATUS_COMPLETED
                )
            ]
        )

        self.server.task_completed(
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.task_result
        )
        if expected_status is None:
            mock_set_task_status.assert_not_called()
            return
        else:
            kwargs = {'exception_details': mock.ANY}
            if has_exception_details is False:
                kwargs = {}
            mock_set_task_status.assert_called_once_with(
                mock.sentinel.context,
                mock.sentinel.task_id,
                expected_status,
                **kwargs,
            )

        mock_get_tasks_execution.assert_called_with(
            mock.sentinel.context,
            mock.sentinel.execution_id,
        )
        mock_get_action.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.action_id,
            include_task_info=True,
        )
        mock_update_transfer_action_info.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.action_id,
            mock.sentinel.instance,
            mock.sentinel.task_result,
        )

        mock_get_action.reset_mock()
        mock_update_transfer_action_info.reset_mock()

        # no task result
        self.server.task_completed(
            mock.sentinel.context,
            mock.sentinel.task_id,
            None
        )
        mock_update_transfer_action_info.assert_not_called()
        self.assertEqual(2, mock_get_action.call_count)

    @mock.patch.object(db_api, 'set_task_status')
    def test_cancel_execution_for_osmorphing_debugging(
        self,
        mock_set_task_status
    ):
        subtask = mock.Mock()
        execution = mock.Mock()
        execution.tasks = [subtask]

        subtask.task_type = constants.TASK_TYPE_OS_MORPHING
        subtask.status = constants.TASK_STATUS_PENDING
        self.server._cancel_execution_for_osmorphing_debugging(
            mock.sentinel.context,
            execution
        )
        mock_set_task_status.assert_not_called()

        subtask.task_type = None
        self.assertRaises(
            exception.CoriolisException,
            self.server._cancel_execution_for_osmorphing_debugging,
            mock.sentinel.context,
            execution
        )

        subtask.status = constants.TASK_STATUS_COMPLETED
        self.server._cancel_execution_for_osmorphing_debugging(
            mock.sentinel.context,
            execution
        )
        mock_set_task_status.assert_not_called()

        subtask.status = constants.TASK_STATUS_SCHEDULED
        subtask.on_error = True
        self.server._cancel_execution_for_osmorphing_debugging(
            mock.sentinel.context,
            execution
        )
        mock_set_task_status.assert_called_once_with(
            mock.sentinel.context,
            subtask.id,
            constants.TASK_STATUS_CANCELED_FOR_DEBUGGING,
            exception_details=mock.ANY
        )

    @mock.patch.object(server.ConductorServerEndpoint,
                       "_advance_execution_state")
    @mock.patch.object(db_api, "get_tasks_execution")
    @mock.patch.object(db_api, "set_task_status")
    @mock.patch.object(db_api, "get_task")
    @ddt.file_data("data/confirm_task_cancellation.yml")
    @ddt.unpack
    def test_confirm_task_cancellation(
        self,
        mock_get_task,
        mock_set_task_status,
        mock_get_tasks_execution,
        mock_advance_execution_state,
        task_status,
        expected_final_status,
        expected_advance_execution_state_call
    ):
        task = mock.Mock()
        task.status = getattr(constants, task_status)
        expected_final_status = getattr(constants, expected_final_status)
        mock_get_task.return_value = task
        mock_execution = mock.MagicMock()
        mock_execution.type = constants.EXECUTION_TYPE_DEPLOYMENT
        mock_get_tasks_execution.return_value = mock_execution

        testutils.get_wrapped_function(self.server.confirm_task_cancellation)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.cancellation_details
        )

        mock_set_task_status.assert_called_once_with(
            mock.sentinel.context,
            task.id,
            expected_final_status,
            exception_details=mock.ANY
        )
        if expected_advance_execution_state_call:
            mock_get_tasks_execution.assert_called_once_with(
                mock.sentinel.context, task.execution_id)
            mock_advance_execution_state.assert_called_once_with(
                mock.sentinel.context,
                mock_get_tasks_execution.return_value,
                requery=False
            )
        else:
            mock_get_tasks_execution.assert_not_called()
            mock_advance_execution_state.assert_not_called()

    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_check_delete_reservation_for_transfer"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_cancel_tasks_execution"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_set_tasks_execution_status"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_cancel_execution_for_osmorphing_debugging"
    )
    @mock.patch.object(lockutils, "lock")
    @mock.patch.object(db_api, "get_action")
    @mock.patch.object(db_api, "get_tasks_execution")
    @mock.patch.object(db_api, "set_task_status")
    @mock.patch.object(db_api, "get_task")
    @ddt.file_data("data/set_task_error_config.yml")
    @ddt.unpack
    def test_set_task_error(
            self,
            mock_get_task,
            mock_set_task_status,
            mock_get_tasks_execution,
            mock_get_action,
            mock_lock,
            mock_cancel_execution_for_osmorphing_debugging,
            mock_set_tasks_execution_status,
            mock_cancel_tasks_execution,
            mock_check_delete_reservation_for_transfer,
            config,
            expected_status,
    ):
        task_status = config['task_status']
        mock_get_tasks_execution.return_value = mock.Mock(
            type=constants.EXECUTION_TYPE_DEPLOYMENT,
            action_id=mock.sentinel.action_id,
            tasks=[
                mock.Mock(
                    id=mock.sentinel.task_id,
                    status=constants.TASK_STATUS_COMPLETED
                )
            ]
        )
        mock_get_task.return_value = mock.Mock(
            status=task_status,
        )
        self.server.set_task_error(
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.exception_details,
        )
        mock_get_task.assert_called_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
        )
        mock_set_task_status.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
            expected_status,
            mock.ANY,
        )

    @mock.patch.object(db_api, "add_task_event")
    @mock.patch.object(db_api, "get_task")
    def test_add_task_event(
        self,
        mock_get_task,
        mock_add_task_event
    ):
        task = mock.Mock()
        mock_get_task.return_value = task
        task.status = constants.EXECUTION_STATUS_COMPLETED

        self.assertRaises(
            exception.InvalidTaskState,
            testutils.get_wrapped_function(self.server.add_task_event),
            self.server,
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.level,
            mock.sentinel.message
        )

        mock_get_task.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id
        )
        mock_add_task_event.assert_not_called()

        mock_get_task.reset_mock()
        task.status = constants.EXECUTION_STATUS_RUNNING

        testutils.get_wrapped_function(self.server.add_task_event)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.level,
            mock.sentinel.message
        )

        mock_get_task.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id
        )
        mock_add_task_event.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.level,
            mock.sentinel.message
        )

    @mock.patch.object(db_api, "add_task_progress_update")
    @mock.patch.object(db_api, "get_task")
    def test_add_task_progress_update(
        self,
        mock_get_task,
        mock_add_task_progress_update
    ):
        task = mock.Mock()
        mock_get_task.return_value = task
        task.status = constants.EXECUTION_STATUS_COMPLETED

        self.assertRaises(
            exception.InvalidTaskState,
            testutils.get_wrapped_function(
                self.server.add_task_progress_update),
            self.server,
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.message,
            initial_step=mock.sentinel.initial_step,
            total_steps=mock.sentinel.total_steps
        )

        mock_get_task.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id
        )
        mock_add_task_progress_update.assert_not_called()

        mock_get_task.reset_mock()
        task.status = constants.EXECUTION_STATUS_RUNNING

        result = testutils.get_wrapped_function(
            self.server.add_task_progress_update)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.message,
            initial_step=mock.sentinel.initial_step,
            total_steps=mock.sentinel.total_steps
        )

        self.assertEqual(
            mock_add_task_progress_update.return_value,
            result
        )
        mock_get_task.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id
        )
        mock_add_task_progress_update.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.message,
            initial_step=mock.sentinel.initial_step,
            total_steps=mock.sentinel.total_steps
        )

    @mock.patch.object(db_api, "update_task_progress_update")
    def test_update_task_progress_update(
        self,
        mock_update_task_progress_update
    ):
        testutils.get_wrapped_function(
            self.server.update_task_progress_update)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.progress_update_index,
            mock.sentinel.new_current_step,
            new_total_steps=mock.sentinel.new_total_steps,
            new_message=mock.sentinel.new_message,
        )

        mock_update_task_progress_update.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.progress_update_index,
            mock.sentinel.new_current_step,
            new_total_steps=mock.sentinel.new_total_steps,
            new_message=mock.sentinel.new_message,
        )

    @mock.patch.object(db_api, "get_transfer_schedule")
    def test__get_transfer_schedule(
        self,
        mock_get_transfer_schedule
    ):
        result = self.server._get_transfer_schedule(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.schedule_id,
            expired=True
        )

        self.assertEqual(
            mock_get_transfer_schedule.return_value,
            result
        )
        mock_get_transfer_schedule.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.schedule_id,
            expired=True
        )

        mock_get_transfer_schedule.reset_mock()
        mock_get_transfer_schedule.return_value = None

        self.assertRaises(
            exception.NotFound,
            self.server._get_transfer_schedule,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.schedule_id,
            expired=False
        )

        mock_get_transfer_schedule.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.schedule_id,
            expired=False
        )

    @mock.patch.object(server.ConductorServerEndpoint, "get_transfer_schedule")
    @mock.patch.object(db_api, "add_transfer_schedule")
    @mock.patch.object(models, "TransferSchedule")
    @mock.patch.object(server.ConductorServerEndpoint, "_get_transfer")
    @mock.patch.object(keystone, "create_trust")
    def test_create_transfer_schedule(
        self,
        mock_create_trust,
        mock_get_transfer,
        mock_transfer_schedule,
        mock_add_transfer_schedule,
        mock_get_transfer_schedule
    ):
        context = mock.Mock()
        transfer_schedule = mock.Mock()
        context.trust_id = mock.sentinel.trust_id
        mock_transfer_schedule.return_value = transfer_schedule

        result = self.server.create_transfer_schedule(
            context,
            mock.sentinel.transfer_id,
            mock.sentinel.schedule,
            mock.sentinel.enabled,
            mock.sentinel.exp_date,
            mock.sentinel.shutdown_instance,
            mock.sentinel.auto_deploy
        )

        self.assertEqual(
            mock_get_transfer_schedule.return_value,
            result
        )
        self.assertEqual(
            (
                transfer_schedule.transfer,
                transfer_schedule.transfer_id,
                transfer_schedule.schedule,
                transfer_schedule.expiration_date,
                transfer_schedule.enabled,
                transfer_schedule.shutdown_instance,
                transfer_schedule.auto_deploy,
                transfer_schedule.trust_id
            ),
            (
                mock_get_transfer.return_value,
                mock.sentinel.transfer_id,
                mock.sentinel.schedule,
                mock.sentinel.exp_date,
                mock.sentinel.enabled,
                mock.sentinel.shutdown_instance,
                mock.sentinel.auto_deploy,
                mock.sentinel.trust_id
            )
        )
        mock_create_trust.assert_called_once_with(context)
        mock_get_transfer.assert_called_once_with(
            context,
            mock.sentinel.transfer_id,
        )
        mock_transfer_schedule.assert_called_once()
        mock_add_transfer_schedule.assert_called_once_with(
            context,
            transfer_schedule,
            mock.ANY
        )
        mock_get_transfer_schedule.assert_called_once_with(
            context,
            mock.sentinel.transfer_id,
            transfer_schedule.id
        )

    @mock.patch.object(
        server.ConductorServerEndpoint, "_get_transfer_schedule")
    @mock.patch.object(db_api, "update_transfer_schedule")
    def test_update_transfer_schedule(
        self,
        mock_update_transfer_schedule,
        mock_get_transfer_schedule
    ):
        result = testutils.get_wrapped_function(
            self.server.update_transfer_schedule)(
                self.server,
                mock.sentinel.context,
                mock.sentinel.transfer_id,
                mock.sentinel.schedule_id,
                mock.sentinel.updated_values,
        )

        self.assertEqual(
            mock_get_transfer_schedule.return_value,
            result
        )
        mock_update_transfer_schedule.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.schedule_id,
            mock.sentinel.updated_values,
            None,
            mock.ANY
        )
        mock_get_transfer_schedule.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.schedule_id,
        )

    @mock.patch.object(keystone, "delete_trust")
    @mock.patch.object(context, "get_admin_context")
    @mock.patch.object(server.ConductorServerEndpoint, "_transfer_cron_client")
    def test_cleanup_schedule_resources(
        self,
        mock_transfer_cron_client,
        mock_get_admin_context,
        mock_delete_trust,
    ):
        schedule = mock.Mock()
        schedule.trust_id = None

        self.server._cleanup_schedule_resources(
            mock.sentinel.context,
            schedule
        )

        mock_transfer_cron_client.unregister.assert_called_once_with(
            mock.sentinel.context,
            schedule
        )
        mock_get_admin_context.assert_not_called()
        mock_delete_trust.assert_not_called()

        mock_transfer_cron_client.reset_mock()
        schedule.trust_id = mock.sentinel.trust_id

        self.server._cleanup_schedule_resources(
            mock.sentinel.context,
            schedule
        )

        mock_transfer_cron_client.unregister.assert_called_once_with(
            mock.sentinel.context,
            schedule
        )
        mock_get_admin_context.assert_called_once_with(
            trust_id=mock.sentinel.trust_id)
        mock_delete_trust.assert_called_once_with(
            mock_get_admin_context.return_value)

    @mock.patch.object(db_api, "delete_transfer_schedule")
    @mock.patch.object(server.ConductorServerEndpoint, "_get_transfer")
    def test_delete_transfer_schedule(
        self,
        mock_get_transfer,
        mock_delete_transfer_schedule
    ):
        transfer = mock.Mock()
        transfer.last_execution_status = constants.EXECUTION_STATUS_COMPLETED
        mock_get_transfer.return_value = transfer

        testutils.get_wrapped_function(self.server.delete_transfer_schedule)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.schedule_id
        )

        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id
        )
        mock_delete_transfer_schedule.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.schedule_id,
            None,
            mock.ANY
        )

        mock_get_transfer.reset_mock()
        mock_delete_transfer_schedule.reset_mock()
        transfer.last_execution_status = constants.EXECUTION_STATUS_RUNNING

        self.assertRaises(
            exception.InvalidTransferState,
            testutils.get_wrapped_function(
                self.server.delete_transfer_schedule),
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.schedule_id
        )

        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id
        )
        mock_delete_transfer_schedule.assert_not_called()

    @mock.patch.object(db_api, "get_transfer_schedules")
    def test_get_transfer_schedules(self, mock_get_transfer_schedules):
        result = testutils.get_wrapped_function(
            self.server.get_transfer_schedules)(
                self.server,
                mock.sentinel.context,
                transfer_id=None,
                expired=True
        )

        self.assertEqual(
            mock_get_transfer_schedules.return_value,
            result
        )
        mock_get_transfer_schedules.assert_called_once_with(
            mock.sentinel.context,
            transfer_id=None,
            expired=True
        )

    @mock.patch.object(db_api, "get_transfer_schedule")
    def test_get_transfer_schedule(self, mock_get_transfer_schedule):
        result = testutils.get_wrapped_function(
            self.server.get_transfer_schedule)(
                self.server,
                mock.sentinel.context,
                mock.sentinel.transfer_id,
                mock.sentinel.schedule_id,
                expired=True
        )

        self.assertEqual(
            mock_get_transfer_schedule.return_value,
            result
        )
        mock_get_transfer_schedule.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            mock.sentinel.schedule_id,
            expired=True
        )

    @mock.patch.object(server.ConductorServerEndpoint,
                       "get_transfer_tasks_execution")
    @mock.patch.object(server.ConductorServerEndpoint, "_begin_tasks")
    @mock.patch.object(db_api, "add_transfer_tasks_execution")
    @mock.patch.object(db_api, "update_transfer_action_info_for_instance")
    @mock.patch.object(server.ConductorServerEndpoint,
                       "_check_execution_tasks_sanity")
    @mock.patch.object(server.ConductorServerEndpoint, "_create_task")
    @mock.patch.object(utils, "sanitize_task_info")
    @mock.patch.object(models, "TasksExecution")
    @mock.patch.object(server.ConductorServerEndpoint,
                       "_check_valid_transfer_tasks_execution")
    @mock.patch.object(server.ConductorServerEndpoint,
                       "_check_transfer_running_executions")
    @mock.patch.object(server.ConductorServerEndpoint,
                       "_check_minion_pools_for_action")
    @mock.patch.object(models, "Transfer")
    @mock.patch.object(server.ConductorServerEndpoint, "_get_transfer")
    @ddt.file_data("data/update_transfer_config.yml")
    @ddt.unpack
    def test_update_transfer(
        self,
        mock_get_transfer,
        mock_transfer,
        mock_check_minion_pools_for_action,
        mock_check_transfer_running_executions,
        mock_check_valid_transfer_tasks_execution,
        mock_TasksExecution,
        mock_sanitize_task_info,
        mock_create_task,
        mock_check_execution_tasks_sanity,
        mock_update_transfer_action_info_for_instance,
        mock_add_transfer_tasks_execution,
        mock_begin_tasks,
        mock_get_transfer_tasks_execution,
        config,
        has_updated_values,
        has_transfer_instance
    ):
        transfer = mock.Mock()
        dummy = mock.Mock()
        execution = mock.Mock()
        transfer.instances = config['transfer'].get("instances", [])
        transfer.info = config['transfer'].get("info", {})
        mock_get_transfer.return_value = transfer
        mock_transfer.return_value = dummy
        mock_TasksExecution.return_value = execution
        updated_properties = config.get("updated_properties", {})

        result = testutils.get_wrapped_function(self.server.update_transfer)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            updated_properties
        )

        self.assertEqual(
            mock_get_transfer_tasks_execution.return_value,
            result
        )
        mock_get_transfer.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            include_task_info=True
        )
        mock_check_transfer_running_executions.assert_called_once_with(
            mock.sentinel.context,
            mock_get_transfer.return_value,
        )
        mock_check_valid_transfer_tasks_execution.assert_called_once_with(
            mock_get_transfer.return_value,
            force=True,
        )
        self.assertEqual(
            execution.action,
            mock_get_transfer.return_value
        )
        mock_check_execution_tasks_sanity.assert_called_once_with(
            execution,
            transfer.info
        )
        mock_add_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            execution
        )
        mock_begin_tasks.assert_called_once_with(
            mock.sentinel.context,
            transfer,
            execution
        )
        mock_get_transfer_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.transfer_id,
            execution.id
        )
        if has_updated_values:
            mock_check_minion_pools_for_action.assert_called_once_with(
                mock.sentinel.context,
                dummy
            )
        else:
            mock_check_minion_pools_for_action.assert_not_called()
        if has_transfer_instance:
            expected_sanitize_task_info_calls = []
            create_task_calls = []
            update_transfer_action_info_for_instance_calls = []
            for instance in config['transfer'].get("info", {}):
                expected_sanitize_task_info_calls.append(
                    mock.call(mock.ANY))
                expected_sanitize_task_info_calls.append(
                    mock.call(transfer.info[instance]))
                create_task_calls.append(mock.call(
                    instance,
                    constants.TASK_TYPE_GET_INSTANCE_INFO,
                    execution))
                create_task_calls.append(mock.call(
                    instance,
                    constants.TASK_TYPE_UPDATE_SOURCE_TRANSFER,
                    execution))
                create_task_calls.append(mock.call(
                    instance,
                    constants.TASK_TYPE_UPDATE_DESTINATION_TRANSFER,
                    execution,
                    depends_on=mock.ANY))
                update_transfer_action_info_for_instance_calls.append(
                    mock.call(
                        mock.sentinel.context,
                        transfer.id,
                        instance,
                        transfer.info[instance])
                )
            mock_sanitize_task_info.assert_has_calls(
                expected_sanitize_task_info_calls)
            mock_create_task.assert_has_calls(create_task_calls)
            mock_update_transfer_action_info_for_instance.assert_has_calls(
                update_transfer_action_info_for_instance_calls)

    @mock.patch.object(server.ConductorServerEndpoint, "get_region")
    @mock.patch.object(db_api, "add_region")
    @mock.patch.object(models, "Region")
    def test_create_region(
        self,
        mock_Region,
        mock_add_region,
        mock_get_region
    ):
        result = self.server.create_region(
            mock.sentinel.context,
            mock.sentinel.region_name,
            description=mock.sentinel.description,
            enabled=True
        )

        self.assertEqual(
            mock_get_region.return_value,
            result
        )
        self.assertEqual(
            (
                mock_Region.return_value.name,
                mock_Region.return_value.description,
                mock_Region.return_value.enabled
            ),
            (
                mock.sentinel.region_name,
                mock.sentinel.description,
                True
            )
        )
        mock_Region.assert_called_once()
        mock_add_region.assert_called_once_with(
            mock.sentinel.context,
            mock_Region.return_value
        )
        mock_get_region.assert_called_once_with(
            mock.sentinel.context,
            mock_Region.return_value.id
        )

    @mock.patch.object(db_api, "get_regions")
    def test_get_regions(self, mock_get_regions):
        result = self.server.get_regions(mock.sentinel.context)

        self.assertEqual(
            mock_get_regions.return_value,
            result
        )
        mock_get_regions.assert_called_once_with(mock.sentinel.context)

    @mock.patch.object(db_api, "get_region")
    def test_get_region(self, mock_get_region):
        result = testutils.get_wrapped_function(
            self.server.get_region)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.region_id
        )

        self.assertEqual(
            mock_get_region.return_value,
            result
        )
        mock_get_region.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.region_id)

        mock_get_region.reset_mock()
        mock_get_region.return_value = None
        self.assertRaises(
            exception.NotFound,
            testutils.get_wrapped_function(self.server.get_region),
            self.server,
            mock.sentinel.context,
            mock.sentinel.region_id,
        )
        mock_get_region.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.region_id)

    @mock.patch.object(db_api, "get_region")
    @mock.patch.object(db_api, "update_region")
    def test_update_region(
        self,
        mock_update_region,
        mock_get_region
    ):
        result = testutils.get_wrapped_function(
            self.server.update_region)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.region_id,
            mock.sentinel.updated_values
        )

        self.assertEqual(
            mock_get_region.return_value,
            result
        )
        mock_update_region.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.region_id,
            mock.sentinel.updated_values
        )
        mock_get_region.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.region_id
        )

    @mock.patch.object(db_api, "delete_region")
    def test_delete_region(self, mock_delete_region):
        testutils.get_wrapped_function(self.server.delete_region)(
            self.server, mock.sentinel.context, mock.sentinel.region_id)

        mock_delete_region.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.region_id)

    @mock.patch.object(rpc_worker_client.WorkerClient, "get_service_status")
    @mock.patch.object(db_api, "delete_service")
    @mock.patch.object(db_api, "update_service")
    @mock.patch.object(server.ConductorServerEndpoint, "get_service")
    @mock.patch.object(db_api, "add_service")
    @mock.patch.object(models, "Service")
    @mock.patch.object(db_api, "find_service")
    def test_register_service(
        self,
        mock_find_service,
        mock_Service,
        mock_add_service,
        mock_get_service,
        mock_update_service,
        mock_delete_service,
        mock_get_service_status
    ):
        self.assertRaises(
            exception.Conflict,
            self.server.register_service,
            mock.sentinel.context,
            mock.sentinel.host,
            mock.sentinel.binary,
            mock.sentinel.topic,
            mock.sentinel.enabled,
            mapped_regions=mock.sentinel.mapped_regions,
            providers=mock.sentinel.providers,
            specs=mock.sentinel.specs
        )

        mock_find_service.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.host,
            mock.sentinel.binary,
            topic=mock.sentinel.topic
        )

        mock_find_service.reset_mock()
        mock_find_service.return_value = None

        result = self.server.register_service(
            mock.sentinel.context,
            mock.sentinel.host,
            mock.sentinel.binary,
            mock.sentinel.topic,
            mock.sentinel.enabled,
            mapped_regions=mock.sentinel.mapped_regions,
            providers=mock.sentinel.providers,
            specs=mock.sentinel.specs
        )

        self.assertEqual(
            mock_get_service.return_value,
            result
        )
        self.assertEqual(
            (
                mock_Service.return_value.host,
                mock_Service.return_value.binary,
                mock_Service.return_value.topic,
                mock_Service.return_value.enabled,
                mock_Service.return_value.providers,
                mock_Service.return_value.specs
            ),
            (
                mock.sentinel.host,
                mock.sentinel.binary,
                mock.sentinel.topic,
                mock.sentinel.enabled,
                mock.sentinel.providers,
                mock.sentinel.specs
            )
        )
        mock_find_service.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.host,
            mock.sentinel.binary,
            topic=mock.sentinel.topic
        )
        mock_Service.assert_called_once()
        mock_add_service.assert_called_once_with(
            mock.sentinel.context,
            mock_Service.return_value
        )
        mock_get_service_status.assert_not_called()
        mock_update_service.assert_called_once_with(
            mock.sentinel.context,
            mock_Service.return_value.id,
            {"mapped_regions": mock.sentinel.mapped_regions}
        )
        mock_delete_service.assert_not_called()
        mock_get_service.assert_called_once_with(
            mock.sentinel.context,
            mock_Service.return_value.id
        )

        mock_add_service.reset_mock()
        mock_get_service.reset_mock()
        mock_update_service.reset_mock()
        mock_update_service.side_effect = CoriolisTestException()
        self.assertRaises(
            CoriolisTestException,
            self.server.register_service,
            mock.sentinel.context,
            mock.sentinel.host,
            mock.sentinel.binary,
            mock.sentinel.topic,
            mock.sentinel.enabled,
            mapped_regions=mock.sentinel.mapped_regions,
            providers=None,
            specs=None
        )
        self.assertEqual(
            (
                mock_Service.return_value.providers,
                mock_Service.return_value.specs
            ),
            (
                mock_get_service_status.return_value["providers"],
                mock_get_service_status.return_value["specs"]
            )
        )
        mock_add_service.assert_called_once_with(
            mock.sentinel.context,
            mock_Service.return_value
        )
        mock_get_service_status.assert_called_once_with(
            mock.sentinel.context)
        mock_update_service.assert_called_once_with(
            mock.sentinel.context,
            mock_Service.return_value.id,
            {"mapped_regions": mock.sentinel.mapped_regions}
        )
        mock_delete_service.assert_called_once_with(
            mock.sentinel.context,
            mock_Service.return_value.id
        )
        mock_get_service.assert_not_called()

    @mock.patch.object(db_api, "find_service")
    def test_check_service_registered(self, mock_find_service):
        result = self.server.check_service_registered(
            mock.sentinel.context,
            mock.sentinel.host,
            mock.sentinel.binary,
            mock.sentinel.topic
        )

        self.assertEqual(
            mock_find_service.return_value,
            result
        )
        mock_find_service.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.host,
            mock.sentinel.binary,
            topic=mock.sentinel.topic
        )

    @mock.patch.object(db_api, "find_service")
    def test_check_service_registered_no_service(self, mock_find_service):
        mock_find_service.return_value = None
        result = self.server.check_service_registered(
            mock.sentinel.context,
            mock.sentinel.host,
            mock.sentinel.binary,
            mock.sentinel.topic
        )

        self.assertEqual(
            None,
            result
        )
        mock_find_service.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.host,
            mock.sentinel.binary,
            topic=mock.sentinel.topic
        )

    @mock.patch.object(db_api, "update_service")
    @mock.patch.object(rpc_worker_client.WorkerClient, "get_service_status")
    @mock.patch.object(db_api, "get_service")
    def test_refresh_service_status(
        self,
        mock_get_service,
        mock_get_service_status,
        mock_update_service
    ):
        result = testutils.get_wrapped_function(
            self.server.refresh_service_status)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.service_id,
        )
        self.assertEqual(
            mock_get_service.return_value,
            result
        )
        mock_get_service.has_calls([
            mock.call(
                mock.sentinel.context,
                mock.sentinel.service_id
            ) * 2
        ])
        mock_get_service_status.assert_called_once_with(
            mock.sentinel.context)
        mock_update_service.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.service_id,
            {
                "providers": mock_get_service_status.return_value["providers"],
                "specs": mock_get_service_status.return_value["specs"],
                "status": constants.SERVICE_STATUS_UP
            }
        )

    @mock.patch.object(db_api, "get_services")
    def test_get_services(self, mock_get_services):
        result = self.server.get_services(mock.sentinel.context)

        self.assertEqual(
            mock_get_services.return_value,
            result
        )
        mock_get_services.assert_called_once_with(mock.sentinel.context)

    @mock.patch.object(db_api, "get_service")
    def test_get_service(self, mock_get_service):
        result = testutils.get_wrapped_function(
            self.server.get_service)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.service_id
        )

        self.assertEqual(
            mock_get_service.return_value,
            result
        )
        mock_get_service.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.service_id)

        mock_get_service.reset_mock()
        mock_get_service.return_value = None
        self.assertRaises(
            exception.NotFound,
            testutils.get_wrapped_function(self.server.get_service),
            self.server,
            mock.sentinel.context,
            mock.sentinel.service_id,
        )
        mock_get_service.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.service_id)

    @mock.patch.object(db_api, "delete_service")
    def test_delete_service(self, mock_delete_service):
        testutils.get_wrapped_function(self.server.delete_service)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.service_id
        )

        mock_delete_service.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.service_id)

    @mock.patch.object(db_api, "get_service")
    @mock.patch.object(db_api, "update_service")
    def test_update_service(
        self,
        mock_update_service,
        mock_get_service
    ):
        result = testutils.get_wrapped_function(
            self.server.update_service)(
            self.server,
            mock.sentinel.context,
            mock.sentinel.service_id,
            mock.sentinel.updated_values
        )

        self.assertEqual(
            mock_get_service.return_value,
            result
        )
        mock_update_service.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.service_id,
            mock.sentinel.updated_values
        )
        mock_get_service.assert_called_once_with(
            mock.sentinel.context,
            mock.sentinel.service_id
        )

    @mock.patch.object(cfg.CONF, "conductor")
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_check_delete_reservation_for_transfer"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_cancel_tasks_execution"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_set_tasks_execution_status"
    )
    @mock.patch.object(
        server.ConductorServerEndpoint,
        "_cancel_execution_for_osmorphing_debugging"
    )
    @mock.patch.object(lockutils, "lock")
    @mock.patch.object(db_api, "get_action")
    @mock.patch.object(db_api, "get_tasks_execution")
    @mock.patch.object(db_api, "set_task_status")
    @mock.patch.object(db_api, "get_task")
    def test_set_task_error_os_morphing(
            self,
            mock_get_task,
            mock_set_task_status,
            mock_get_tasks_execution,
            mock_get_action,
            mock_lock,
            mock_cancel_execution_for_osmorphing_debugging,
            mock_set_tasks_execution_status,
            mock_cancel_tasks_execution,
            mock_check_delete_reservation_for_transfer,
            mock_conf_conductor,
    ):
        execution = mock.Mock(
            type=constants.EXECUTION_TYPE_TRANSFER_UPDATE,
            action_id=mock.sentinel.action_id,
            tasks=[
                mock.Mock(
                    id=mock.sentinel.task_id,
                    status=constants.TASK_STATUS_COMPLETED,
                    task_type=constants.TASK_TYPE_OS_MORPHING,
                )
            ]
        )

        # non running tasks of type OS Morphing with debugging enabled
        mock_get_tasks_execution.return_value = execution
        mock_get_task.return_value = mock.Mock(
            task_type=constants.TASK_TYPE_OS_MORPHING,
            status=constants.TASK_STATUS_RUNNING,
        )

        self.server.set_task_error(
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.exception_details,
        )
        mock_cancel_execution_for_osmorphing_debugging.assert_called_once_with(
            mock.sentinel.context, mock_get_tasks_execution.return_value, )
        self.assertEqual(2, mock_set_task_status.call_count)
        mock_set_tasks_execution_status.assert_called_once_with(
            mock.sentinel.context,
            mock_get_tasks_execution.return_value,
            constants.EXECUTION_STATUS_CANCELED_FOR_DEBUGGING
        )
        mock_cancel_tasks_execution.assert_not_called()

        # non running tasks of type OS Morphing with debugging disabled
        mock_cancel_execution_for_osmorphing_debugging.reset_mock()
        mock_conf_conductor.debug_os_morphing_errors = False
        self.server.set_task_error(
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.exception_details,
        )
        mock_cancel_execution_for_osmorphing_debugging.assert_not_called()
        mock_cancel_tasks_execution.assert_called_once_with(
            mock.sentinel.context,
            mock_get_tasks_execution.return_value,
        )

        # deployment execution
        mock_check_delete_reservation_for_transfer.assert_not_called()
        execution.type = constants.EXECUTION_TYPE_DEPLOYMENT
        self.server.set_task_error(
            mock.sentinel.context,
            mock.sentinel.task_id,
            mock.sentinel.exception_details,
        )
