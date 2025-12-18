# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.conductor.rpc import client
from coriolis import constants
from coriolis.tests import test_base

INSTANCE_ARGS = {
    "transfer_scenario": "mock_transfer_scenario",
    "origin_endpoint_id": "mock_origin_endpoint_id",
    "destination_endpoint_id": "mock_destination_endpoint_id",
    "origin_minion_pool_id": "mock_origin_minion_pool_id",
    "destination_minion_pool_id": "mock_destination_minion_pool_id",
    "instance_osmorphing_minion_pool_mappings": {
        "mock_instance_1": "mock_pool_1",
        "mock_instance_2": "mock_pool_2",
    },
    "destination_environment": {
        "storage_mappings": {'mock_destination_key': 'mock_destination_value'}
    },
    "instances": ['mock_instance_1', 'mock_instance_2'],
    "notes": "mock_notes",
    "network_map": {'mock_network_key': 'mock_network_value'},
    "storage_mappings": {
        'mock_destination_key': 'mock_destination_value'
    },
    "source_environment": {'mock_source_key': 'mock_source_value'},
    "user_scripts": {'mock_scripts_key': 'mock_scripts_value'},
}


class ConductorClientTestCase(test_base.CoriolisRPCClientTestCase):

    def setUp(self):
        super(ConductorClientTestCase, self).setUp()
        self.client = client.ConductorClient()

    def test_create_endpoint(self):
        args = {
            "name": "mock_name",
            "endpoint_type": "mock_endpoint_type",
            "description": "mock_description",
            "connection_info": "mock_connection_info",
            "mapped_regions": "mock_mapped_regions"
        }
        self._test(self.client.create_endpoint, args)

    def test_update_endpoint(self):
        args = {
            "endpoint_id": "mock_endpoint_id",
            "updated_values": "mock_updated_values",
        }
        self._test(self.client.update_endpoint, args)

    def test_get_endpoints(self):
        args = {}
        self._test(self.client.get_endpoints, args)

    def test_get_endpoint(self):
        args = {
            "endpoint_id": "mock_endpoint_id",
        }
        self._test(self.client.get_endpoint, args)

    def test_delete_endpoint(self):
        args = {
            "endpoint_id": "mock_endpoint_id",
        }
        self._test(self.client.delete_endpoint, args)

    def test_get_endpoint_instances(self):
        args = {
            "endpoint_id": "mock_endpoint_id",
            "source_environment": "mock_source_environment",
            "marker": None,
            "limit": None,
            "instance_name_pattern": None,
            "refresh": False
        }
        self._test(self.client.get_endpoint_instances, args)

    def test_get_endpoint_instance(self):
        args = {
            "endpoint_id": "mock_endpoint_id",
            "source_environment": "mock_source_environment",
            "instance_name": "mock_instance_name"
        }
        self._test(self.client.get_endpoint_instance, args)

    def test_get_endpoint_source_options(self):
        args = {
            "endpoint_id": "mock_endpoint_id",
            "env": "mock_env",
            "option_names": "mock_option_names"
        }
        self._test(self.client.get_endpoint_source_options, args)

    def test_get_endpoint_destination_options(self):
        args = {
            "endpoint_id": "mock_endpoint_id",
            "env": "mock_env",
            "option_names": "mock_option_names"
        }
        self._test(self.client.get_endpoint_destination_options, args)

    def test_get_endpoint_networks(self):
        args = {
            "endpoint_id": "mock_endpoint_id",
            "env": "mock_env"
        }
        self._test(self.client.get_endpoint_networks, args)

    def test_get_endpoint_storage(self):
        args = {
            "endpoint_id": "mock_endpoint_id",
            "env": "mock_env"
        }
        self._test(self.client.get_endpoint_storage, args)

    def test_validate_endpoint_connection(self):
        args = {
            "endpoint_id": "mock_endpoint_id"
        }
        self._test(self.client.validate_endpoint_connection, args)

    def test_validate_endpoint_target_environment(self):
        args = {
            "endpoint_id": "mock_endpoint_id",
            "target_env": "mock_target_env"
        }
        self._test(self.client.validate_endpoint_target_environment, args)

    def test_validate_endpoint_source_environment(self):
        args = {
            "endpoint_id": "mock_endpoint_id",
            "source_env": "mock_source_env"
        }
        self._test(self.client.validate_endpoint_source_environment, args)

    def test_get_available_providers(self):
        args = {}
        self._test(self.client.get_available_providers, args)

    def test_get_provider_schemas(self):
        args = {
            "platform_name": "mock_platform_name",
            "provider_type": "mock_provider_type"
        }
        self._test(self.client.get_provider_schemas, args)

    def test_execute_transfer_tasks(self):
        args = {
            "transfer_id": "mock_transfer_id",
            "shutdown_instances": False,
            "auto_deploy": False,
        }
        self._test(self.client.execute_transfer_tasks, args)

    def test_get_transfer_tasks_executions(self):
        args = {
            "transfer_id": "mock_transfer_id",
            "include_tasks": False
        }
        self._test(self.client.get_transfer_tasks_executions, args)

    def test_get_transfer_tasks_execution(self):
        args = {
            "transfer_id": "mock_transfer_id",
            "execution_id": "mock_execution_id",
            "include_task_info": False
        }
        self._test(self.client.get_transfer_tasks_execution, args)

    def test_delete_transfer_tasks_execution(self):
        args = {
            "transfer_id": "mock_transfer_id",
            "execution_id": "mock_execution_id"
        }
        self._test(self.client.delete_transfer_tasks_execution, args)

    def test_cancel_transfer_tasks_execution(self):
        args = {
            "transfer_id": "mock_transfer_id",
            "execution_id": "mock_execution_id",
            "force": "mock_force"
        }
        self._test(self.client.cancel_transfer_tasks_execution, args)

    def test_create_instances_transfer(self):
        args = {
            **INSTANCE_ARGS,
        }
        new_args = {
            "notes": None,
            "user_scripts": None,
            "clone_disks": True,
            "skip_os_morphing": False,
        }
        args.update(new_args)
        self._test(self.client.create_instances_transfer, args)

    def test_get_transfers(self):
        args = {
            "include_tasks_executions": False,
            "include_task_info": False,
        }
        self._test(self.client.get_transfers, args)

    def test_get_transfer(self):
        args = {
            "transfer_id": "mock_transfer_id",
            "include_task_info": False,
        }
        self._test(self.client.get_transfer, args)

    def test_delete_transfer(self):
        args = {
            "transfer_id": "mock_transfer_id",
        }
        self._test(self.client.delete_transfer, args)

    def test_delete_transfer_disks(self):
        args = {
            "transfer_id": "mock_transfer_id"
        }
        self._test(self.client.delete_transfer_disks, args)

    def test_deploy_transfer_instances(self):
        args = {
            "transfer_id": "mock_transfer_id",
            "instance_osmorphing_minion_pool_mappings": None,
            "clone_disks": False,
            "force": False,
            "skip_os_morphing": False,
            "user_scripts": None,
            "wait_for_execution": None,
            "trust_id": None,
        }
        self._test(self.client.deploy_transfer_instances, args)

    def test_set_task_host(self):
        args = {
            "task_id": "mock_task_id",
            "host": "mock_host"
        }
        self._test(self.client.set_task_host, args)

    def test_set_task_process(self):
        args = {
            "task_id": "mock_task_id",
            "process_id": "mock_process_id"
        }
        self._test(self.client.set_task_process, args)

    def test_task_completed(self):
        args = {
            "task_id": "mock_task_id",
            "task_result": "mock_task_result"
        }
        self._test(self.client.task_completed, args)

    def test_confirm_task_cancellation(self):
        args = {
            "task_id": "mock_task_id",
            "cancellation_details": "mock_cancellation_details"
        }
        self._test(self.client.confirm_task_cancellation, args)

    def test_set_task_error(self):
        args = {
            "task_id": "mock_task_id",
            "exception_details": "mock_exception_details"
        }
        self._test(self.client.set_task_error, args)

    def test_add_task_event(self):
        args = {
            "task_id": "mock_task_id",
            "level": "mock_level",
            "message": "mock_message"
        }
        self._test(self.client.add_task_event, args, rpc_op='_cast')

    def test_update_task_progress_update(self):
        args = {
            "task_id": "mock_task_id",
            "progress_update_index": "mock_progress_update_index",
            "new_current_step": "mock_new_current_step",
            "new_total_steps": None,
            "new_message": None
        }
        self._test(self.client.update_task_progress_update, args,
                   rpc_op='_cast')

    def test_create_transfer_schedule(self):
        args = {
            "transfer_id": "mock_transfer_id",
            "schedule": "mock_schedule",
            "enabled": "mock_enabled",
            "exp_date": "mock_exp_date",
            "shutdown_instance": "mock_shutdown_instance",
            "auto_deploy": False,
        }
        self._test(self.client.create_transfer_schedule, args)

    def test_update_transfer_schedule(self):
        args = {
            "transfer_id": "mock_transfer_id",
            "schedule_id": "mock_schedule_id",
            "updated_values": "mock_updated_values"
        }
        self._test(self.client.update_transfer_schedule, args)

    def test_delete_transfer_schedule(self):
        args = {
            "transfer_id": "mock_transfer_id",
            "schedule_id": "mock_schedule_id"
        }
        self._test(self.client.delete_transfer_schedule, args)

    def test_get_transfer_schedules(self):
        args = {
            "transfer_id": None,
            "expired": True
        }
        self._test(self.client.get_transfer_schedules, args)

    def test_get_transfer_schedule(self):
        args = {
            "transfer_id": "mock_transfer_id",
            "schedule_id": "mock_schedule_id",
            "expired": True
        }
        self._test(self.client.get_transfer_schedule, args)

    def test_update_transfer(self):
        args = {
            "transfer_id": "mock_transfer_id",
            "updated_properties": "mock_updated_properties"
        }
        self._test(self.client.update_transfer, args)

    def test_get_diagnostics(self):
        self._test(self.client.get_diagnostics, args={})

    def test_get_all_diagnostics(self):
        self._test(self.client.get_all_diagnostics, args={})

    def test_create_region(self):
        args = {
            "region_name": "mock_region_name",
            "description": "mock_description",
            "enabled": True
        }
        self._test(self.client.create_region, args)

    def test_get_regions(self):
        self._test(self.client.get_regions, args={})

    def test_get_region(self):
        args = {
            "region_id": "mock_region_id"
        }
        self._test(self.client.get_region, args)

    def test_update_region(self):
        args = {
            "region_id": "mock_region_id",
            "updated_values": "mock_updated_values"
        }
        self._test(self.client.update_region, args)

    def test_delete_region(self):
        args = {
            "region_id": "mock_region_id"
        }
        self._test(self.client.delete_region, args)

    def test_register_service(self):
        args = {
            "host": "mock_host",
            "binary": "mock_binary",
            "topic": "mock_topic",
            "enabled": "mock_enabled",
            "mapped_regions": "mock_mapped_regions",
            "providers": None,
            "specs": None
        }
        self._test(self.client.register_service, args)

    def test_check_service_registered(self):
        args = {
            "host": "mock_host",
            "binary": "mock_binary",
            "topic": "mock_topic"
        }
        self._test(self.client.check_service_registered, args)

    def test_refresh_service_status(self):
        args = {
            "service_id": "mock_service_id"
        }
        self._test(self.client.refresh_service_status, args)

    def test_get_services(self):
        self._test(self.client.get_services, args={})

    def test_get_service(self):
        args = {
            "service_id": "mock_service_id"
        }
        self._test(self.client.get_service, args)

    def test_update_service(self):
        args = {
            "service_id": "mock_service_id",
            "updated_values": "mock_updated_values"
        }
        self._test(self.client.update_service, args)

    def test_delete_service(self):
        args = {
            "service_id": "mock_service_id"
        }
        self._test(self.client.delete_service, args)

    def test_confirm_transfer_minions_allocation(self):
        args = {
            "transfer_id": "mock_transfer_id",
            "minion_machine_allocations": "mock_minion_machine_allocations"
        }
        self._test(self.client.confirm_transfer_minions_allocation, args)

    def test_report_transfer_minions_allocation_error(self):
        args = {
            "transfer_id": "mock_transfer_id",
            "minion_allocation_error_details":
                "mock_minion_allocation_error_details"
        }
        self._test(self.client.report_transfer_minions_allocation_error, args)

    def test_confirm_deployment_minions_allocation(self):
        args = {
            "deployment_id": "mock_deployment_id",
            "minion_machine_allocations": "mock_minion_machine_allocations"
        }
        self._test(self.client.confirm_deployment_minions_allocation, args)

    def test_report_deployment_minions_allocation_error(self):
        args = {
            "deployment_id": "mock_deployment_id",
            "minion_allocation_error_details":
                "mock_minion_allocation_error_details"
        }
        self._test(
            self.client.report_deployment_minions_allocation_error, args)

    def test_add_task_progress_update(self):
        args = {
            "task_id": "mock_task_id",
            "message": "mock_message",
            "initial_step": "mock_message",
            "total_steps": "mock_message"
        }
        ctxt = mock.sentinel.ctxt

        with mock.patch.object(self.client, '_cast') as op_mock:
            self.client.add_task_progress_update(
                ctxt, return_event=False, **args)
            op_mock.assert_called_once_with(
                ctxt, 'add_task_progress_update', **args)

        with mock.patch.object(self.client, '_call') as op_mock:
            self.client.add_task_progress_update(
                ctxt, return_event=True, **args)
            op_mock.assert_called_once_with(
                ctxt, 'add_task_progress_update', **args)


class ConductorTaskRpcEventHandlerTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(ConductorTaskRpcEventHandlerTestCase, self).setUp()
        self.ctxt = mock.Mock()
        self.task_id = mock.sentinel.task_id
        self.client = client.ConductorTaskRpcEventHandler(
            self.ctxt, self.task_id)

    def test_get_progress_update_identifier(self):
        self.assertEqual(
            mock.sentinel.index,
            client.ConductorTaskRpcEventHandler.get_progress_update_identifier(
                {'index': mock.sentinel.index})
        )

    @mock.patch.object(client.ConductorClient, 'add_task_progress_update')
    def test_add_progress_update(self, mock_add_task_progress_update):
        message = mock.sentinel.message
        initial_step = mock.sentinel.initial_step
        total_steps = mock.sentinel.total_steps
        return_event = False
        result = self.client.add_progress_update(
            message, initial_step, total_steps, return_event)

        self.assertEqual(
            mock_add_task_progress_update.return_value,
            result
        )
        mock_add_task_progress_update.assert_called_once_with(
            self.ctxt,
            self.task_id,
            message,
            initial_step=initial_step,
            total_steps=total_steps,
            return_event=return_event
        )

    @mock.patch.object(client.ConductorClient, 'update_task_progress_update')
    def test_update_progress_update(self, mock_update_task_progress_update):
        update_identifier = mock.sentinel.update_identifier
        new_current_step = mock.sentinel.new_current_step
        new_total_steps = None
        new_message = None

        self.client.update_progress_update(
            update_identifier, new_current_step, new_total_steps, new_message)

        mock_update_task_progress_update.assert_called_once_with(
            self.ctxt,
            self.task_id,
            update_identifier,
            new_current_step,
            new_total_steps=new_total_steps,
            new_message=new_message
        )

    @mock.patch.object(client.ConductorClient, 'add_task_event')
    def test_add_event(self, mock_add_task_event):
        level = constants.TASK_EVENT_INFO
        message = mock.sentinel.message

        self.client.add_event(message, level)

        mock_add_task_event.assert_called_once_with(
            self.ctxt,
            self.task_id,
            level,
            message
        )
