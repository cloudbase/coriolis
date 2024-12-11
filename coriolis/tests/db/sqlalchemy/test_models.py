# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.db.sqlalchemy import models
from coriolis.tests import test_base


class TaskEventTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy Task Event."""

    def test_to_dict(self):
        task_event = models.TaskEvent()
        task_event.id = mock.sentinel.id
        task_event.task_id = mock.sentinel.task_id
        task_event.level = mock.sentinel.level
        task_event.message = mock.sentinel.message
        task_event.index = mock.sentinel.index
        task_event.created_at = mock.sentinel.created_at
        task_event.updated_at = mock.sentinel.updated_at
        task_event.deleted_at = mock.sentinel.deleted_at
        task_event.deleted = mock.sentinel.deleted
        expected_result = {
            "id": mock.sentinel.id,
            "task_id": mock.sentinel.task_id,
            "level": mock.sentinel.level,
            "message": mock.sentinel.message,
            "index": mock.sentinel.index,
            "created_at": mock.sentinel.created_at,
            "updated_at": mock.sentinel.updated_at,
            "deleted_at": mock.sentinel.deleted_at,
            "deleted": mock.sentinel.deleted
        }

        result = task_event.to_dict()

        self.assertEqual(
            expected_result,
            result
        )


class MinionPoolEventTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy Minion Pool Event."""

    def test_to_dict(self):
        minion_pool_event = models.MinionPoolEvent()
        minion_pool_event.id = mock.sentinel.id
        minion_pool_event.pool_id = mock.sentinel.pool_id
        minion_pool_event.level = mock.sentinel.level
        minion_pool_event.message = mock.sentinel.message
        minion_pool_event.index = mock.sentinel.index
        minion_pool_event.created_at = mock.sentinel.created_at
        minion_pool_event.updated_at = mock.sentinel.updated_at
        minion_pool_event.deleted_at = mock.sentinel.deleted_at
        minion_pool_event.deleted = mock.sentinel.deleted
        expected_result = {
            "id": mock.sentinel.id,
            "pool_id": mock.sentinel.pool_id,
            "level": mock.sentinel.level,
            "message": mock.sentinel.message,
            "index": mock.sentinel.index,
            "created_at": mock.sentinel.created_at,
            "updated_at": mock.sentinel.updated_at,
            "deleted_at": mock.sentinel.deleted_at,
            "deleted": mock.sentinel.deleted
        }

        result = minion_pool_event.to_dict()

        self.assertEqual(
            expected_result,
            result
        )


class TaskProgressUpdateTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy Task Progress Update."""

    def test_to_dict(self):
        task_progress_update = models.TaskProgressUpdate()
        task_progress_update.id = mock.sentinel.id
        task_progress_update.task_id = mock.sentinel.task_id
        task_progress_update.index = mock.sentinel.index
        task_progress_update.current_step = mock.sentinel.current_step
        task_progress_update.total_steps = mock.sentinel.total_steps
        task_progress_update.message = mock.sentinel.message
        task_progress_update.created_at = mock.sentinel.created_at
        task_progress_update.updated_at = mock.sentinel.updated_at
        task_progress_update.deleted_at = mock.sentinel.deleted_at
        task_progress_update.deleted = mock.sentinel.deleted
        expected_result = {
            "id": mock.sentinel.id,
            "task_id": mock.sentinel.task_id,
            "index": mock.sentinel.index,
            "current_step": mock.sentinel.current_step,
            "total_steps": mock.sentinel.total_steps,
            "message": mock.sentinel.message,
            "created_at": mock.sentinel.created_at,
            "updated_at": mock.sentinel.updated_at,
            "deleted_at": mock.sentinel.deleted_at,
            "deleted": mock.sentinel.deleted
        }

        result = task_progress_update.to_dict()

        self.assertEqual(
            expected_result,
            result
        )


class MinionPoolProgressUpdateTestCase(test_base.CoriolisBaseTestCase):
    """
    Test suite for the Coriolis Database Sqlalchemy Minion Pool Progress
    Update.
    """

    def test_to_dict(self):
        minion_pool_progress_update = models.MinionPoolProgressUpdate()
        minion_pool_progress_update.id = mock.sentinel.id
        minion_pool_progress_update.pool_id = mock.sentinel.pool_id
        minion_pool_progress_update.current_step = mock.sentinel.current_step
        minion_pool_progress_update.total_steps = mock.sentinel.total_steps
        minion_pool_progress_update.message = mock.sentinel.message
        minion_pool_progress_update.index = mock.sentinel.index
        minion_pool_progress_update.created_at = mock.sentinel.created_at
        minion_pool_progress_update.updated_at = mock.sentinel.updated_at
        minion_pool_progress_update.deleted_at = mock.sentinel.deleted_at
        minion_pool_progress_update.deleted = mock.sentinel.deleted
        expected_result = {
            "id": mock.sentinel.id,
            "pool_id": mock.sentinel.pool_id,
            "current_step": mock.sentinel.current_step,
            "total_steps": mock.sentinel.total_steps,
            "message": mock.sentinel.message,
            "index": mock.sentinel.index,
            "created_at": mock.sentinel.created_at,
            "updated_at": mock.sentinel.updated_at,
            "deleted_at": mock.sentinel.deleted_at,
            "deleted": mock.sentinel.deleted
        }

        result = minion_pool_progress_update.to_dict()

        self.assertEqual(
            expected_result,
            result
        )


class TaskTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy Task."""

    def test_to_dict(self):
        task = models.Task()
        mock_event1 = models.TaskEvent()
        mock_event2 = models.TaskEvent()
        mock_progress_update1 = models.TaskProgressUpdate()
        mock_progress_update2 = models.TaskProgressUpdate()
        task.id = mock.sentinel.id
        task.execution_id = mock.sentinel.execution_id
        task.instance = mock.sentinel.instance
        task.host = mock.sentinel.host
        task.process_id = mock.sentinel.process_id
        task.status = mock.sentinel.status
        task.task_type = mock.sentinel.task_type
        task.exception_details = mock.sentinel.exception_details
        task.depends_on = mock.sentinel.depends_on
        task.index = mock.sentinel.index
        task.on_error = mock.sentinel.on_error
        task.events = [mock_event1, mock_event2]
        task.progress_updates = [mock_progress_update1, mock_progress_update2]
        task.created_at = mock.sentinel.created_at
        task.updated_at = mock.sentinel.updated_at
        task.deleted_at = mock.sentinel.deleted_at
        task.deleted = mock.sentinel.deleted
        expected_result = {
            "id": mock.sentinel.id,
            "execution_id": mock.sentinel.execution_id,
            "instance": mock.sentinel.instance,
            "host": mock.sentinel.host,
            "process_id": mock.sentinel.process_id,
            "status": mock.sentinel.status,
            "task_type": mock.sentinel.task_type,
            "exception_details": mock.sentinel.exception_details,
            "depends_on": mock.sentinel.depends_on,
            "index": mock.sentinel.index,
            "on_error": mock.sentinel.on_error,
            "events": [mock_event1.to_dict(), mock_event2.to_dict()],
            "progress_updates": [
                mock_progress_update1.to_dict(),
                mock_progress_update2.to_dict()],
            "created_at": mock.sentinel.created_at,
            "updated_at": mock.sentinel.updated_at,
            "deleted_at": mock.sentinel.deleted_at,
            "deleted": mock.sentinel.deleted
        }

        result = task.to_dict()

        self.assertEqual(
            expected_result,
            result
        )


class TasksExecutionTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy Tasks Execution."""

    def test_to_dict(self):
        tasks_execution = models.TasksExecution()
        mock_task1 = models.Task()
        mock_task2 = models.Task()
        tasks_execution.id = mock.sentinel.id
        tasks_execution.action_id = mock.sentinel.action_id
        tasks_execution.tasks = [mock_task1, mock_task2]
        tasks_execution.status = mock.sentinel.status
        tasks_execution.number = mock.sentinel.number
        tasks_execution.type = mock.sentinel.type
        tasks_execution.created_at = mock.sentinel.created_at
        tasks_execution.updated_at = mock.sentinel.updated_at
        tasks_execution.deleted_at = mock.sentinel.deleted_at
        tasks_execution.deleted = mock.sentinel.deleted
        expected_result = {
            "id": mock.sentinel.id,
            "action_id": mock.sentinel.action_id,
            "tasks": [mock_task1.to_dict(), mock_task2.to_dict()],
            "status": mock.sentinel.status,
            "number": mock.sentinel.number,
            "type": mock.sentinel.type,
            "created_at": mock.sentinel.created_at,
            "updated_at": mock.sentinel.updated_at,
            "deleted_at": mock.sentinel.deleted_at,
            "deleted": mock.sentinel.deleted
        }

        result = tasks_execution.to_dict()

        self.assertEqual(
            expected_result,
            result
        )


class BaseTransferActionTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy Base Transfer Action."""

    def test_to_dict(self):
        transfer = models.BaseTransferAction()
        mock_execution1 = models.TasksExecution()
        mock_execution2 = models.TasksExecution()
        transfer.base_id = mock.sentinel.base_id
        transfer.user_id = mock.sentinel.user_id
        transfer.project_id = mock.sentinel.project_id
        transfer.destination_environment = \
            mock.sentinel.destination_environment
        transfer.type = mock.sentinel.type
        transfer.executions = [mock_execution1, mock_execution2]
        transfer.instances = mock.sentinel.instances
        transfer.reservation_id = mock.sentinel.reservation_id
        transfer.notes = mock.sentinel.notes
        transfer.origin_endpoint_id = mock.sentinel.origin_endpoint_id
        transfer.destination_endpoint_id = \
            mock.sentinel.destination_endpoint_id
        transfer.transfer_result = mock.sentinel.transfer_result
        transfer.network_map = mock.sentinel.network_map
        transfer.storage_mappings = mock.sentinel.storage_mappings
        transfer.source_environment = mock.sentinel.source_environment
        transfer.last_execution_status = mock.sentinel.last_execution_status
        transfer.created_at = mock.sentinel.created_at
        transfer.updated_at = mock.sentinel.updated_at
        transfer.deleted_at = mock.sentinel.deleted_at
        transfer.deleted = mock.sentinel.deleted
        transfer.origin_minion_pool_id = mock.sentinel.origin_minion_pool_id
        transfer.deleted = mock.sentinel.deleted
        transfer.destination_minion_pool_id = \
            mock.sentinel.destination_minion_pool_id
        transfer.instance_osmorphing_minion_pool_mappings = \
            mock.sentinel.instance_osmorphing_minion_pool_mappings
        transfer.user_scripts = mock.sentinel.user_scripts
        transfer.info = mock.sentinel.info
        expected_result = {
            "base_id": mock.sentinel.base_id,
            "user_id": mock.sentinel.user_id,
            "project_id": mock.sentinel.project_id,
            "destination_environment": mock.sentinel.destination_environment,
            "type": mock.sentinel.type,
            "executions": [
                mock_execution1.to_dict(), mock_execution2.to_dict()],
            "instances": mock.sentinel.instances,
            "reservation_id": mock.sentinel.reservation_id,
            "notes": mock.sentinel.notes,
            "origin_endpoint_id": mock.sentinel.origin_endpoint_id,
            "destination_endpoint_id": mock.sentinel.destination_endpoint_id,
            "transfer_result": mock.sentinel.transfer_result,
            "network_map": mock.sentinel.network_map,
            "storage_mappings": mock.sentinel.storage_mappings,
            "source_environment": mock.sentinel.source_environment,
            "last_execution_status": mock.sentinel.last_execution_status,
            "created_at": mock.sentinel.created_at,
            "updated_at": mock.sentinel.updated_at,
            "deleted_at": mock.sentinel.deleted_at,
            "deleted": mock.sentinel.deleted,
            "origin_minion_pool_id": mock.sentinel.origin_minion_pool_id,
            "destination_minion_pool_id":
                mock.sentinel.destination_minion_pool_id,
            "instance_osmorphing_minion_pool_mappings":
                mock.sentinel.instance_osmorphing_minion_pool_mappings,
            "user_scripts": mock.sentinel.user_scripts,
            "info": mock.sentinel.info
        }

        result = transfer.to_dict()

        self.assertEqual(
            expected_result,
            result
        )


class ReplicaTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy Replica."""

    def test_to_dict(self):
        transfer = models.Transfer()
        transfer.id = mock.sentinel.id

        result = transfer.to_dict()

        self.assertEqual(
            mock.sentinel.id,
            result["id"]
        )


class DeploymentTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy Deployment."""

    def test_to_dict(self):
        transfer = mock.MagicMock(id=mock.sentinel.transfer_id)
        deployment = models.Deployment()
        deployment.id = mock.sentinel.id
        deployment.transfer_id = transfer.id
        deployment.transfer = transfer
        expected_result = {
            "id": mock.sentinel.id,
            "transfer_id": mock.sentinel.transfer_id,
            "transfer_scenario_type": transfer.scenario,
        }

        result = deployment.to_dict()

        assert all(item in result.items() for item in expected_result.items())


class MinionMachineTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy Minion Machine."""

    def test_to_dict(self):
        minion = models.MinionMachine()
        minion.id = mock.sentinel.id
        minion.user_id = mock.sentinel.user_id
        minion.project_id = mock.sentinel.project_id
        minion.created_at = mock.sentinel.created_at
        minion.updated_at = mock.sentinel.updated_at
        minion.deleted_at = mock.sentinel.deleted_at
        minion.deleted = mock.sentinel.deleted
        minion.pool_id = mock.sentinel.pool_id
        minion.allocation_status = mock.sentinel.allocation_status
        minion.power_status = mock.sentinel.power_status
        minion.connection_info = mock.sentinel.connection_info
        minion.allocated_action = mock.sentinel.allocated_action
        minion.last_used_at = mock.sentinel.last_used_at
        minion.backup_writer_connection_info = \
            mock.sentinel.backup_writer_connection_info
        minion.provider_properties = mock.sentinel.provider_properties
        expected_result = {
            "id": mock.sentinel.id,
            "user_id": mock.sentinel.user_id,
            "project_id": mock.sentinel.project_id,
            "created_at": mock.sentinel.created_at,
            "updated_at": mock.sentinel.updated_at,
            "deleted_at": mock.sentinel.deleted_at,
            "deleted": mock.sentinel.deleted,
            "pool_id": mock.sentinel.pool_id,
            "allocation_status": mock.sentinel.allocation_status,
            "power_status": mock.sentinel.power_status,
            "connection_info": mock.sentinel.connection_info,
            "allocated_action": mock.sentinel.allocated_action,
            "last_used_at": mock.sentinel.last_used_at,
            "backup_writer_connection_info":
                mock.sentinel.backup_writer_connection_info,
            "provider_properties": mock.sentinel.provider_properties
        }

        result = minion.to_dict()

        self.assertEqual(
            expected_result,
            result
        )


class MinionPoolTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Database Sqlalchemy Minion Pool."""

    def test_to_dict(self):
        minion = models.MinionPool()
        mock_minion_machine1 = models.MinionMachine()
        mock_minion_machine2 = models.MinionMachine()
        mock_event1 = models.MinionPoolEvent()
        mock_event2 = models.MinionPoolEvent()
        mock_progress_update1 = models.MinionPoolProgressUpdate()
        mock_progress_update2 = models.MinionPoolProgressUpdate()
        minion.minion_machines = [mock_minion_machine1, mock_minion_machine2]
        minion.events = [mock_event1, mock_event2]
        minion.progress_updates = [
            mock_progress_update1, mock_progress_update2]
        minion.id = mock.sentinel.id
        minion.name = mock.sentinel.name
        minion.notes = mock.sentinel.notes
        minion.endpoint_id = mock.sentinel.endpoint_id
        minion.environment_options = mock.sentinel.environment_options
        minion.os_type = mock.sentinel.os_type
        minion.maintenance_trust_id = mock.sentinel.maintenance_trust_id
        minion.platform = mock.sentinel.platform
        minion.created_at = mock.sentinel.created_at
        minion.updated_at = mock.sentinel.updated_at
        minion.deleted_at = mock.sentinel.deleted_at
        minion.deleted = mock.sentinel.deleted
        minion.shared_resources = mock.sentinel.shared_resources
        minion.status = mock.sentinel.status
        minion.minimum_minions = mock.sentinel.minimum_minions
        minion.maximum_minions = mock.sentinel.maximum_minions
        minion.minion_max_idle_time = mock.sentinel.minion_max_idle_time
        minion.minion_retention_strategy = \
            mock.sentinel.minion_retention_strategy
        expected_result = {
            "id": mock.sentinel.id,
            "name": mock.sentinel.name,
            "notes": mock.sentinel.notes,
            "endpoint_id": mock.sentinel.endpoint_id,
            "environment_options": mock.sentinel.environment_options,
            "os_type": mock.sentinel.os_type,
            "maintenance_trust_id": mock.sentinel.maintenance_trust_id,
            "platform": mock.sentinel.platform,
            "created_at": mock.sentinel.created_at,
            "updated_at": mock.sentinel.updated_at,
            "deleted_at": mock.sentinel.deleted_at,
            "deleted": mock.sentinel.deleted,
            "shared_resources": mock.sentinel.shared_resources,
            "status": mock.sentinel.status,
            "minimum_minions": mock.sentinel.minimum_minions,
            "maximum_minions": mock.sentinel.maximum_minions,
            "minion_max_idle_time": mock.sentinel.minion_max_idle_time,
            "minion_retention_strategy":
                mock.sentinel.minion_retention_strategy,
            "minion_machines": [
                mock_minion_machine1.to_dict(),
                mock_minion_machine2.to_dict()],
            "events": [
                mock_event1.to_dict(),
                mock_event2.to_dict()],
            "progress_updates": [
                mock_progress_update1.to_dict(),
                mock_progress_update2.to_dict()],
        }

        result = minion.to_dict()

        self.assertEqual(
            expected_result,
            result
        )
