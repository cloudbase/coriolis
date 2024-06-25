# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

from taskflow.types import failure

from coriolis.conductor.rpc.client import ConductorClient
from coriolis import exception
from coriolis.minion_manager.rpc.client import MinionManagerClient
from coriolis.minion_manager.rpc import tasks
from coriolis.taskflow import base
from coriolis.tests import test_base


class CoriolisTestException(Exception):
    pass


class MinionManagerTaskEventMixinTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis MinionManagerTaskEventMixin class."""

    def setUp(self):
        super(MinionManagerTaskEventMixinTestCase, self).setUp()
        self.task = tasks.MinionManagerTaskEventMixin()
        self.task._minion_pool_id = mock.sentinel.minion_pool_id

    def test__conductor_client(self):
        with mock.patch(
            'coriolis.conductor.rpc.client.ConductorClient',
                return_value=mock.MagicMock(spec=ConductorClient)) as \
                mock_Conductor_client:
            result = self.task._conductor_client

            self.assertEqual(
                result, mock_Conductor_client.return_value
            )
            mock_Conductor_client.assert_called_once_with()

    def test__conductor_client_already_set(self):
        with mock.patch(
            'coriolis.conductor.rpc.client.ConductorClient') as \
                mock_Conductor_client:
            self.task._conductor_client_instance = mock.MagicMock(
                spec=ConductorClient)

            result = self.task._conductor_client

            self.assertEqual(
                result, self.task._conductor_client_instance
            )
            mock_Conductor_client.assert_not_called()

    def test__minion_manager_client(self):
        with mock.patch(
            'coriolis.minion_manager.rpc.client.MinionManagerClient',
                return_value=mock.MagicMock(spec=MinionManagerClient)) as \
                mock_Minion_Manager_Client:
            result = self.task._minion_manager_client

            self.assertEqual(
                result, mock_Minion_Manager_Client.return_value
            )
            mock_Minion_Manager_Client.assert_called_once_with()

    def test__minion_manager_client_already_set(self):
        with mock.patch(
            'coriolis.minion_manager.rpc.client.MinionManagerClient') as \
                mock_Minion_Manager_Client:
            self.task._minion_manager_client_instance = mock.MagicMock(
                spec=MinionManagerClient)

            result = self.task._minion_manager_client

            self.assertEqual(
                result, self.task._minion_manager_client_instance
            )
            mock_Minion_Manager_Client.assert_not_called()

    @mock.patch.object(tasks.db_api, 'add_minion_pool_event')
    def test__add_minion_pool_event(self, mock_add_minion_pool_event):
        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
            result = self.task._add_minion_pool_event(
                mock.sentinel.context, mock.sentinel.message,
                level=tasks.constants.TASK_EVENT_INFO)
            self.assertIsNone(result)

        mock_add_minion_pool_event.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.minion_pool_id,
            tasks.constants.TASK_EVENT_INFO, mock.sentinel.message
        )

    @mock.patch.object(tasks.db_api, 'get_minion_machine')
    def test__get_minion_machine(self, mock_get_minion_machine):
        result = self.task._get_minion_machine(
            mock.sentinel.context, mock.sentinel.minion_machine_id)
        self.assertEqual(result, mock_get_minion_machine.return_value)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.minion_machine_id)

    @mock.patch.object(tasks.db_api, 'get_minion_machine')
    def test__get_minion_machine_not_found(self, mock_get_minion_machine):
        mock_get_minion_machine.return_value = None

        self.assertRaises(
            tasks.exception.NotFound, self.task._get_minion_machine,
            mock.sentinel.context, mock.sentinel.minion_machine_id,
            raise_if_not_found=True)

    @mock.patch.object(tasks.db_api, 'set_minion_pool_status')
    def test__set_minion_pool_status(self, mock_set_minion_pool_status):
        with mock.patch.object(
            tasks.minion_manager_utils, 'get_minion_pool_lock') as \
                mock_get_minion_pool_lock:
            result = self.task._set_minion_pool_status(
                mock.sentinel.context, mock.sentinel.minion_pool_id,
                mock.sentinel.new_status)
            self.assertIsNone(result)

            mock_get_minion_pool_lock.assert_called_once_with(
                mock.sentinel.minion_pool_id, external=True)

            mock_set_minion_pool_status.assert_called_once_with(
                mock.sentinel.context, mock.sentinel.minion_pool_id,
                mock.sentinel.new_status)

    @mock.patch.object(tasks.db_api, 'update_minion_machine')
    def test__update_minion_machine(self, mock_update_minion_machine):
        with mock.patch.object(
            tasks.minion_manager_utils, 'get_minion_pool_lock') as \
                mock_get_minion_pool_lock:
            result = self.task._update_minion_machine(
                mock.sentinel.ctxt, mock.sentinel.minion_pool_id,
                mock.sentinel.minion_machine_id, mock.sentinel.updated_values)
            self.assertIsNone(result)

            mock_get_minion_pool_lock.assert_called_once_with(
                mock.sentinel.minion_pool_id, external=True)

            mock_update_minion_machine.assert_called_once_with(
                mock.sentinel.ctxt, mock.sentinel.minion_machine_id,
                mock.sentinel.updated_values)

    @mock.patch.object(tasks.db_api, 'set_minion_machine_allocation_status')
    def test__set_minion_machine_allocation_status(
            self, mock_set_minion_machine_allocation_status):
        with mock.patch.object(
            tasks.minion_manager_utils, 'get_minion_pool_lock') as \
                mock_get_minion_pool_lock:
            result = self.task._set_minion_machine_allocation_status(
                mock.sentinel.ctxt, mock.sentinel.minion_pool_id,
                mock.sentinel.minion_machine_id, mock.sentinel.new_status)
            self.assertIsNone(result)

            mock_get_minion_pool_lock.assert_called_once_with(
                mock.sentinel.minion_pool_id, external=True)

            mock_set_minion_machine_allocation_status.assert_called_once_with(
                mock.sentinel.ctxt, mock.sentinel.minion_machine_id,
                mock.sentinel.new_status)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_update_minion_machine'
    )
    def test__set_minion_machine_power_status(
            self, mock_update_minion_machine):
        result = self.task._set_minion_machine_power_status(
            mock.sentinel.ctxt, mock.sentinel.minion_pool_id,
            mock.sentinel.minion_machine_id, mock.sentinel.new_status)
        self.assertIsNone(result)

        mock_update_minion_machine.assert_called_once_with(
            mock.sentinel.ctxt, mock.sentinel.minion_pool_id,
            mock.sentinel.minion_machine_id,
            {'power_status': mock.sentinel.new_status}
        )


class TestBaseReportMinionAllocationFailureForActionTask(
    tasks._BaseReportMinionAllocationFailureForActionTask):
    def _get_task_name(self, action_id):
        pass

    def _report_machine_allocation_failure(
            self, context, action_id, failure_str):
        pass


class BaseReportMinionAllocationFailureForActionTaskTestCase(
        test_base.CoriolisBaseTestCase):
    def setUp(self):
        super(BaseReportMinionAllocationFailureForActionTaskTestCase,
              self).setUp()
        self.task = TestBaseReportMinionAllocationFailureForActionTask(
            mock.sentinel.action_id)

    def test_execute(self):
        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
            result = self.task.execute(mock.sentinel.context)
            self.assertIsNone(result)

    @mock.patch.object(
        MinionManagerClient, 'deallocate_minion_machines_for_action'
    )
    def test_revert(self, mock_deallocate_minion_machines_for_action):
        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.INFO):
            result = self.task.revert(
                mock.sentinel.context, mock.sentinel.flow_failures)
            self.assertIsNone(result)

        mock_deallocate_minion_machines_for_action.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.action_id)


class ReportMinionAllocationFailureForMigrationTaskTestCase(
        test_base.CoriolisBaseTestCase):
    def setUp(self):
        super(ReportMinionAllocationFailureForMigrationTaskTestCase,
              self).setUp()
        self.action_id = mock.sentinel.action_id
        self.task = tasks.ReportMinionAllocationFailureForMigrationTask(
            self.action_id)

    def test_get_task_name(self):
        result = self.task._get_task_name(self.action_id)
        self.assertEqual(
            result,
            f"migration-{self.action_id}-minion-allocation-failure"
        )

    @mock.patch.object(
        ConductorClient, 'report_migration_minions_allocation_error'
    )
    def test__report_machine_allocation_failure(
            self, mock_report_migration_minions_allocation_error):
        result = self.task._report_machine_allocation_failure(
            mock.sentinel.context, self.action_id, mock.sentinel.failure_str)
        self.assertIsNone(result)

        mock_report_migration_minions_allocation_error.assert_called_once_with(
            mock.sentinel.context, self.action_id, mock.sentinel.failure_str
        )


class ReportMinionAllocationFailureForReplicaTaskTestCase(
        test_base.CoriolisBaseTestCase):
    def setUp(self):
        super(ReportMinionAllocationFailureForReplicaTaskTestCase,
              self).setUp()
        self.action_id = mock.sentinel.action_id
        self.task = tasks.ReportMinionAllocationFailureForReplicaTask(
            self.action_id)

    def test_get_task_name(self):
        result = self.task._get_task_name(self.action_id)
        self.assertEqual(
            result,
            f"replica-{self.action_id}-minion-allocation-failure"
        )

    @mock.patch.object(
        ConductorClient, 'report_replica_minions_allocation_error'
    )
    def test__report_machine_allocation_failure(
            self, mock_report_replica_minions_allocation_error):
        result = self.task._report_machine_allocation_failure(
            mock.sentinel.context, self.action_id, mock.sentinel.failure_str)

        self.assertIsNone(result)
        mock_report_replica_minions_allocation_error.assert_called_once_with(
            mock.sentinel.context, self.action_id, mock.sentinel.failure_str
        )


class TestBaseConfirmMinionAllocationForActionTask(
    tasks._BaseConfirmMinionAllocationForActionTask):

    def _confirm_machine_allocation_for_action(
            self, context, action_id, machine_allocations):
        pass

    def _get_action_label(self):
        return 'Test action'

    def _get_task_name(self, action_id):
        return 'Test task'


class BaseConfirmMinionAllocationForActionTaskTestCase(
        test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(BaseConfirmMinionAllocationForActionTaskTestCase, self).setUp()
        self.minion_machine = mock.MagicMock()
        self.minion_machine.id = mock.sentinel.minion_machine_id
        self.minion_machine.pool_id = mock.sentinel.pool_id
        self.minion_machine.allocated_action = mock.sentinel.action_id
        self.minion_machine.allocation_status = (
            tasks.constants.MINION_MACHINE_STATUS_IN_USE)
        self.minion_machine.power_status = (
            tasks.constants.MINION_MACHINE_POWER_STATUS_POWERED_ON)

        self.allocated_machine_id_mappings = {
            mock.sentinel.minion_machine_id: {
                'machine_allocation': mock.sentinel.machine_allocation,
                'origin_minion_id': mock.sentinel.origin_minion_id,
                'destination_minion_id': mock.sentinel.destination_minion_id,
                'osmorphing_minion_id': mock.sentinel.osmorphing_minion_id,
            }
        }

        self.task = TestBaseConfirmMinionAllocationForActionTask(
            mock.sentinel.action_id, self.allocated_machine_id_mappings)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    def test_execute(self, mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        result = self.task.execute(mock.sentinel.context)

        mock_get_minion_machine.assert_has_calls([
            mock.call(mock.sentinel.context, mock.sentinel.origin_minion_id,
                      raise_if_not_found=True),
            mock.call(mock.sentinel.context,
                      mock.sentinel.destination_minion_id,
                      raise_if_not_found=True),
            mock.call(mock.sentinel.context,
                      mock.sentinel.osmorphing_minion_id,
                      raise_if_not_found=True)], any_order=True)

        self.assertIsNone(result)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    def test_execute_raises_exception_when_allocation_status_is_not_in_use(
            self, mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        self.minion_machine.allocation_status = 'DEALLOCATED'

        self.assertRaises(
            exception.InvalidMinionMachineState,
            self.task.execute, mock.sentinel.context)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin_minion_id,
            raise_if_not_found=True)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    def test_execute_raises_exception_when_allocated_action_is_not_correct(
            self, mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        self.minion_machine.allocated_action = 'ANOTHER_ACTION'

        self.assertRaises(
            exception.InvalidMinionMachineState,
            self.task.execute, mock.sentinel.context)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin_minion_id,
            raise_if_not_found=True)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    def test_execute_raises_exception_when_power_status_is_not_powered_on(
            self, mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        self.minion_machine.power_status = 'POWERED_OFF'

        self.assertRaises(
            exception.InvalidMinionMachineState,
            self.task.execute, mock.sentinel.context)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin_minion_id,
            raise_if_not_found=True)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    def test_execute_no_machine_allocation(self, mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        self.allocated_machine_id_mappings[
            mock.sentinel.minion_machine_id] = {}

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.WARN):
                self.task.execute(mock.sentinel.context)

        mock_get_minion_machine.assert_not_called()

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    def test_execute_raises_exception_when_required_properties_are_missing(
            self, mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        self.minion_machine.provider_properties = None

        self.assertRaises(
            exception.InvalidMinionMachineState,
            self.task.execute, mock.sentinel.context)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin_minion_id,
            raise_if_not_found=True)

    @mock.patch.object(
        TestBaseConfirmMinionAllocationForActionTask,
        '_confirm_machine_allocation_for_action'
    )
    @mock.patch.object(
        TestBaseConfirmMinionAllocationForActionTask, '_get_minion_machine'
    )
    def test_execute_with_exception_not_found(
            self, mock_get_minion_machine, mock_confirm_allocation):
        mock_get_minion_machine.return_value = self.minion_machine
        mock_confirm_allocation.side_effect = exception.NotFound()

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.ERROR):
            self.assertRaises(
                exception.MinionMachineAllocationFailure,
                self.task.execute, mock.sentinel.context)

    @mock.patch.object(
        TestBaseConfirmMinionAllocationForActionTask,
        '_confirm_machine_allocation_for_action'
    )
    @mock.patch.object(
        TestBaseConfirmMinionAllocationForActionTask, '_get_minion_machine'
    )
    def test_execute_raises_exception_when_invalid_migration_state(
            self, mock_get_minion_machine, mock_confirm_allocation):
        mock_get_minion_machine.return_value = self.minion_machine
        mock_confirm_allocation.side_effect = [
            exception.InvalidReplicaState(reason='Invalid state')]

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.ERROR):
            self.assertRaises(
                exception.MinionMachineAllocationFailure,
                self.task.execute, mock.sentinel.context)


class ConfirmMinionAllocationForMigrationTaskTestCase(
        test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(ConfirmMinionAllocationForMigrationTaskTestCase, self).setUp()
        self.action_id = mock.sentinel.action_id
        self.allocated_machine_id_mappings = {
            mock.sentinel.minion_machine_id: {
                'machine_allocation': mock.sentinel.machine_allocation,
                'origin_minion_id': mock.sentinel.origin_minion_id,
                'destination_minion_id': mock.sentinel.destination_minion_id,
                'osmorphing_minion_id': mock.sentinel.osmorphing_minion_id,
            }
        }

        self.task = tasks.ConfirmMinionAllocationForMigrationTask(
            mock.sentinel.action_id, self.allocated_machine_id_mappings)

    def test__get_action_label(self):
        result = self.task._get_action_label()

        self.assertEqual(result, 'migration')

    def test_get_task_name(self):
        result = self.task._get_task_name(self.action_id)

        self.assertEqual(
            result,
            f"migration-{self.action_id}-minion-allocation-confirmation"
        )

    @mock.patch.object(
        ConductorClient, 'confirm_migration_minions_allocation'
    )
    def test__confirm_machine_allocation_for_action(
            self, mock_confirm_migration_minions_allocation):
        result = self.task._confirm_machine_allocation_for_action(
            mock.sentinel.context, self.action_id,
            mock.sentinel.machine_allocations)

        self.assertIsNone(result)
        mock_confirm_migration_minions_allocation.assert_called_once_with(
            mock.sentinel.context, self.action_id,
            mock.sentinel.machine_allocations)


class ConfirmMinionAllocationForReplicaTaskTestCase(
        test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(ConfirmMinionAllocationForReplicaTaskTestCase, self).setUp()
        self.allocated_machine_id_mappings = {
            mock.sentinel.minion_machine_id: {
                'machine_allocation': mock.sentinel.machine_allocation,
                'origin_minion_id': mock.sentinel.origin_minion_id,
                'destination_minion_id': mock.sentinel.destination_minion_id,
                'osmorphing_minion_id': mock.sentinel.osmorphing_minion_id,
            }
        }

        self.task = tasks.ConfirmMinionAllocationForReplicaTask(
            mock.sentinel.action_id, self.allocated_machine_id_mappings)

    def test__get_action_label(self):
        result = self.task._get_action_label()

        self.assertEqual(result, 'replica')

    def test_get_task_name(self):
        result = self.task._get_task_name(mock.sentinel.action_id)

        self.assertEqual(
            result,
            f"replica-{mock.sentinel.action_id}-minion-allocation-confirmation"
        )

    @mock.patch.object(
        ConductorClient, 'confirm_replica_minions_allocation'
    )
    def test__confirm_machine_allocation_for_action(
            self, mock_confirm_replica_minions_allocation):
        result = self.task._confirm_machine_allocation_for_action(
            mock.sentinel.context, mock.sentinel.action_id,
            mock.sentinel.machine_allocations)

        self.assertIsNone(result)
        mock_confirm_replica_minions_allocation.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.action_id,
            mock.sentinel.machine_allocations)


class UpdateMinionPoolStatusTaskTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis UpdateMinionPoolStatusTask class."""

    def setUp(self):
        super(UpdateMinionPoolStatusTaskTestCase, self).setUp()
        self.status_to_revert_to = mock.sentinel.status_to_revert_to

        self.task = tasks.UpdateMinionPoolStatusTask(
            mock.sentinel.minion_pool_id, mock.sentinel.status,
            status_to_revert_to=self.status_to_revert_to)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_set_minion_pool_status'
    )
    @mock.patch.object(tasks.db_api, 'get_minion_pool')
    def test_execute(self, mock_get_minion_pool, mock_set_minion_pool_status,
                     mock_add_minion_pool_event):
        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
            result = self.task.execute(mock.sentinel.context)
            self.assertEqual(result, self.task._target_status)

        mock_get_minion_pool.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.minion_pool_id,
            include_machines=False, include_events=False,
            include_progress_updates=False)

        mock_set_minion_pool_status.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.minion_pool_id,
            self.task._target_status)
        mock_add_minion_pool_event.assert_called_once_with(
            mock.sentinel.context,
            f"Pool status transitioned from '{self.task._previous_status}' to "
            f"'{self.task._target_status}'"
        )

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_set_minion_pool_status'
    )
    @mock.patch.object(tasks.db_api, 'get_minion_pool')
    def test_execute_when_previous_status_equals_target_status(
            self, mock_get_minion_pool, mock_set_minion_pool_status,
            mock_add_minion_pool_event):
        self.task._previous_status = self.task._target_status

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
            result = self.task.execute(mock.sentinel.context)
            self.assertEqual(result, self.task._target_status)

        mock_get_minion_pool.assert_not_called()
        mock_set_minion_pool_status.assert_not_called()
        mock_add_minion_pool_event.assert_not_called()

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_set_minion_pool_status'
    )
    @mock.patch.object(tasks.db_api, 'get_minion_pool')
    def test_revert(self, mock_get_minion_pool, mock_set_minion_pool_status,
                    mock_add_minion_pool_event):
        mock_get_minion_pool.return_value.status = 'DEALLOCATED'
        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
            result = self.task.revert(mock.sentinel.context)
            self.assertIsNone(result)

        mock_get_minion_pool.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.minion_pool_id,
            include_machines=False, include_events=False,
            include_progress_updates=False)

        mock_set_minion_pool_status.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.minion_pool_id,
            self.status_to_revert_to)

        mock_add_minion_pool_event.assert_called_once_with(
            mock.sentinel.context,
            f"Pool status reverted from 'DEALLOCATED' to "
            f"'{self.status_to_revert_to}'"
        )

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_set_minion_pool_status'
    )
    @mock.patch.object(tasks.db_api, 'get_minion_pool')
    def test_revert_no_minion_pool(self, mock_get_minion_pool,
                                   mock_set_minion_pool_status,
                                   mock_add_minion_pool_event):
        mock_get_minion_pool.return_value = None

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
            result = self.task.revert(mock.sentinel.context)
            self.assertIsNone(result)

        mock_set_minion_pool_status.assert_not_called()
        mock_add_minion_pool_event.assert_not_called()

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_set_minion_pool_status'
    )
    @mock.patch.object(tasks.db_api, 'get_minion_pool')
    def test_revert_when_previous_status_equals_target_status(
            self, mock_get_minion_pool, mock_set_minion_pool_status,
            mock_add_minion_pool_event):
        mock_get_minion_pool.return_value.status = self.status_to_revert_to

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
            result = self.task.revert(mock.sentinel.context)
            self.assertIsNone(result)

        mock_get_minion_pool.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.minion_pool_id,
            include_machines=False, include_events=False,
            include_progress_updates=False)

        mock_set_minion_pool_status.assert_not_called()
        mock_add_minion_pool_event.assert_not_called()


class TestBaseMinionManangerTask(tasks.BaseMinionManangerTask):
    def _get_task_name(self, minion_pool_id, minion_machine_id):
        return 'test_task'


class BaseMinionManangerTaskTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis BaseMinionManangerTask class."""

    def setUp(self):
        super(BaseMinionManangerTaskTestCase, self).setUp()
        self.task = TestBaseMinionManangerTask(
            mock.sentinel.minion_pool_id, mock.sentinel.minion_machine_id,
            mock.sentinel.main_task_runner_type)

    @mock.patch.object(base.BaseRunWorkerTask, 'execute')
    def test_execute(self, mock_execute):
        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.INFO):
            result = self.task.execute(
                mock.sentinel.context, mock.sentinel.origin,
                mock.sentinel.destination, mock.sentinel.task_info)
            self.assertEqual(result, mock_execute.return_value)

        mock_execute.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, mock.sentinel.task_info
        )

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event')
    @mock.patch.object(base.BaseRunWorkerTask, 'revert')
    def test_revert(self, mock_revert, mock_add_minion_pool_event):
        result = self.task.revert(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, mock.sentinel.task_info)
        self.assertIsNone(result)

        mock_add_minion_pool_event.assert_called_once_with(
            mock.sentinel.context,
            ("Failure occurred for one or more operations on minion pool "
             "'sentinel.minion_pool_id'. Please check the logs for additional "
             "details. Error messages were:\nNo flow failures provided."),
            level=tasks.constants.TASK_EVENT_ERROR
        )
        mock_revert.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, mock.sentinel.task_info
        )


class ValidateMinionPoolOptionsTaskTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis ValidateMinionPoolOptionsTask class."""

    def setUp(self):
        super(ValidateMinionPoolOptionsTaskTestCase, self).setUp()
        self.minion_pool_id = 'test_pool_id'

        self.task = tasks.ValidateMinionPoolOptionsTask(
            self.minion_pool_id, mock.sentinel.minion_machine_id,
            mock.sentinel.minion_pool_type)

    def test__get_task_name(self):
        result = self.task._get_task_name(
            self.minion_pool_id, mock.sentinel.minion_machine_id)

        self.assertEqual(
            result,
            tasks.MINION_POOL_VALIDATION_TASK_NAME_FORMAT %
            (self.minion_pool_id)
        )

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    def test_execute(self, mock_execute, mock_add_minion_pool_event):
        result = self.task.execute(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, mock.sentinel.task_info)
        self.assertIsNone(result)

        mock_execute.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, mock.sentinel.task_info
        )

        mock_add_minion_pool_event.assert_has_calls([
            mock.call(
                mock.sentinel.context, "Validating minion pool options"),
            mock.call(
                mock.sentinel.context,
                "Successfully validated minion pool options")
        ])

    @mock.patch.object(tasks.BaseMinionManangerTask, 'revert')
    def test_revert(self, mock_revert):
        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
            result = self.task.revert(
                mock.sentinel.context, mock.sentinel.origin,
                mock.sentinel.destination, mock.sentinel.task_info)
            self.assertIsNone(result)

        mock_revert.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, mock.sentinel.task_info
        )


class AllocateSharedPoolResourcesTaskTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis AllocateSharedPoolResourcesTask class."""

    def setUp(self):
        super(AllocateSharedPoolResourcesTaskTestCase, self).setUp()
        self.minion_pool_id = 'test_pool_id'
        self.task_info = {
            'pool_shared_resources': True
        }

        self.task = tasks.AllocateSharedPoolResourcesTask(
            self.minion_pool_id, mock.sentinel.minion_machine_id,
            mock.sentinel.minion_pool_type)

    def test__get_task_name(self):
        result = self.task._get_task_name(
            self.minion_pool_id, mock.sentinel.minion_machine_id)

        self.assertEqual(
            result,
            tasks.MINION_POOL_ALLOCATE_SHARED_RESOURCES_TASK_NAME_FORMAT %
            (self.minion_pool_id)
        )

    @mock.patch.object(tasks.db_api, 'get_minion_pool')
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    @mock.patch.object(tasks.db_api, 'update_minion_pool')
    @mock.patch.object(tasks.minion_manager_utils, 'get_minion_pool_lock')
    def test_execute(self, mock_get_minion_pool_lock, mock_update_minion_pool,
                     mock_execute, mock_add_minion_pool_event,
                     mock_get_minion_pool):
        mock_execute.return_value = {'pool_shared_resources': {}}
        mock_get_minion_pool.return_value.shared_resources = False

        result = self.task.execute(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)
        self.assertEqual(result, mock_execute.return_value)

        mock_get_minion_pool_lock.assert_called_with(
            self.minion_pool_id, external=True)
        mock_get_minion_pool.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id)
        mock_update_minion_pool.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id,
            {'shared_resources': {}})
        mock_add_minion_pool_event.assert_has_calls([
            mock.call(
                mock.sentinel.context, "Deploying shared pool resources"),
            mock.call(
                mock.sentinel.context,
                "Successfully deployed shared pool resources")
        ])

    @mock.patch.object(tasks.db_api, 'get_minion_pool')
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    @mock.patch.object(tasks.db_api, 'update_minion_pool')
    @mock.patch.object(tasks.minion_manager_utils, 'get_minion_pool_lock')
    def test_execute_shared_resources_already_allocated(
            self, mock_get_minion_pool_lock, mock_update_minion_pool,
            mock_execute, mock_add_minion_pool_event, mock_get_minion_pool):
        mock_get_minion_pool.return_value.shared_resources = True

        self.assertRaises(
            exception.InvalidMinionPoolState, self.task.execute,
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)

        mock_get_minion_pool_lock.assert_called_once_with(
            self.minion_pool_id, external=True)
        mock_get_minion_pool.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id)
        mock_add_minion_pool_event.assert_not_called()
        mock_execute.assert_not_called()
        mock_update_minion_pool.assert_not_called()

    @mock.patch.object(tasks.db_api, 'get_minion_pool')
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    @mock.patch.object(tasks.db_api, 'update_minion_pool')
    @mock.patch.object(tasks.minion_manager_utils, 'get_minion_pool_lock')
    def test_execute_no_minion_pool(
            self, mock_get_minion_pool_lock, mock_update_minion_pool,
            mock_execute, mock_add_minion_pool_event, mock_get_minion_pool):
        mock_get_minion_pool.return_value = None

        self.assertRaises(
            exception.InvalidMinionPoolSelection, self.task.execute,
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)

        mock_get_minion_pool_lock.assert_called_once_with(
            self.minion_pool_id, external=True)
        mock_get_minion_pool.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id)
        mock_add_minion_pool_event.assert_not_called()
        mock_execute.assert_not_called()
        mock_update_minion_pool.assert_not_called()

    @mock.patch.object(tasks.BaseMinionManangerTask, 'revert')
    @mock.patch.object(tasks.minion_manager_utils, 'get_minion_pool_lock')
    @mock.patch.object(tasks.db_api, 'update_minion_pool')
    def test_revert(self, mock_update_minion_pool, mock_get_minion_pool_lock,
                    mock_revert):
        self.task_info = {}
        self.update_values = {
            'pool_shared_resources': None
        }
        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
            result = self.task.revert(
                mock.sentinel.context, mock.sentinel.origin,
                mock.sentinel.destination, self.task_info)
            self.assertIsNone(result)

        mock_revert.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info
        )
        mock_get_minion_pool_lock.assert_called_once_with(
            self.minion_pool_id, external=True)
        mock_update_minion_pool.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id, self.update_values)


class DeallocateSharedPoolResourcesTaskTestCase(
    test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis DeallocateSharedPoolResourcesTask class."""

    def setUp(self):
        super(DeallocateSharedPoolResourcesTaskTestCase, self).setUp()
        self.minion_pool_id = 'test_pool_id'
        self.task_info = {
            'pool_shared_resources': True,
            'pool_environment_options': 'test_options'
        }

        self.task = tasks.DeallocateSharedPoolResourcesTask(
            self.minion_pool_id, mock.sentinel.minion_machine_id,
            mock.sentinel.minion_pool_type)

    def test__get_task_name(self):
        result = self.task._get_task_name(
            self.minion_pool_id, mock.sentinel.minion_machine_id)

        self.assertEqual(
            result,
            tasks.MINION_POOL_DEALLOCATE_SHARED_RESOURCES_TASK_NAME_FORMAT %
            (self.minion_pool_id)
        )

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(tasks.db_api, 'update_minion_pool')
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    def test_execute(self, mock_execute, mock_update_minion_pool,
                     mock_add_minion_pool_event):
        self.update_values = {
            'shared_resources': None,
        }
        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.WARN):
                result = self.task.execute(
                    mock.sentinel.context, mock.sentinel.origin,
                    mock.sentinel.destination, self.task_info)
                self.assertEqual(result, self.task_info)

        mock_add_minion_pool_event.assert_has_calls([
            mock.call(
                mock.sentinel.context, "Deallocating shared pool resources"),
            mock.call(
                mock.sentinel.context,
                "Successfully deallocated shared pool resources")
        ])
        mock_execute.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)
        mock_update_minion_pool.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id, self.update_values)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(tasks.db_api, 'update_minion_pool')
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    def test_execute_no_pool_shared_resources(
            self, mock_execute, mock_update_minion_pool,
            mock_add_minion_pool_event):
        self.task_info = {}
        self.assertRaises(
            exception.InvalidInput, self.task.execute,
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)

        mock_add_minion_pool_event.assert_called_once_with(
            mock.sentinel.context,
            "Deallocating shared pool resources"
        )
        mock_execute.assert_not_called()
        mock_update_minion_pool.assert_not_called()


class AllocateMinionMachineTaskTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis AllocateMinionMachineTask class."""

    def setUp(self):
        super(AllocateMinionMachineTaskTestCase, self).setUp()
        self.minion_machine = mock.MagicMock()
        self.minion_machine.id = 'test_machine_id'
        self.minion_machine.pool_id = 'test_pool_id'
        self.minion_machine.allocation_status = (
            tasks.constants.MINION_MACHINE_STATUS_UNINITIALIZED)
        self.minion_machine.allocated_action = mock.sentinel.allocation_action
        self.minion_pool_id = 'test_pool_id'
        self.minion_machine_id = 'test_machine_id'
        self.task_info = {
            'pool_environment_options': 'test_env_options',
            'pool_shared_resources': True,
            'pool_identifier': 'test_identifier',
            'pool_os_type': 'linux',
        }
        self.mock_failure = mock.MagicMock(spec=failure.Failure)

        self.task = tasks.AllocateMinionMachineTask(
            self.minion_pool_id, self.minion_machine_id,
            mock.sentinel.main_task_runner_type)

        self.task._allocate_to_action = self.minion_machine.allocated_action

    def test__get_task_name(self):
        result = self.task._get_task_name(
            self.minion_pool_id, self.minion_machine_id)

        self.assertEqual(
            result,
            tasks.MINION_POOL_ALLOCATE_MACHINE_TASK_NAME_FORMAT %
            (self.minion_pool_id, self.minion_machine_id)
        )

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_update_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin,
        '_set_minion_machine_allocation_status'
    )
    @mock.patch.object(tasks.timeutils, 'utcnow')
    @mock.patch.object(tasks.models, 'MinionMachine')
    @mock.patch.object(tasks.db_api, 'add_minion_machine')
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    def test_execute(
            self, mock_execute, mock_add_minion_machine, mock_minion_machine,
            mock_utcnow, mock_set_minion_machine_allocation,
            mock_add_minion_pool_event, mock_update_minion_machine,
            mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        mock_execute.return_value = {
            'minion_connection_info': 'test_connection_info',
            'minion_provider_properties': 'test_provider_properties',
            'minion_backup_writer_connection_info': (
                'test_backup_writer_connection_info'),
        }
        expected_updated_values = {
            'last_used_at': mock_utcnow.return_value,
            'allocation_status': (
                tasks.constants.MINION_MACHINE_STATUS_IN_USE),
            'power_status': (
                tasks.constants.MINION_MACHINE_POWER_STATUS_POWERED_ON),
            'connection_info': 'test_connection_info',
            'provider_properties': 'test_provider_properties',
            'backup_writer_connection_info': (
                'test_backup_writer_connection_info'),
            'allocated_action': self.task._allocate_to_action
        }
        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.INFO):
            result = self.task.execute(
                mock.sentinel.context, mock.sentinel.origin,
                mock.sentinel.destination, self.task_info)
            self.assertEqual(result, self.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=False)
        mock_set_minion_machine_allocation.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id, self.minion_machine_id,
            tasks.constants.MINION_MACHINE_STATUS_ALLOCATING)
        mock_minion_machine.assert_not_called()
        mock_add_minion_machine.assert_not_called()
        mock_add_minion_pool_event.assert_has_calls([
            mock.call(
                mock.sentinel.context,
                f"Allocating minion machine with internal pool ID "
                f"'{self.minion_machine_id}' to be used for transfer "
                f"action with ID '{self.task._allocate_to_action}'"),
            mock.call(
                mock.sentinel.context,
                f"Successfully allocated minion machine with internal pool "
                f"ID '{self.minion_machine_id}'")
        ])
        mock_execute.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)
        mock_update_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id, self.minion_machine_id,
            expected_updated_values)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    def test_execute_when_allocation_status_is_not_uninitialized(
            self, mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        self.minion_machine.allocation_status = 'ALLOCATING'
        self.assertRaises(
            exception.InvalidMinionMachineState, self.task.execute,
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=False)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    def test_execute_when_minion_machine_belongs_to_different_pool(
            self, mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        self.minion_machine.pool_id = 'DIFFERENT_POOL_ID'

        self.assertRaises(
            exception.InvalidMinionMachineState, self.task.execute,
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=False)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    def test_execute_when_minion_machine_already_allocated(
            self, mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        self.task._allocate_to_action = 'ANOTHER_ACTION'

        self.assertRaises(
            exception.InvalidMinionMachineState, self.task.execute,
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=False)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_update_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin,
        '_set_minion_machine_allocation_status'
    )
    @mock.patch.object(tasks.models, 'MinionMachine')
    @mock.patch.object(tasks.db_api, 'add_minion_machine')
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    def test_execute_add_new_minion_machine(
            self, mock_execute, mock_add_minion_machine, mock_minion_machine,
            mock_set_minion_machine_allocation, mock_add_minion_pool_event,
            mock_update_minion_machine, mock_get_minion_machine):
        mock_get_minion_machine.return_value = None
        mock_minion_machine.return_value = self.minion_machine
        mock_execute.side_effect = CoriolisTestException

        self.assertRaises(CoriolisTestException, self.task.execute,
                          mock.sentinel.context, mock.sentinel.origin,
                          mock.sentinel.destination, self.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=False)
        mock_minion_machine.assert_called_once_with()
        mock_add_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine)
        mock_set_minion_machine_allocation.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id, self.minion_machine_id,
            tasks.constants.MINION_MACHINE_STATUS_ERROR_DEPLOYING)
        mock_add_minion_pool_event.assert_called_once_with(
            mock.sentinel.context,
            f"Allocating minion machine with internal pool ID "
            f"'{self.minion_machine_id}' to be used for transfer "
            f"action with ID '{self.task._allocate_to_action}'",
        )
        mock_update_minion_machine.assert_not_called()

    @mock.patch.object(tasks.minion_manager_utils, 'get_minion_pool_lock')
    @mock.patch.object(tasks.db_api, 'get_minion_machine')
    @mock.patch.object(tasks.db_api, 'delete_minion_machine')
    @mock.patch.object(tasks.BaseMinionManangerTask, 'revert')
    def test_revert(self, mock_revert, mock_delete_minion_machine,
                    mock_get_minion_machine, mock_get_minion_pool_lock):
        self.task_info['minion_provider_properties'] = (
            mock_get_minion_machine.return_value.provider_properties)
        mock_get_minion_machine.return_value = self.minion_machine
        self.mock_failure.traceback_str = 'test_traceback'
        kwargs = {'result': self.mock_failure}

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.WARN):
                result = self.task.revert(
                    mock.sentinel.context, mock.sentinel.origin,
                    mock.sentinel.destination, self.task_info, **kwargs)
                self.assertIsNone(result)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id)
        mock_get_minion_pool_lock.assert_called_once_with(
            self.minion_pool_id, external=True)
        mock_delete_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id)
        mock_revert.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info, **kwargs
        )

    @mock.patch.object(tasks.minion_manager_utils, 'get_minion_pool_lock')
    @mock.patch.object(tasks.db_api, 'get_minion_machine')
    @mock.patch.object(tasks.db_api, 'delete_minion_machine')
    @mock.patch.object(tasks.BaseMinionManangerTask, 'revert')
    def test_revert_failed_deletion(
            self, mock_revert, mock_delete_minion_machine,
            mock_get_minion_machine, mock_get_minion_pool_lock):
        mock_get_minion_machine.return_value = None
        mock_delete_minion_machine.side_effect = CoriolisTestException

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.WARN):
            result = self.task.revert(
                mock.sentinel.context, mock.sentinel.origin,
                mock.sentinel.destination, self.task_info)
            self.assertIsNone(result)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id)
        mock_get_minion_pool_lock.assert_called_once_with(
            self.minion_pool_id, external=True)
        mock_revert.assert_not_called()

    @mock.patch.object(tasks.minion_manager_utils, 'get_minion_pool_lock')
    @mock.patch.object(tasks.db_api, 'get_minion_machine')
    @mock.patch.object(tasks.db_api, 'delete_minion_machine')
    @mock.patch.object(tasks.BaseMinionManangerTask, 'revert')
    def test_revert_unexpected_result_type(
            self, mock_revert, mock_delete_minion_machine,
            mock_get_minion_machine, mock_get_minion_pool_lock):
        self.task_info['minion_provider_properties'] = (
            mock_get_minion_machine.return_value.provider_properties)
        mock_get_minion_machine.return_value = self.minion_machine
        kwargs = {'result': 'test_result'}

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.WARN):
            result = self.task.revert(
                mock.sentinel.context, mock.sentinel.origin,
                mock.sentinel.destination, self.task_info, **kwargs)
            self.assertIsNone(result)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id)
        mock_get_minion_pool_lock.assert_called_once_with(
            self.minion_pool_id, external=True)
        mock_delete_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id)
        mock_revert.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info, **kwargs
        )

    @mock.patch.object(tasks.minion_manager_utils, 'get_minion_pool_lock')
    @mock.patch.object(tasks.db_api, 'get_minion_machine')
    @mock.patch.object(tasks.db_api, 'delete_minion_machine')
    @mock.patch.object(tasks.BaseMinionManangerTask, 'revert')
    def test_revert_raises_unhandled_exception(
            self, mock_revert, mock_delete_minion_machine,
            mock_get_minion_machine, mock_get_minion_pool_lock):
        mock_get_minion_machine.return_value = self.minion_machine
        mock_revert.side_effect = CoriolisTestException

        self.assertRaises(
            CoriolisTestException, self.task.revert,
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)

        mock_get_minion_pool_lock.assert_called_once_with(
            self.minion_pool_id, external=True)
        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id)
        mock_delete_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id)

    @mock.patch.object(tasks.minion_manager_utils, 'get_minion_pool_lock')
    @mock.patch.object(tasks.db_api, 'get_minion_machine')
    @mock.patch.object(tasks.db_api, 'delete_minion_machine')
    @mock.patch.object(tasks.BaseMinionManangerTask, 'revert')
    def test_revert_no_raise_on_cleanup_failure(
            self, mock_revert, mock_delete_minion_machine,
            mock_get_minion_machine, mock_get_minion_pool_lock):
        mock_get_minion_machine.return_value = self.minion_machine
        self.task._raise_on_cleanup_failure = False
        mock_revert.side_effect = CoriolisTestException

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.WARN):
            result = self.task.revert(
                mock.sentinel.context, mock.sentinel.origin,
                mock.sentinel.destination, self.task_info)
            self.assertIsNone(result)

        mock_get_minion_pool_lock.assert_called_once_with(
            self.minion_pool_id, external=True)
        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id)
        mock_delete_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id)


class DeallocateMinionMachineTaskTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis DeallocateMinionMachineTask class."""

    def setUp(self):
        super(DeallocateMinionMachineTaskTestCase, self).setUp()
        self.minion_machine = mock.MagicMock()
        self.minion_machine.id = 'test_machine_id'
        self.minion_machine.pool_id = 'test_pool_id'
        self.minion_machine.allocation_status = (
            tasks.constants.MINION_MACHINE_STATUS_IN_USE)
        self.minion_machine.allocated_action = mock.sentinel.allocation_action
        self.minion_pool_id = 'test_pool_id'
        self.minion_machine_id = 'test_machine_id'
        self.task_info = {
            'minion_provider_properties': None,
        }
        self.mock_failure = mock.MagicMock(spec=failure.Failure)

        self.task = tasks.DeallocateMinionMachineTask(
            self.minion_pool_id, self.minion_machine_id,
            mock.sentinel.main_task_runner_type)

        self.task._deallocate_from_action = (
            self.minion_machine.allocated_action)

    def test__get_task_name(self):
        result = self.task._get_task_name(
            self.minion_pool_id, self.minion_machine_id)

        self.assertEqual(
            result,
            tasks.MINION_POOL_DEALLOCATE_MACHINE_TASK_NAME_FORMAT %
            (self.minion_pool_id, self.minion_machine_id)
        )

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin,
        '_set_minion_machine_allocation_status'
    )
    @mock.patch.object(tasks.db_api, 'delete_minion_machine')
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    @mock.patch.object(tasks.minion_manager_utils, 'get_minion_pool_lock')
    def test_execute(
            self, mock_get_minion_pool_lock, mock_execute,
            mock_delete_minion_machine, mock_set_minion_machine_allocation,
            mock_add_minion_pool_event, mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        self.minion_machine.provider_properties = None

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
            result = self.task.execute(
                mock.sentinel.context, mock.sentinel.origin,
                mock.sentinel.destination, self.task_info)
            self.assertEqual(result, self.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id)
        mock_set_minion_machine_allocation.assert_not_called()
        mock_add_minion_pool_event.assert_has_calls([
            mock.call(
                mock.sentinel.context,
                f"Deallocating minion machine with internal pool ID "
                f"'{self.minion_machine_id}'"),
            mock.call(
                mock.sentinel.context,
                f"Minion machine with ID '{self.minion_machine_id}' "
                f"had no provider properties set. "
                f"Presuming it failed to deploy in the first place and "
                f"simply removing the machine's entry from the DB"),
            mock.call(
                mock.sentinel.context,
                f"Successfully deallocated minion machine with internal pool "
                f"ID '{self.minion_machine_id}'")
        ])
        mock_execute.assert_not_called()
        mock_delete_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id)
        mock_get_minion_pool_lock.assert_called_once_with(
            self.minion_pool_id, external=True)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    def test_execute_no_machine(self, mock_get_minion_machine):
        mock_get_minion_machine.return_value = None

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.INFO):
            result = self.task.execute(
                mock.sentinel.context, mock.sentinel.origin,
                mock.sentinel.destination, self.task_info)
            self.assertEqual(result, self.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin,
        '_set_minion_machine_allocation_status'
    )
    @mock.patch.object(tasks.db_api, 'delete_minion_machine')
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    @mock.patch.object(tasks.minion_manager_utils, 'get_minion_pool_lock')
    def test_execute_with_provider_properties(
            self, mock_get_minion_pool_lock, mock_execute,
            mock_delete_minion_machine, mock_set_minion_machine_allocation,
            mock_add_minion_pool_event, mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        self.task_info['minion_provider_properties'] = (
            mock_get_minion_machine.return_value.provider_properties)

        result = self.task.execute(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)
        self.assertEqual(result, self.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id)
        mock_add_minion_pool_event.assert_has_calls([
            mock.call(
                mock.sentinel.context,
                f"Deallocating minion machine with internal pool ID "
                f"'{self.minion_machine_id}'"),
            mock.call(
                mock.sentinel.context,
                f"Successfully deallocated minion machine with internal pool "
                f"ID '{self.minion_machine_id}'")
        ])
        mock_set_minion_machine_allocation.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id, self.minion_machine_id,
            tasks.constants.MINION_MACHINE_STATUS_DEALLOCATING)
        mock_execute.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)
        mock_delete_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id)
        mock_get_minion_pool_lock.assert_called_once_with(
            self.minion_pool_id, external=True)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin,
        '_set_minion_machine_allocation_status'
    )
    @mock.patch.object(tasks.db_api, 'delete_minion_machine')
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    @mock.patch.object(tasks.minion_manager_utils, 'get_minion_pool_lock')
    def test_execute_with_exception(
            self, mock_get_minion_pool_lock, mock_execute,
            mock_delete_minion_machine, mock_set_minion_machine_allocation,
            mock_add_minion_pool_event, mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        mock_execute.side_effect = CoriolisTestException

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.WARN):
            self.assertRaises(
                CoriolisTestException, self.task.execute,
                mock.sentinel.context, mock.sentinel.origin,
                mock.sentinel.destination, self.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id)
        mock_add_minion_pool_event.assert_has_calls([
            mock.call(
                mock.sentinel.context,
                f"Deallocating minion machine with internal pool ID "
                f"'{self.minion_machine_id}'"),
            mock.call(
                mock.sentinel.context,
                f"Exception occured while deallocating minion machine "
                f"'{self.minion_machine_id}' "
                f"There might be leftover instance resources requiring "
                f"manual cleanup", level=tasks.constants.TASK_EVENT_ERROR)
        ])
        mock_set_minion_machine_allocation.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id, self.minion_machine_id,
            tasks.constants.MINION_MACHINE_STATUS_DEALLOCATING)
        mock_delete_minion_machine.assert_not_called()
        mock_get_minion_pool_lock.assert_not_called()


class HealthcheckMinionMachineTaskTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis HealthcheckMinionMachineTask class."""

    def setUp(self):
        super(HealthcheckMinionMachineTaskTestCase, self).setUp()
        self.minion_machine = mock.MagicMock()
        self.minion_machine.id = 'test_machine_id'
        self.minion_machine.pool_id = 'test_pool_id'
        self.minion_machine.allocated_action = mock.sentinel.allocation_action
        self.minion_pool_id = 'test_pool_id'
        self.minion_machine_id = 'test_machine_id'
        self.task = tasks.HealthcheckMinionMachineTask(
            self.minion_pool_id, self.minion_machine_id,
            mock.sentinel.minion_pool_type,
            machine_status_on_success=(
                tasks.constants.MINION_MACHINE_STATUS_AVAILABLE))

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin,
        '_set_minion_machine_allocation_status'
    )
    def test_execute(
            self, mock_set_minion_machine_allocation, mock_execute,
            mock_add_minion_pool_event, mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        expected_res = {
            'healthy': True,
            'error': None
        }
        execution_info = {
            'minion_provider_properties': (
                mock_get_minion_machine.return_value.provider_properties),
            'minion_connection_info': (
                mock_get_minion_machine.return_value.connection_info),
        }

        result = self.task.execute(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, mock.sentinel.task_info)
        self.assertEqual(result, expected_res)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=False)
        mock_execute.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, execution_info)
        mock_add_minion_pool_event.assert_has_calls([
            mock.call(
                mock.sentinel.context,
                f"Healthchecking minion machine with internal pool ID "
                f"'{self.minion_machine_id}'"),
            mock.call(
                mock.sentinel.context,
                f"Successfully healthchecked minion machine with internal "
                f"pool ID '{self.minion_machine_id}'")
        ])
        mock_set_minion_machine_allocation.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id, self.minion_machine_id,
            tasks.constants.MINION_MACHINE_STATUS_AVAILABLE)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin,
        '_set_minion_machine_allocation_status'
    )
    def test_execute_no_machine(
            self, mock_set_minion_machine_allocation, mock_execute,
            mock_add_minion_pool_event, mock_get_minion_machine):
        mock_get_minion_machine.return_value = None
        expected_base_msg = (
            f"Could not find minion machine DB entry with ID "
            f"'{self.minion_machine_id}' for healthcheck.")
        expected_res = {
            'healthy': False,
            'error': expected_base_msg
        }

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.INFO):
                result = self.task.execute(
                    mock.sentinel.context, mock.sentinel.origin,
                    mock.sentinel.destination, mock.sentinel.task_info)
                self.assertEqual(result, expected_res)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=False)
        mock_add_minion_pool_event.assert_called_once_with(
            mock.sentinel.context,
            f"{expected_base_msg} Reporting healthcheck as failed",
            level=tasks.constants.TASK_EVENT_WARNING
        )
        mock_set_minion_machine_allocation.assert_not_called()
        mock_execute.assert_not_called()

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    def test_execute_no_machine_with_fail_on_error(
            self, mock_add_minion_pool_event, mock_get_minion_machine):
        expected_base_msg = (
            f"Could not find minion machine DB entry with ID "
            f"'{self.minion_machine_id}' for healthcheck.")
        self.task._fail_on_error = True
        mock_get_minion_machine.return_value = None

        self.assertRaises(
            exception.InvalidMinionMachineState, self.task.execute,
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, mock.sentinel.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=False)
        mock_add_minion_pool_event.assert_called_once_with(
            mock.sentinel.context,
            f"{expected_base_msg} Reporting healthcheck as failed",
            level=tasks.constants.TASK_EVENT_WARNING
        )

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin,
        '_set_minion_machine_allocation_status'
    )
    def test_execute_with_allocation_status(
            self, mock_set_minion_machine_allocation, mock_execute,
            mock_add_minion_pool_event, mock_get_minion_machine):
        self.minion_machine.allocation_status = (
            tasks.constants.MINION_MACHINE_STATUS_ERROR)
        mock_get_minion_machine.return_value = self.minion_machine
        expected_base_msg = (
            f"Minion Machine with ID '{self.minion_machine_id}' is marked as "
            f"'{self.minion_machine.allocation_status}' in the DB.")
        expected_res = {
            'error': expected_base_msg,
            'healthy': False
        }

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
            result = self.task.execute(
                mock.sentinel.context, mock.sentinel.origin,
                mock.sentinel.destination, mock.sentinel.task_info)
            self.assertEqual(result, expected_res)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=False)
        mock_add_minion_pool_event.assert_has_calls([
            mock.call(
                mock.sentinel.context,
                f'{expected_base_msg}'" Reporting healthcheck as failed",
                level=tasks.constants.TASK_EVENT_WARNING),
        ])
        mock_set_minion_machine_allocation.assert_not_called()
        mock_execute.assert_not_called()

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    def test_execute_with_allocation_status_and_fail_on_error(
            self, mock_add_minion_pool_event, mock_get_minion_machine):
        self.minion_machine.allocation_status = (
            tasks.constants.MINION_MACHINE_STATUS_ERROR)
        self.task._fail_on_error = True
        mock_get_minion_machine.return_value = self.minion_machine
        expected_base_msg = (
            f"Minion Machine with ID '{self.minion_machine_id}' is marked as "
            f"'{self.minion_machine.allocation_status}' in the DB.")

        self.assertRaises(
            exception.InvalidMinionMachineState, self.task.execute,
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, mock.sentinel.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=False)
        mock_add_minion_pool_event.assert_called_once_with(
            mock.sentinel.context,
            f'{expected_base_msg}'" Reporting healthcheck as failed",
            level=tasks.constants.TASK_EVENT_WARNING
        )

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin,
        '_set_minion_machine_allocation_status'
    )
    def test_execute_with_exception(
            self, mock_set_minion_machine_allocation, mock_execute,
            mock_add_minion_pool_event, mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        mock_execute.side_effect = CoriolisTestException
        expected_res = {
            'error': "",
            'healthy': False
        }

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
            result = self.task.execute(
                mock.sentinel.context, mock.sentinel.origin,
                mock.sentinel.destination, mock.sentinel.task_info)
            self.assertEqual(result, expected_res)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=False)
        mock_add_minion_pool_event.assert_has_calls([
            mock.call(
                mock.sentinel.context,
                f"Healthchecking minion machine with internal pool ID "
                f"'{self.minion_machine_id}'"),
            mock.call(
                mock.sentinel.context,
                f"Healthcheck for machine with internal pool ID "
                f"'{self.minion_machine_id}' has failed.",
                level=tasks.constants.TASK_EVENT_WARNING)
        ])
        mock_set_minion_machine_allocation.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id, self.minion_machine_id,
            tasks.constants.MINION_MACHINE_STATUS_ERROR)

    def test__get_task_name(self):
        result = self.task._get_task_name(
            self.minion_pool_id, self.minion_machine_id)

        self.assertEqual(
            result,
            tasks.MINION_POOL_HEALTHCHECK_MACHINE_TASK_NAME_FORMAT %
            (self.minion_pool_id, self.minion_machine_id)
        )


class MinionMachineHealtchcheckDeciderTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis MinionMachineHealtchcheckDecider class."""

    def setUp(self):
        super(MinionMachineHealtchcheckDeciderTestCase, self).setUp()
        self.minion_pool_id = 'test_pool_id'
        self.minion_machine_id = 'test_machine_id'
        self.healthcheck_task_name = (
            tasks.MINION_POOL_HEALTHCHECK_MACHINE_TASK_NAME_FORMAT %
            (self.minion_pool_id, self.minion_machine_id)
        )

        self.task = tasks.MinionMachineHealtchcheckDecider(
            self.minion_pool_id, self.minion_machine_id,
            on_successful_healthcheck=True)

    def test_call_with_empty_history(self):
        history = {}

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.WARN):
            result = self.task.__call__(history)
            self.assertFalse(result)

    def test_call_with_healthy_result(self):
        history = {
            self.healthcheck_task_name: {
                "healthy": True
            }
        }

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
            result = self.task.__call__(history)
            self.assertEqual(result, self.task._on_success)

    def test_call_with_unhealthy_result(self):
        history = {
            self.healthcheck_task_name: {
                "healthy": False
            }
        }

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
                result = self.task.__call__(history)
                self.assertEqual(result, not self.task._on_success)


class PowerOnMinionMachineTaskTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis PowerOnMinionMachineTask class."""

    def setUp(self):
        super(PowerOnMinionMachineTaskTestCase, self).setUp()
        self.minion_machine = mock.MagicMock()
        self.minion_machine.id = 'test_machine_id'
        self.minion_machine.pool_id = 'test_pool_id'
        self.minion_machine.allocated_action = mock.sentinel.allocation_action
        self.minion_pool_id = 'test_pool_id'
        self.minion_machine_id = 'test_machine_id'
        self.task_info = {
            'minion_provider_properties': None,
        }
        self.minion_machine.power_status = (
            tasks.constants.MINION_MACHINE_POWER_STATUS_POWERED_OFF)

        self.task = tasks.PowerOnMinionMachineTask(
            self.minion_pool_id, self.minion_machine_id, self.task_info)

    def test__get_task_name(self):
        result = self.task._get_task_name(
            self.minion_pool_id, self.minion_machine_id)

        self.assertEqual(
            result,
            tasks.MINION_POOL_POWER_ON_MACHINE_TASK_NAME_FORMAT %
            (self.minion_pool_id, self.minion_machine_id)
        )

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_set_minion_machine_power_status'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    def test_execute(self, mock_execute, mock_add_minion_pool_event,
                     mock_set_minion_machine_power_status,
                     mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        self.minion_machine.power_status = (
            tasks.constants.MINION_MACHINE_POWER_STATUS_POWERED_OFF)
        self.task_info['minion_provider_properties'] = (
            mock_get_minion_machine.return_value.provider_properties)

        result = self.task.execute(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)
        self.assertEqual(result, self.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=True)
        mock_set_minion_machine_power_status.assert_has_calls([
            mock.call(
                mock.sentinel.context, self.minion_pool_id,
                self.minion_machine_id,
                tasks.constants.MINION_MACHINE_POWER_STATUS_POWERING_ON
            ),
            mock.call(
                mock.sentinel.context, self.minion_pool_id,
                self.minion_machine_id,
                tasks.constants.MINION_MACHINE_POWER_STATUS_POWERED_ON)
        ])
        mock_execute.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)
        mock_add_minion_pool_event.assert_called_once_with(
            mock.sentinel.context,
            f"Successfully powered on minion machine with internal pool ID "
            f"'{self.minion_machine_id}'")

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    def test_execute_power_on(self, mock_get_minion_machine):
        self.minion_machine.power_status = (
            tasks.constants.MINION_MACHINE_POWER_STATUS_POWERED_ON)
        mock_get_minion_machine.return_value = self.minion_machine

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
                result = self.task.execute(
                    mock.sentinel.context, mock.sentinel.origin,
                    mock.sentinel.destination, self.task_info)
                self.assertEqual(result, self.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=True)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    def test_execute_invalid_power_status(self, mock_get_minion_machine):
        mock_get_minion_machine.return_value = self.minion_machine
        self.minion_machine.power_status = 'INVALID_POWER_STATUS'

        self.assertRaises(
            exception.InvalidMinionMachineState, self.task.execute,
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=True)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_set_minion_machine_power_status'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin,
        '_set_minion_machine_allocation_status'
    )
    def test_execute_with_exception(
            self, mock_set_minion_machine_allocation,
            mock_get_minion_machine, mock_add_minion_pool_event,
            mock_set_minion_machine_power_status):
        mock_get_minion_machine.return_value = self.minion_machine
        mock_set_minion_machine_power_status.side_effect = (
            exception.CoriolisException)

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.WARN):
            self.assertRaises(
                exception.CoriolisException, self.task.execute,
                mock.sentinel.context, mock.sentinel.origin,
                mock.sentinel.destination, self.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=True)
        mock_add_minion_pool_event.assert_called_once_with(
            mock.sentinel.context,
            f"Exception occurred while powering on minion machine with "
            f"internal pool ID '{self.minion_machine_id}'. "
            f"The minion machine will be marked as ERROR'd "
            f"and automatically redeployed later.",
            level=tasks.constants.TASK_EVENT_ERROR
        )
        mock_set_minion_machine_allocation.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id, self.minion_machine_id,
            tasks.constants.MINION_MACHINE_STATUS_POWER_ERROR)


class PowerOffMinionMachineTaskTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis PowerOffMinionMachineTask class."""

    def setUp(self):
        super(PowerOffMinionMachineTaskTestCase, self).setUp()
        self.minion_machine = mock.MagicMock()
        self.minion_machine.id = 'test_machine_id'
        self.minion_machine.pool_id = 'test_pool_id'
        self.minion_machine.allocated_action = mock.sentinel.allocation_action
        self.minion_pool_id = 'test_pool_id'
        self.minion_machine_id = 'test_machine_id'
        self.task_info = {
            'minion_provider_properties': None,
        }
        self.minion_machine.power_status = (
            tasks.constants.MINION_MACHINE_POWER_STATUS_POWERED_ON)

        self.task = tasks.PowerOffMinionMachineTask(
            self.minion_pool_id, self.minion_machine_id, self.task_info)

    def test__get_task_name(self):
        result = self.task._get_task_name(
            self.minion_pool_id, self.minion_machine_id)

        self.assertEqual(
            result,
            tasks.MINION_POOL_POWER_OFF_MACHINE_TASK_NAME_FORMAT %
            (self.minion_pool_id, self.minion_machine_id)
        )

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_set_minion_machine_power_status'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin,
        '_set_minion_machine_allocation_status'
    )
    @mock.patch.object(tasks.BaseMinionManangerTask, 'execute')
    def test_execute(self, mock_execute, mock_set_minion_machine_allocation,
                     mock_get_minion_machine, mock_add_minion_pool_event,
                     mock_set_minion_machine_power_status):
        mock_get_minion_machine.return_value = self.minion_machine
        self.task_info['minion_provider_properties'] = (
            mock_get_minion_machine.return_value.provider_properties)

        result = self.task.execute(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)
        self.assertEqual(result, self.task_info)

        mock_execute.assert_called_once_with(
            mock.sentinel.context, mock.sentinel.origin,
            mock.sentinel.destination, self.task_info)
        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=True)
        mock_set_minion_machine_power_status.assert_has_calls([
            mock.call(
                mock.sentinel.context, self.minion_pool_id,
                self.minion_machine_id,
                tasks.constants.MINION_MACHINE_POWER_STATUS_POWERING_OFF
            ),
            mock.call(
                mock.sentinel.context, self.minion_pool_id,
                self.minion_machine_id,
                tasks.constants.MINION_MACHINE_POWER_STATUS_POWERED_OFF)
        ])
        mock_set_minion_machine_allocation.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id, self.minion_machine_id,
            tasks.constants.MINION_MACHINE_STATUS_AVAILABLE)
        mock_add_minion_pool_event.assert_called_once_with(
            mock.sentinel.context,
            f"Successfully powered off minion machine with internal pool ID "
            f"'{self.minion_machine_id}'")

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    def test_execute_power_off(self, mock_get_minion_machine):
        self.minion_machine.power_status = (
            tasks.constants.MINION_MACHINE_POWER_STATUS_POWERED_OFF)
        mock_get_minion_machine.return_value = self.minion_machine

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.DEBUG):
                result = self.task.execute(
                    mock.sentinel.context, mock.sentinel.origin,
                    mock.sentinel.destination, self.task_info)
                self.assertEqual(result, self.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=True)

    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_set_minion_machine_power_status'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_add_minion_pool_event'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin, '_get_minion_machine'
    )
    @mock.patch.object(
        tasks.MinionManagerTaskEventMixin,
        '_set_minion_machine_allocation_status'
    )
    def test_execute_with_exception(
            self, mock_set_minion_machine_allocation,
            mock_get_minion_machine, mock_add_minion_pool_event,
            mock_set_minion_machine_power_status):
        mock_get_minion_machine.return_value = self.minion_machine
        mock_set_minion_machine_power_status.side_effect = (
            exception.CoriolisException)

        with self.assertLogs('coriolis.minion_manager.rpc.tasks',
                             level=logging.WARN):
            self.assertRaises(
                exception.CoriolisException, self.task.execute,
                mock.sentinel.context, mock.sentinel.origin,
                mock.sentinel.destination, self.task_info)

        mock_get_minion_machine.assert_called_once_with(
            mock.sentinel.context, self.minion_machine_id,
            raise_if_not_found=True)
        mock_add_minion_pool_event.assert_called_once_with(
            mock.sentinel.context,
            f"Exception occurred while powering off minion machine with "
            f"internal pool ID '{self.minion_machine_id}'. "
            f"The minion machine will be marked as ERROR'd "
            f"and automatically redeployed later.",
            level=tasks.constants.TASK_EVENT_ERROR
        )
        mock_set_minion_machine_allocation.assert_called_once_with(
            mock.sentinel.context, self.minion_pool_id, self.minion_machine_id,
            tasks.constants.MINION_MACHINE_STATUS_POWER_ERROR)
