# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import re
import uuid
import ddt
from unittest import mock

from coriolis.conductor.rpc import server
from coriolis import constants, exception
from coriolis.tests import test_base, testutils


@ddt.ddt
class ConductorServerEndpointTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Conductor RPC server."""

    def setUp(self):
        super(ConductorServerEndpointTestCase, self).setUp()
        self.server = server.ConductorServerEndpoint()

    @mock.patch.object(server.ConductorServerEndpoint, '_create_task')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_check_replica_running_executions')
    @mock.patch.object(server.ConductorServerEndpoint, '_get_replica')
    def test_delete_replica_disks_invalid_state(self, mock_get_replica,
                                                mock_check_replica_running,
                                                mock_create_task):
        mock_replica = mock_get_replica.return_value
        mock_replica.instances = [mock.sentinel.instance]
        mock_replica.info = {}
        delete_replica_disks = testutils.get_wrapped_function(
            self.server.delete_replica_disks)

        self.assertRaises(exception.InvalidReplicaState,
                          delete_replica_disks,
                          self.server, mock.sentinel.context, mock.sentinel.replica_id)

        mock_get_replica.assert_called_once_with(mock.sentinel.context,
                                                 mock.sentinel.replica_id)
        mock_check_replica_running.assert_called_once_with(
            mock.sentinel.context, mock_replica)
        mock_create_task.assert_not_called()

    @ddt.file_data('data/execution_tasks_config.yml')
    @ddt.unpack
    def test_check_execution_tasks_sanity(self, tasks_config, init_task_info, expected_result):
        def convertToTask(task_config):
            instance_task = mock.Mock()
            instance_task.instance = task_config.get(
                'instance', mock.sentinel.instance)
            instance_task.id = task_config.get('id', str(uuid.uuid4()))
            instance_task.status = task_config.get(
                'status', constants.TASK_STATUS_SCHEDULED)
            instance_task.depends_on = task_config.get('depends_on', None)
            instance_task.task_type = task_config.get(
                'task_type', constants.TASK_TYPE_DEPLOY_MIGRATION_SOURCE_RESOURCES)
            return instance_task

        execution = mock.sentinel.execution
        execution.id = str(uuid.uuid4())
        execution.type = mock.sentinel.execution_type

        execution.tasks = [convertToTask(t) for t in tasks_config]

        if init_task_info != None:
            initial_task_info = init_task_info
        else:
            initial_task_info = {mock.sentinel.instance: {
                'source_environment': mock.sentinel.source_environment,
                'export_info': mock.sentinel.export_info
            }}

        if not expected_result:
            self.server._check_execution_tasks_sanity(
                execution, initial_task_info)
        else:
            exception_mappings = {
                'INVALID_STATE': exception.InvalidTaskState,
                'MISSING_PARAMS': exception.TaskParametersException,
                'MISSING_DEPENDENCIES': exception.TaskDependencyException,
                'FIELDS_CONFLICT': exception.TaskFieldsConflict,
                'DEADLOCK': exception.ExecutionDeadlockException,
            }
            with self.assertRaisesRegex(exception_mappings[expected_result['type']], expected_result.get('message', "")):
                self.server._check_execution_tasks_sanity(
                    execution, initial_task_info)
