# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.


import logging
from unittest import mock

from taskflow.types import failure

from coriolis import exception
from coriolis.scheduler.rpc.client import SchedulerClient
from coriolis.taskflow import base
from coriolis.tests import test_base
from coriolis.worker.rpc import client as rpc_client


class CoriolisTestException(Exception):
    pass


class BaseCoriolisTaskflowTaskTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis BaseCoriolisTaskflowTask class."""

    class ConcreteTask(base.BaseCoriolisTaskflowTask):
        def execute(self):
            pass

    def setUp(self):
        super(BaseCoriolisTaskflowTaskTestCase, self).setUp()
        self.task = self.ConcreteTask()
        self.task_failure = mock.Mock()
        self.flow_failures = mock.Mock()
        self.mock_instance = mock.Mock()

    def test_get_error_str_for_flow_failures(self):
        self.task_failure.exception_str = "Exception"
        self.task_failure.traceback_str = "Traceback"

        result = self.task._get_error_str_for_flow_failures(
            {"task1": self.task_failure}, full_tracebacks=True)
        self.assertEqual(result, " Traceback for task 'task1': Traceback")

    def test_get_error_str_for_flow_no_failures(self):
        result = self.task._get_error_str_for_flow_failures(
            None, full_tracebacks=False)
        self.assertEqual(result, "No flow failures provided.")

    def test_get_error_str_for_flow_failures_no_items(self):
        self.flow_failures.items.return_value = False
        result = self.task._get_error_str_for_flow_failures(
            self.flow_failures, full_tracebacks=False)
        self.assertEqual(result, "No flow failures present.")

    def test_get_error_str_for_flow_failures_task_process_exception(self):
        self.task_failure.exception_str = "Exception\nTraceback"
        self.task_failure.traceback_str = None
        self.task_failure.exception = exception.TaskProcessException()

        result = self.task._get_error_str_for_flow_failures(
            {"task1": self.task_failure}, full_tracebacks=False)
        self.assertEqual(result, " Error message for task 'task1': Traceback")

    def test_get_error_str_for_flow_failures_exception_multiline(self):
        self.task_failure.exception_str = "Line 1\nLine 2\nLine 3"
        self.task_failure.traceback_str = None
        self.task_failure.exception = exception.TaskProcessException()

        result = self.task._get_error_str_for_flow_failures(
            {"task1": self.task_failure}, full_tracebacks=False)
        self.assertEqual(result, " Error message for task 'task1': Line 2")

    def test_revert(self):
        with self.assertLogs('coriolis.taskflow.base', level=logging.ERROR):
            self.task.revert(self.mock_instance, mock.ANY)

    def test_revert_with_error(self):
        mock_failure = mock.Mock(spec=failure.Failure)
        mock_failure.traceback_str = "Mock traceback"
        with self.assertLogs('coriolis.taskflow.base', level=logging.ERROR):
            self.task.revert(result=mock_failure)


class BaseRunWorkerTaskTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis BaseRunWorkerTask class."""

    def setUp(self):
        super(BaseRunWorkerTaskTestCase, self).setUp()
        self.task = base.BaseRunWorkerTask(
            'test_task', 'test_task_id', 'test-task_instance',
            'test_runner_type')
        self.kwargs = {'test_task_id': 'test_task_id'}
        self.depends_on = ['test_task_id']
        self.mock_task_runner = mock.Mock()
        self.mock_worker_rpc = mock.Mock()
        self.mock_cleanup_task_runner = mock.Mock()

    def test_scheduler_client_property(self):
        mock_scheduler_client = mock.Mock(spec=SchedulerClient)

        with mock.patch('coriolis.scheduler.rpc.client.SchedulerClient',
                        return_value=mock_scheduler_client):

            self.assertEqual(self.task._scheduler_client,
                             mock_scheduler_client)

    def test_scheduler_client_already_set(self):
        mock_scheduler_client = mock.Mock(spec=SchedulerClient)
        self.task._scheduler_client_instance = mock_scheduler_client

        with mock.patch('coriolis.scheduler.rpc.client.SchedulerClient') as \
                mock_SchedulerClient:
            self.assertEqual(
                self.task._scheduler_client, mock_scheduler_client)
            mock_SchedulerClient.assert_not_called()

    def test_set_provides_for_dependencies(self):
        self.task._set_provides_for_dependencies(self.kwargs)
        self.assertEqual(self.kwargs,
                         {'test_task_id': 'test_task_id',
                          'provides': ['task-test_task-result']})

    def test_set_provides_for_dependencies_with_provides_key(self):
        kwargs = {'provides': ['test']}
        self.task._set_provides_for_dependencies(kwargs)
        self.assertEqual(kwargs, {'provides':
                                  ['test', 'task-test_task-result']})

    def test_set_requires_for_dependencies(self):
        self.task._set_requires_for_dependencies(self.kwargs, self.depends_on)
        self.assertEqual(self.kwargs,
                         {'test_task_id': 'test_task_id',
                          'requires': ['task-test_task_id-result']})

    def test_set_requires_for_dependencies_with_requires_key(self):
        kwargs = {'requires': ['test']}
        self.task._set_requires_for_dependencies(kwargs, self.depends_on)
        self.assertEqual(kwargs,
                         {'requires': ['test', 'task-test_task_id-result']})

    @mock.patch('coriolis.tasks.factory.get_task_runner_class')
    def test_set_requires_for_task_info_fields(self, mock_get_task_runner):
        self.mock_task_runner.get_required_task_info_properties.\
            return_value = ['prop1']
        mock_get_task_runner.return_value = self.mock_task_runner
        kwargs = {'requires': ['test']}

        self.task._set_requires_for_task_info_fields(kwargs)
        self.assertEqual(kwargs, {'requires': ['test', 'prop1']})

    @mock.patch('coriolis.tasks.factory.get_task_runner_class')
    def test_set_requires_for_task_info_fields_with_cleanup(
            self, mock_get_task_runner):
        self.mock_task_runner.get_required_task_info_properties.\
            return_value = ['main_task_dep1', 'main_task_dep2']
        self.mock_task_runner.get_returned_task_info_properties.\
            return_value = ['main_task_dep1']

        self.mock_cleanup_task_runner.get_required_task_info_properties.\
            return_value = ['cleanup_task_dep1', 'cleanup_task_dep2']

        mock_get_task_runner.side_effect = [self.mock_task_runner,
                                            self.mock_cleanup_task_runner]

        self.task._cleanup_task_runner_type = 'cleanup_runner_type'
        kwargs = {'requires': ['test']}

        self.task._set_requires_for_task_info_fields(kwargs)
        self.assertEqual(sorted(kwargs['requires']),
                         sorted(['test', 'main_task_dep1', 'main_task_dep2',
                                 'cleanup_task_dep1', 'cleanup_task_dep2']))

    @mock.patch('coriolis.tasks.factory.get_task_runner_class')
    def test_set_provides_for_task_info_fields(self, mock_get_task_runner):
        self.mock_task_runner.get_returned_task_info_properties.\
            return_value = ['prop1']
        mock_get_task_runner.return_value = self.mock_task_runner
        kwargs = {'provides': ['test']}

        self.task._set_provides_for_task_info_fields(kwargs)
        self.assertEqual(kwargs, {'provides': ['test', 'prop1']})

    @mock.patch('coriolis.tasks.factory.get_task_runner_class')
    def test_set_provides_for_task_info_fields_with_cleanup(
            self, mock_get_task_runner):
        self.mock_task_runner.get_returned_task_info_properties.\
            return_value = ['main_task_dep1', 'main_task_dep2']
        self.mock_task_runner.get_required_task_info_properties.\
            return_value = ['main_task_dep1']

        self.mock_cleanup_task_runner.get_returned_task_info_properties.\
            return_value = ['cleanup_task_dep1', 'cleanup_task_dep2']

        mock_get_task_runner.side_effect = [self.mock_task_runner,
                                            self.mock_cleanup_task_runner]

        self.task._cleanup_task_runner_type = 'cleanup_runner_type'
        kwargs = {'provides': ['test']}

        self.task._set_provides_for_task_info_fields(kwargs)

        self.assertEqual(sorted(kwargs['provides']),
                         sorted(['test', 'main_task_dep1', 'main_task_dep2',
                                 'cleanup_task_dep1', 'cleanup_task_dep2']))

    @mock.patch.object(rpc_client.WorkerClient, 'from_service_definition')
    @mock.patch.object(SchedulerClient, 'get_worker_service_for_task')
    def test_get_worker_service_rpc_for_task(self, mock_get_worker_service,
                                             mock_from_service_definition):
        mock_get_worker_service.return_value = {'id': 'worker_service_id'}
        mock_from_service_definition.return_value = 'worker_client'

        worker_client = self.task._get_worker_service_rpc_for_task(
            'ctxt', 'task_id', 'task_type', 'origin', 'destination')

        self.assertEqual(worker_client, 'worker_client')

        mock_get_worker_service.assert_called_once_with(
            'ctxt', {'id': 'task_id', 'task_type': 'task_type'}, 'origin',
            'destination', retry_count=5, retry_period=2, random_choice=True)
        mock_from_service_definition.assert_called_once_with({
            'id': 'worker_service_id'},
            timeout=base.CONF.taskflow.worker_task_execution_timeout)

    @mock.patch.object(
        base.BaseRunWorkerTask,
        '_get_worker_service_rpc_for_task'
    )
    def test_execute_task(self, mock_get_worker_rpc):
        mock_get_worker_rpc.return_value = self.mock_worker_rpc

        result = self.task._execute_task('ctxt', 'task_id', 'task_type',
                                         'origin', 'destination', 'task_info')

        self.assertEqual(result, self.mock_worker_rpc.run_task.return_value)

        mock_get_worker_rpc.assert_called_once_with(
            'ctxt', 'test_task_id', 'task_type', 'origin', 'destination')
        self.mock_worker_rpc.run_task.assert_called_once_with(
            'ctxt', 'test_task_id', 'task_type', 'origin', 'destination',
            'test-task_instance', 'task_info')

    @mock.patch.object(
        base.BaseRunWorkerTask,
        '_get_worker_service_rpc_for_task'
    )
    def test_execute_task_exception(self, mock_get_worker_rpc):
        mock_get_worker_rpc.return_value = self.mock_worker_rpc
        self.mock_worker_rpc.run_task.side_effect = CoriolisTestException()

        self.assertRaises(CoriolisTestException, self.task._execute_task,
                          'ctxt', 'task_id', 'task_type', 'origin',
                          'destination', 'task_info')

        mock_get_worker_rpc.assert_called_once_with(
            'ctxt', 'test_task_id', 'task_type', 'origin', 'destination')
        self.mock_worker_rpc.run_task.assert_called_once_with(
            'ctxt', 'test_task_id', 'task_type', 'origin', 'destination',
            'test-task_instance', 'task_info')

    @mock.patch.object(base.BaseRunWorkerTask, '_execute_task')
    def test_execute(self, mock_execute_task):
        result = self.task.execute('ctxt', 'origin', 'destination',
                                   'task_info')

        self.assertEqual(result, mock_execute_task.return_value)
        mock_execute_task.assert_called_once_with(
            'ctxt', 'test_task_id', 'test_runner_type', 'origin',
            'destination', 'task_info')

    @mock.patch.object(base.BaseRunWorkerTask, '_execute_task')
    def test_revert(self, mock_execute_task):
        self.task._cleanup_task_runner_type = 'cleanup_task_type'

        result = self.task.revert('ctxt', 'origin', 'destination', 'task_info')

        self.assertEqual(result, None)

        mock_execute_task.assert_called_once_with(
            'ctxt', 'test_task_id', 'cleanup_task_type', 'origin',
            'destination', 'task_info')

    def test_revert_no_cleanup_task_runner_type(self):
        result = self.task.revert('ctxt', 'origin', 'destination', 'task_info')

        self.assertEqual(result, None)

    @mock.patch.object(base.BaseRunWorkerTask, '_execute_task')
    def test_revert_with_exception(self, mock_execute_task):
        mock_execute_task.side_effect = CoriolisTestException()

        self.task._cleanup_task_runner_type = 'cleanup_task_type'
        with self.assertLogs('coriolis.taskflow.base', level=logging.WARN):
            self.task.revert('ctxt', 'origin', 'destination', 'task_info')

        mock_execute_task.assert_called_once_with(
            'ctxt', 'test_task_id', 'cleanup_task_type', 'origin',
            'destination', 'task_info')

    @mock.patch.object(base.BaseRunWorkerTask, '_execute_task')
    def test_revert_with_exception_and_raise_on_cleanup_failure(
            self, mock_execute_task):
        mock_execute_task.side_effect = CoriolisTestException()

        self.task._cleanup_task_runner_type = 'cleanup_task_type'

        self.task._raise_on_cleanup_failure = True
        self.assertRaises(CoriolisTestException, self.task.revert, 'ctxt',
                          'origin', 'destination', 'task_info')
