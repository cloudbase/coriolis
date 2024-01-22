# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.


import eventlet
import logging
import multiprocessing
import sys
from unittest import mock

from six.moves import queue

from coriolis.taskflow import runner
from coriolis.tests import test_base


class CoriolisTestException(Exception):
    pass


class TaskFlowRunnerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis TaskFlow class."""

    def setUp(self):
        super(TaskFlowRunnerTestCase, self).setUp()
        self.runner = runner.TaskFlowRunner(
            'svc_name', runner.TASKFLOW_EXECUTION_ORDER_PARALLEL,
            runner.TASKFLOW_EXECUTOR_THREADED, 1)
        self.mock_flow = mock.Mock()
        self.mock_engine = mock.Mock()
        self.mock_mp_log_q = mock.Mock()
        self.store = {'key': 'value'}

    def test_log_flow_transition(self):
        state = 'new_state'
        details = {
            'flow_name': 'flow_name',
            'flow_uuid': 'flow_uuid',
            'old_state': 'old_state'
        }

        with self.assertLogs('coriolis.taskflow.runner', level=logging.DEBUG):
            self.runner._log_flow_transition(state, details)

    def test_log_task_transition(self):
        state = 'new_state'
        details = {
            'task_name': 'task_name',
            'task_uuid': 'task_uuid',
            'old_state': 'old_state'
        }

        with self.assertLogs('coriolis.taskflow.runner', level=logging.DEBUG):
            self.runner._log_task_transition(state, details)

    @mock.patch('taskflow.engines.load')
    def test_setup_engine_for_flow(self, mock_load):
        result = self.runner._setup_engine_for_flow(self.mock_flow)

        mock_load.assert_called_once_with(
            self.mock_flow, None, executor=self.runner._executor,
            engine=self.runner._execution_order,
            max_workers=self.runner._max_workers)

        engine = mock_load.return_value

        engine.notifier.register.assert_called_once_with(
            mock.ANY, self.runner._log_flow_transition)
        engine.atom_notifier.register.assert_called_once_with(
            mock.ANY, self.runner._log_task_transition)

        self.assertEqual(engine, result)

    def test__run_flow(self):
        self.mock_flow.name = 'flow_name'

        self.runner._setup_engine_for_flow = mock.Mock()
        self.runner._setup_engine_for_flow.return_value = self.mock_engine
        self.mock_engine.run = mock.Mock()

        self.runner._run_flow(self.mock_flow)
        self.mock_engine.compile.assert_called_once()
        self.mock_engine.prepare.assert_called_once()
        self.mock_engine.run.assert_called_once()

    @mock.patch('coriolis.utils.get_exception_details')
    def test_run_flow_exception(self, mock_get_exception_details):
        self.mock_flow.name = 'flow_name'

        self.runner._setup_engine_for_flow = mock.Mock()
        self.runner._setup_engine_for_flow.return_value = self.mock_engine
        self.mock_engine.run = mock.Mock()
        self.mock_engine.run.side_effect = CoriolisTestException()

        with self.assertLogs('coriolis.taskflow.runner', level=logging.WARN):
            self.assertRaises(CoriolisTestException, self.runner._run_flow,
                              self.mock_flow)
        self.mock_engine.compile.assert_called_once()
        self.mock_engine.prepare.assert_called_once()
        self.mock_engine.run.assert_called_once()
        mock_get_exception_details.assert_called_once()

    @mock.patch.object(runner.TaskFlowRunner, '_run_flow')
    def test_run_flow(self, mock_run_flow):
        self.runner.run_flow(self.mock_flow)
        mock_run_flow.assert_called_once_with(self.mock_flow, store=None)

    @mock.patch('oslo_config.cfg.CONF')
    @mock.patch('coriolis.utils.setup_logging')
    @mock.patch('oslo_log.log.getLogger')
    @mock.patch('logging.handlers.QueueHandler')
    def test_setup_task_process_logging(self, mock_queue_handler,
                                        mock_get_logger, mock_setup_logging,
                                        mock_conf):
        mock_handler = mock.Mock()
        mock_get_logger.return_value.logger.handlers = [mock_handler]

        self.runner._setup_task_process_logging(self.mock_mp_log_q)
        mock_conf.assert_called_once_with(sys.argv[1:], project='coriolis',
                                          version='1.0.0')
        mock_setup_logging.assert_called_once()
        mock_get_logger.assert_called_once_with(None)
        mock_queue_handler.assert_called_once_with(self.mock_mp_log_q)
        mock_get_logger.return_value.logger.removeHandler.\
            assert_called_once_with(mock_handler)
        mock_get_logger.return_value.logger.addHandler.assert_called_once_with(
            mock_queue_handler.return_value)

    @mock.patch.object(runner.TaskFlowRunner, '_setup_task_process_logging')
    @mock.patch.object(runner.TaskFlowRunner, '_run_flow')
    def test_run_flow_in_process(self, mock_run_flow, mock_setup_logging):
        self.runner._run_flow_in_process(self.mock_flow, self.mock_mp_log_q,
                                         store=self.store)

        mock_setup_logging.assert_called_once_with(self.mock_mp_log_q)
        mock_run_flow.assert_called_once_with(self.mock_flow, store=self.store)

    @mock.patch.object(logging, 'getLogger')
    def test__handle_mp_log_events(self, mock_get_logger):
        mock_p = mock.Mock()
        self.mock_mp_log_q.get.side_effect = [mock.sentinel.record,
                                              queue.Empty, None]
        mock_p.is_alive.return_value = True

        result = self.runner._handle_mp_log_events(mock_p, self.mock_mp_log_q)
        mock_get_logger.assert_called_once_with(
            mock.sentinel.record.name)
        self.assertIsNone(result)
        mock_get_logger.return_value.handle.assert_called_with(
            mock.sentinel.record)

    def test__handle_mp_log_events_process_dead(self):
        mock_p = mock.Mock()
        self.mock_mp_log_q.get.side_effect = queue.Empty
        mock_p.is_alive.return_value = False

        result = self.runner._handle_mp_log_events(mock_p, self.mock_mp_log_q)
        self.assertIsNone(result)

    @mock.patch.object(eventlet, 'spawn')
    @mock.patch.object(multiprocessing, "get_context")
    def test_spawn_process_flow(self, mock_get_context, mock_spawn):
        mock_process = mock.Mock()
        mock_mp_ctx = mock.Mock()
        mock_mp_ctx.Process.return_value = mock_process
        mock_get_context.return_value = mock_mp_ctx

        self.runner._run_flow_in_process = mock.Mock()
        result = self.runner._spawn_process_flow(self.mock_flow,
                                                 store=self.store)
        self.assertIsNone(result)

        mock_get_context.assert_called_once_with('spawn')
        mock_mp_ctx.Process.assert_called_once_with(
            target=self.runner._run_flow_in_process,
            args=(self.mock_flow, mock_mp_ctx.Queue.return_value, self.store))
        mock_process.start.assert_called_once_with()
        mock_spawn.assert_called_once_with(self.runner._handle_mp_log_events,
                                           mock_process,
                                           mock_mp_ctx.Queue.return_value)

    @mock.patch.object(runner.TaskFlowRunner, '_spawn_process_flow')
    def test_run_flow_in_background(self, mock_spawn_process_flow):
        self.runner.run_flow_in_background(self.mock_flow, store=self.store)
        mock_spawn_process_flow.assert_called_once_with(self.mock_flow,
                                                        store=self.store)
