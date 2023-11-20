# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

import ddt

from coriolis import constants
from coriolis import events
from coriolis.tests import test_base


@ddt.ddt
class EventManagerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis EventManager class."""

    def setUp(self):
        super(EventManagerTestCase, self).setUp()
        self.mock_event_handler = mock.Mock()
        self.event_manager = events.EventManager(self.mock_event_handler)

    def test_call_event_handler(self):
        mock_method = mock.Mock()
        self.mock_event_handler.test_method = mock_method

        result = events.EventManager._call_event_handler(
            self.event_manager, 'test_method', 'arg1', 'arg2', kwarg1='value1')

        mock_method.assert_called_once_with('arg1', 'arg2', kwarg1='value1')
        self.assertEqual(result, mock_method.return_value)

    def test_call_event_handler_method_not_exist(self):
        mock_event_handler = mock.Mock(spec_set=[])
        event_manager = events.EventManager(mock_event_handler)

        self.assertRaises(AttributeError, event_manager._call_event_handler,
                          'non_existent_method', 'arg1', 'arg2',
                          kwarg1='value1')

    def test_call_event_handler_no_event_handler(self):
        self.event_manager._event_handler = None
        result = self.event_manager._call_event_handler('test_method')
        self.assertEqual(result, None)

    @ddt.data(
        {'message': 'test-message',
         'total_steps': -10,
         'initial_step': 0,
         'expected_log_level': logging.WARN},
        {'message': 'test-message',
         'total_steps': 0,
         'initial_step': 0,
         'expected_log_level': logging.WARN},
        {'message': 'test-message',
         'total_steps': 50,
         'initial_step': 100,
         'expected_exception': ValueError},
        {'message': 'test-message',
         'total_steps': 100,
         'initial_step': 100,
         'expected_perc': 100,
         'expected_initial_step': 100,
         'expected_total_steps': 100},
        {'message': 'test-message',
         'total_steps': 100,
         'initial_step': 50,
         'expected_perc': 50,
         'expected_initial_step': 50,
         'expected_total_steps': 100},
        {'message': 'test-message',
         'total_steps': 0,
         'initial_step': 0,
         'expected_perc': 0,
         'expected_initial_step': 0,
         'expected_total_steps': 0})
    def test_add_percentage_step(self, test_data):
        message = test_data['message']
        total_steps = test_data['total_steps']
        initial_step = test_data['initial_step']

        if 'expected_log_level' in test_data:
            with self.assertLogs('coriolis.events',
                                 level=test_data['expected_log_level']):
                self.event_manager.add_percentage_step(message, total_steps,
                                                       initial_step)
        elif 'expected_exception' in test_data:
            self.assertRaises(test_data['expected_exception'],
                              self.event_manager.add_percentage_step,
                              message, total_steps, initial_step)
        else:
            perc_step_data = self.event_manager.add_percentage_step(
                message, total_steps, initial_step)
            self.assertEqual(perc_step_data.last_perc,
                             test_data['expected_perc'])
            self.assertEqual(perc_step_data.last_value,
                             test_data['expected_initial_step'])
            self.assertEqual(perc_step_data.total_steps,
                             test_data['expected_total_steps'])

    def test_set_percentage_step(self):
        perc_step_data = events._PercStepData(
            mock.sentinel.progress_update_id, 50, 0, 100)
        self.event_manager._perc_steps[
            mock.sentinel.progress_update_id] = perc_step_data

        self.event_manager.set_percentage_step(perc_step_data, 60)

        expected_perc_steps = {
            mock.sentinel.progress_update_id: events._PercStepData(
                mock.sentinel.progress_update_id, 60, 0, 100)}
        self.assertEqual(self.event_manager._perc_steps, expected_perc_steps)

    def test_set_percentage_step_no_perc_step(self):
        self.event_manager.set_percentage_step(
            events._PercStepData(
                mock.sentinel.progress_update_id, 60, 0, 100), 60)

        self.assertEqual(self.event_manager._perc_steps, {})

    def test_set_percentage_step_rollback(self):
        self.mock_event_handler.add_progress_update.return_value = (
            mock.sentinel.progress_update)
        self.mock_event_handler.get_progress_update_identifier.return_value = (
            mock.sentinel.progress_update_id)

        self.event_manager.add_percentage_step('test-message', 100, 50)
        with self.assertLogs('coriolis.events', level=logging.WARN):
            result = self.event_manager.set_percentage_step(
                events._PercStepData(
                    mock.sentinel.progress_update_id, 60, 0, 100), 40)
        self.assertEqual(result, None)

    def test_progress_update(self):
        self.event_manager.progress_update('test-message')
        self.mock_event_handler.add_progress_update.assert_called_once_with(
            'test-message', return_event=False)

    def test_info(self):
        self.event_manager.info('test-message')
        self.mock_event_handler.add_event.assert_called_once_with(
            'test-message', level=constants.TASK_EVENT_INFO)

    def test_warn(self):
        self.event_manager.warn('test-message')
        self.mock_event_handler.add_event.assert_called_once_with(
            'test-message', level=constants.TASK_EVENT_WARNING)

    def test_error(self):
        self.event_manager.error('test-message')
        self.mock_event_handler.add_event.assert_called_once_with(
            'test-message', level=constants.TASK_EVENT_ERROR)
