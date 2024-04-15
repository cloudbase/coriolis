# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import datetime
import ddt
import eventlet
import schedule
import sys
import time
from unittest import mock

from coriolis.cron import cron
from coriolis import exception
from coriolis import schemas
from coriolis.tests import test_base


class CoriolisTestException(Exception):
    pass


@ddt.ddt
class CronJobTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis CronJob."""

    @mock.patch.object(schemas, 'validate_value')
    def setUp(self, *_):
        super(CronJobTestCase, self).setUp()
        mock_on_success = mock.Mock()
        mock_on_error = mock.Mock()
        mock_job_callable = mock.Mock()
        args = ['arg1', 'arg2']
        kw = {
            "arg3": "mock_arg3",
            "arg4": "mock_arg4"
        }
        self.cron = cron.CronJob(
            mock.sentinel.name,
            mock.sentinel.description,
            mock.sentinel.schedule,
            True,
            datetime.datetime.fromisoformat('2099-01-01'),
            mock_on_success,
            mock_on_error,
            mock_job_callable,
            *args,
            **kw
        )

    @mock.patch.object(schemas, 'validate_value')
    def test__init__(self, mock_validate_value):
        mock_on_success = mock.Mock()
        mock_on_error = mock.Mock()
        mock_job_callable = mock.Mock()
        args = ['arg1', 'arg2']

        self.cron = cron.CronJob(
            mock.sentinel.name,
            mock.sentinel.description,
            mock.sentinel.schedule,
            True,
            datetime.datetime.fromisoformat('2099-01-01'),
            mock_on_success,
            mock_on_error,
            mock_job_callable,
            args
        )

        self.assertIsInstance(self.cron, cron.CronJob)
        mock_validate_value.assert_called_once_with(
            mock.sentinel.schedule,
            schemas.SCHEDULE_API_BODY_SCHEMA["properties"]["schedule"]
        )

    @mock.patch.object(schemas, 'validate_value')
    def test__init__raises(self, mock_validate_value):
        mock_on_success = mock.Mock()
        mock_on_error = mock.Mock()
        mock_job_callable = mock.Mock()
        args = ['arg1', 'arg2']

        self.assertRaises(
            exception.CoriolisException,
            cron.CronJob,
            mock.sentinel.name,
            mock.sentinel.description,
            mock.sentinel.schedule,
            True,
            datetime.date.fromisoformat('2099-01-01'),
            mock_on_success,
            mock_on_error,
            mock_job_callable,
            args,
            mock.sentinel.kw
        )

        mock_validate_value.assert_called_once_with(
            mock.sentinel.schedule,
            schemas.SCHEDULE_API_BODY_SCHEMA["properties"]["schedule"]
        )

        mock_job_callable = "invalid_job"
        mock_validate_value.reset_mock()

        self.assertRaises(
            exception.CoriolisException,
            cron.CronJob,
            mock.sentinel.name,
            mock.sentinel.description,
            mock.sentinel.schedule,
            True,
            datetime.datetime.fromisoformat('2099-01-01'),
            mock_on_success,
            mock_on_error,
            mock_job_callable,
            args,
            mock.sentinel.kw
        )

        mock_validate_value.assert_not_called()

        mock_job_callable = mock.Mock()
        mock_on_success = "invalid"

        self.assertRaises(
            ValueError,
            cron.CronJob,
            mock.sentinel.name,
            mock.sentinel.description,
            mock.sentinel.schedule,
            True,
            datetime.datetime.fromisoformat('2099-01-01'),
            mock_on_success,
            mock_on_error,
            mock_job_callable,
            args,
            mock.sentinel.kw
        )

        mock_on_success = mock.Mock()
        mock_on_error = "invalid"

        self.assertRaises(
            ValueError,
            cron.CronJob,
            mock.sentinel.name,
            mock.sentinel.description,
            mock.sentinel.schedule,
            True,
            datetime.datetime.fromisoformat('2099-01-01'),
            mock_on_success,
            mock_on_error,
            mock_job_callable,
            args,
            mock.sentinel.kw
        )

    @ddt.data(
        {
            'pairs': [("mock_field1", "mock_field2")],
            'expected_result': [False]
        },
        {
            'pairs': [("mock_field1", "mock_field1")],
            'expected_result': [True]
        },
        {
            'pairs': [("mock_field1", None)],
            'expected_result': [True]
        },
        {
            'pairs': [(None, "mock_field2")],
            'expected_result': [False]
        },
        {
            'pairs': [(None, None)],
            'expected_result': [True]
        },
    )
    def test_compare(self, data):
        pairs = data['pairs']

        result = self.cron._compare(pairs)

        self.assertEqual(
            data['expected_result'],
            result
        )

    def test_is_expired(self):
        self.cron._expires = datetime.datetime.fromisoformat('2000-01-01')

        result = self.cron.is_expired()

        self.assertEqual(
            True,
            result
        )

        self.cron._expires = datetime.datetime.fromisoformat('2099-01-01')

        result = self.cron.is_expired()

        self.assertEqual(
            False,
            result
        )

        self.cron._expires = None

        result = self.cron.is_expired()

        self.assertEqual(
            False,
            result
        )

    @mock.patch.object(cron.CronJob, 'is_expired')
    @ddt.data(
        {
            'schedule': {
                'year': 4,  # year is not in SCHEDULE_FIELDS
                'month': 1,
                'hour': 0
            },
            'expected_result': True
        },
        {
            'schedule': {
                'month': 2,
                'hour': 0
            },
            'expected_result': False
        },
        {
            'schedule': {
                'month': 1,
                'hour': 1
            },
            'expected_result': False
        }
    )
    def test_should_run(self, data, mock_is_expired):
        mock_is_expired.return_value = False
        dt = datetime.datetime.fromisoformat('2099-01-01')
        self.cron._enabled = True
        self.cron.schedule = data['schedule']

        result = self.cron.should_run(dt)

        self.assertEqual(
            data['expected_result'],
            result
        )

    @mock.patch.object(cron.CronJob, '_compare')
    @mock.patch.object(cron.CronJob, 'is_expired')
    def test_should_run_false(self, mock_is_expired, mock_compare):
        mock_is_expired.return_value = True
        dt = datetime.datetime.fromisoformat('2099-01-01')
        self.cron._enabled = True
        self.cron.schedule = {
            'month': 1,
            'hour': 0
        }

        self.assertRaises(
            exception.CoriolisException,
            self.cron.should_run,
            None
        )

        result = self.cron.should_run(dt)

        self.assertEqual(
            False,
            result
        )

        mock_is_expired.return_value = False
        self.cron._enabled = False

        result = self.cron.should_run(dt)

        self.assertEqual(
            False,
            result
        )

        mock_compare.assert_not_called()

    def test_send_status(self):
        queue = mock.Mock()
        self.cron._send_status(queue, mock.sentinel.status)
        queue.put.assert_called_once_with(mock.sentinel.status)

        queue.reset_mock()
        self.cron._send_status(None, mock.sentinel.status)
        queue.put.assert_not_called()

    @mock.patch.object(cron.CronJob, '_send_status')
    def test_start(self, mock_send_status):

        self.cron.start()

        self.cron._func.assert_called_once_with(
            *self.cron._args, **self.cron._kw)
        self.cron._on_success.assert_called_once_with(
            self.cron._func.return_value)
        mock_send_status.assert_called_once_with(
            None,
            {"result": self.cron._func.return_value,
             "description": self.cron._description,
             "name": self.cron.name,
             "error_info": None}
        )

    @mock.patch.object(cron, 'LOG')
    @mock.patch.object(sys, 'exc_info')
    @mock.patch.object(cron.CronJob, '_send_status')
    def test_start_on_error(
        self,
        mock_send_status,
        mock_exc_info,
        mock_LOG
    ):
        mock_exc_info.return_value = "mock_exc_info"
        self.cron._func.side_effect = Exception('err_msg1')
        self.cron._on_error.side_effect = Exception('err_msg2')

        self.cron.start()

        self.cron._func.assert_called_once_with(
            *self.cron._args, **self.cron._kw)
        self.cron._on_error.assert_called_once_with("mock_exc_info")
        mock_send_status.assert_called_once_with(
            None,
            {"result": None,
             "description": self.cron._description,
             "name": self.cron.name,
             "error_info": "mock_exc_info"}
        )
        mock_LOG.assert_has_calls([
            mock.call.exception(self.cron._func.side_effect),
            mock.call.exception(self.cron._on_error.side_effect)
        ])


@ddt.ddt
class CronTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Cron."""

    @mock.patch.object(schemas, 'validate_value')
    def setUp(self, *_):
        super(CronTestCase, self).setUp()
        self.cron = cron.Cron()

    @mock.patch.object(schemas, 'validate_value')
    def test_register(self, *_):
        job = cron.CronJob(
            mock.sentinel.name,
            mock.sentinel.description,
            mock.sentinel.schedule,
            False,
            datetime.datetime.fromisoformat('2099-01-01'),
            mock.Mock(),
            mock.Mock(),
            mock.Mock()
        )
        self.cron.register(job)
        self.assertEqual(
            self.cron._jobs[mock.sentinel.name],
            job
        )

    def test_register_no_job(self):
        self.assertRaises(
            ValueError,
            self.cron.register,
            None
        )

    def test_unregister(self):
        self.cron._jobs = {
            'job_1': 'mock_job',
            'job_2': 'mock_job'
        }
        self.cron.unregister('job_1')
        self.assertEqual(
            {'job_2': 'mock_job'},
            self.cron._jobs
        )

    def test_unregister_jobs_with_prefix(self):
        self.cron._jobs = {
            'pre1_job_1': 'mock_job',
            'pre1_job_2': 'mock_job',
            'pre2_job_3': 'mock_job'
        }
        self.cron.unregister_jobs_with_prefix('pre1')
        self.assertEqual(
            {'pre2_job_3': 'mock_job'},
            self.cron._jobs
        )

    @mock.patch.object(eventlet, 'spawn')
    def test_check_jobs(self, mock_spawn):
        mock_job = mock.Mock()
        mock_job2 = mock.Mock()
        mock_job2.should_run.return_value = False
        self.cron._jobs = {
            'job1': mock_job,
            'job2': mock_job2,
        }

        self.cron._check_jobs()

        mock_spawn.assert_called_once_with(mock_job.start, self.cron._queue)

    @mock.patch.object(time, 'sleep')
    @mock.patch.object(schedule, 'run_pending')
    def test_loop(self, mock_run_pending, mock_sleep):
        mock_sleep.side_effect = [None, CoriolisTestException()]

        self.assertRaises(
            CoriolisTestException,
            self.cron._loop
        )

        mock_run_pending.assert_has_calls([mock.call(), mock.call()])
        mock_sleep.assert_has_calls([mock.call(.2), mock.call(.2)])

    @mock.patch.object(cron, 'LOG')
    def test_result_loop(self, mock_LOG):
        job_info = {
            'result': None,
            'error_info': 'mock_err_info',
            'description': 'mock_description'
        }
        job_info2 = {
            'result': 'mock_result',
            'error_info': None,
            'description': 'mock_description'
        }
        self.cron._queue.put(job_info)
        self.cron._queue.put(job_info2)
        mock_LOG.info.side_effect = CoriolisTestException()
        self.assertRaises(
            CoriolisTestException,
            self.cron._result_loop
        )
        mock_LOG.error.assert_called_once()
        mock_LOG.info.assert_called_once()

    @mock.patch.object(time, 'sleep')
    def test_janitor(self, mock_sleep):
        job1 = mock.Mock()
        job2 = mock.Mock()
        job1.is_expired.return_value = True
        job2.is_expired.return_value = False
        self.cron._jobs = {
            'job1': job1,
            'job2': job2,
        }
        mock_sleep.side_effect = CoriolisTestException()

        self.assertRaises(
            CoriolisTestException,
            self.cron._janitor
        )

        self.assertEqual(
            {'job2': job2},
            self.cron._jobs
        )

    @mock.patch.object(eventlet, 'kill')
    @mock.patch.object(time, 'sleep')
    def test_ripper(self, mock_sleep, mock_kill):
        self.cron._should_stop = True
        self.cron._eventlets = ['mock_event1', 'mock_event2']

        self.cron._ripper()

        self.assertEqual(
            [],
            self.cron._eventlets
        )
        mock_kill.assert_has_calls([
            mock.call('mock_event1'), mock.call('mock_event2')])

    @mock.patch.object(eventlet, 'spawn')
    @mock.patch.object(schedule, 'every')
    def test_start(self, mock_every, mock_spawn):
        mock_spawn.side_effect = [
            'spawn_loop', 'spawn_janitor', 'spawn_result_loop', 'spawn_ripper']

        self.cron.start()

        mock_every.return_value.minute.do.assert_called_once_with(
            self.cron._check_jobs)
        mock_spawn.assert_has_calls([
            mock.call(self.cron._loop),
            mock.call(self.cron._janitor),
            mock.call(self.cron._result_loop),
            mock.call(self.cron._ripper)
        ])
        self.assertEqual(
            ['spawn_loop', 'spawn_janitor', 'spawn_result_loop'],
            self.cron._eventlets
        )

    def test_stop(self):
        self.cron._should_stop = False
        self.cron.stop()
        self.assertEqual(
            True,
            self.cron._should_stop
        )
