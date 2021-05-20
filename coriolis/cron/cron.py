import datetime
import sys
import time

import eventlet
from eventlet import semaphore
from oslo_log import log
from oslo_utils import timeutils
import schedule

from coriolis import exception
from coriolis import schemas


LOG = log.getLogger(__name__)

SCHEDULE_FIELDS = ("minute", "hour", "dom", "month", "dow")


class CronJob(object):

    def __init__(self, name, description, schedule, enabled,
                 expires, on_success, on_error,
                 job_callable, *args, **kw):
        # param: name: string: unique ID that describes this job
        # param: description: string: a short description of the job
        # param: schedule: dict: cron job schedule. This is of the form:
        #     {
        #         "minute": 1,
        #         "hour": 0,
        #         "dom": 20,
        #         "month": 11,
        #         "dow": 1
        #     }
        # param: enabled: bool: Whether or not this cron job is enabled
        # param: expires: datetime: expiration date for this cronjob
        # param: on_success: callable: a function that gets called if the
        # job is successful. This function must accept the result returned
        # by the scheduled function
        #
        # param: on_error: callable: If the function scheduled to run raises
        # an exception, this function will run. on_error MUST accept the
        # exception info raised by the scheduled function, as the only
        # parameter. Any exception thrown by this callback will be logged and
        # ignored.
        #
        # param: job_callable: callable: The function we are scheduling to run,
        # Every other *arg or **kw following this parameter will be passed in
        # directly to this function.

        self.name = name
        if not callable(job_callable):
            raise exception.CoriolisException("Invalid job function")

        schema = schemas.SCHEDULE_API_BODY_SCHEMA["properties"]["schedule"]
        schemas.validate_value(schedule, schema)

        if on_success and not callable(on_success):
            raise ValueError("on_success must be callable")
        if on_error and not callable(on_error):
            raise ValueError("on_error must be callable")

        self._on_success = on_success
        self._on_error = on_error
        self.schedule = schedule
        self._func = job_callable
        self._description = description
        self._args = args
        self._kw = kw
        self._enabled = enabled
        if expires:
            if not isinstance(expires, datetime.datetime):
                raise exception.CoriolisException(
                    "Invalid expires")
        self._expires = expires

    def _compare(self, pairs):
        # we don't support the full cron syntax. Either exact matches
        # or wildcard matches are allowed. Expressions such as:
        # */10 (every 10 units)
        # are not supported
        ret = []
        for item in pairs:
            if item[1] is None or item[0] == item[1]:
                ret.append(True)
            else:
                ret.append(False)
        return ret

    def is_expired(self):
        now = timeutils.utcnow()
        if self._expires and self._expires < now:
            return True
        return False

    def should_run(self, dt):
        # NOTE(gsamfira): I find your lack of strong typing....disturbing
        if type(dt) is not datetime.datetime:
            raise exception.CoriolisException("Invalid datetime object")
        if self.is_expired():
            LOG.debug('Job %s has expired', self.name)
            return False
        if self._enabled is False:
            LOG.debug('Job %s is not enabled', self.name)
            return False

        fields = ('year', 'month', 'dom', 'hour',
                  'minute', 'second', 'dow')
        dt_fields = dict(zip(fields, dt.timetuple()))

        pairs = [(dt_fields[i], self.schedule.get(i)) for i in SCHEDULE_FIELDS]
        compared = self._compare(pairs)
        return False not in compared

    def _send_status(self, queue, status):
        if not queue:
            return
        queue.put(status)

    def start(self, status_queue=None):
        result = None
        exc_info = None
        try:
            result = self._func(*self._args, **self._kw)
            if self._on_success:
                self._on_success(result)
        except BaseException as err:
            exc_info = sys.exc_info()
            LOG.exception(err)
            if self._on_error:
                try:
                    self._on_error(exc_info)
                except Exception as callback_err:
                    LOG.exception(callback_err)
        self._send_status(
            status_queue,
            {"result": result,
             "description": self._description,
             "name": self.name,
             "error_info": exc_info})


class Cron(object):

    def __init__(self):
        self._queue = eventlet.Queue(maxsize=1000)
        self._should_stop = False
        self._jobs = {}
        self._eventlets = []
        self._semaphore = semaphore.Semaphore(value=1)

    def register(self, job):
        if not isinstance(job, CronJob):
            raise ValueError("Invalid job class")
        name = job.name
        LOG.debug("Registering cron job with name '%s'", name)
        with self._semaphore:
            self._jobs[name] = job

    def unregister(self, name):
        job = self._jobs.get(name)
        if job:
            LOG.debug("Unregistering cron job with name '%s'", name)
            with self._semaphore:
                del self._jobs[name]

    def unregister_jobs_with_prefix(self, prefix):
        jobs = [
            job for job in self._jobs
            if job.startswith(prefix)]
        if jobs:
            LOG.debug(
                "Unregistering the following cron jobs based on "
                "the requested prefix ('%s'): %s", prefix, jobs)
            with self._semaphore:
                for job in jobs:
                    del self._jobs[job]

    def _check_jobs(self):
        LOG.debug("Checking cron jobs")
        jobs = self._jobs.copy()
        job_nr = len(jobs)
        spawned = 0
        now = timeutils.utcnow()
        if job_nr:
            for job in jobs:
                LOG.debug('Checking job %s with schedule: %s', jobs[job].name,
                          jobs[job].schedule)
                if jobs[job].should_run(now):
                    LOG.debug("Spawning job %s" % job)
                    eventlet.spawn(jobs[job].start, self._queue)
                    spawned += 1

        done = timeutils.utcnow()
        delta = done - now
        LOG.debug("Spawned %(jobs)d jobs in %(seconds)d seconds" % {
            "seconds": delta.seconds,
            "jobs": spawned})

    def _loop(self):
        while True:
            schedule.run_pending()
            time.sleep(.2)

    def _result_loop(self):
        while True:
            job_info = self._queue.get()
            result = job_info["result"]
            error = job_info["error_info"]
            desc = job_info["description"]
            # TODO(gsamfira): send this to the controller and update
            # the logs table...or do something much more meaningful
            if error:
                LOG.error("Job %(job_desc)s exited with error: %(job_err)r" % {
                    "job_desc": desc,
                    "job_err": error})
            if result:
                LOG.info("Job %(desc)s returned: %(ret)r" % {
                    "desc": desc,
                    "ret": result})

    def _janitor(self):
        # remove expired jobs from memory. The check for expired
        # jobs runs once every minute.
        while True:
            with self._semaphore:
                tmp = {}
                for job in self._jobs:
                    if self._jobs[job].is_expired():
                        LOG.debug("Removing expired job: %s" % job)
                        continue
                    tmp[job] = self._jobs[job]
                self._jobs = tmp.copy()
                tmp = None
            # No need to run very often. Once a minute should do
            time.sleep(60)

    def _ripper(self):
        # Not sure if this will ever be called, but for correctness
        # sake, thought I'd add it
        while True:
            if self._should_stop:
                if len(self._eventlets):
                    for greenthread in self._eventlets:
                        eventlet.kill(greenthread)
                    self._eventlets = []
                return
            time.sleep(.5)

    def start(self):
        schedule.every().minute.do(self._check_jobs)
        self._eventlets.append(eventlet.spawn(self._loop))
        self._eventlets.append(eventlet.spawn(self._janitor))
        self._eventlets.append(eventlet.spawn(self._result_loop))
        eventlet.spawn(self._ripper)

    def stop(self):
        self._should_stop = True
