# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_config import cfg
from oslo_log import log as logging
from taskflow import task as taskflow_tasks
from taskflow.types import failure

from coriolis import constants
from coriolis import exception
from coriolis import utils
from coriolis.tasks import factory as tasks_factory
from coriolis.scheduler.rpc import client as rpc_scheduler_client
from coriolis.worker.rpc import client as rpc_worker_client


TASK_RETURN_VALUE_FORMAT = "%s-result" % (
        constants.TASK_LOCK_NAME_FORMAT)
LOG = logging.getLogger()

taskflow_opts = [
    cfg.IntOpt("worker_task_execution_timeout",
               default=3600,
               help="Number of seconds until Coriolis tasks which are executed"
                    "remotely on a Worker Service through taskflow timeout.")
]

CONF = cfg.CONF
CONF.register_opts(taskflow_opts, 'taskflow')


class BaseCoriolisTaskflowTask(taskflow_tasks.Task):
    """ Base class for all TaskFlow tasks within Coriolis. """

    def _get_error_str_for_flow_failures(
            self, flow_failures, full_tracebacks=True):
        if not flow_failures:
            return "No flow failures provided."

        if not flow_failures.items():
            return "No flow failures present."

        res = ""
        for (task_id, task_failure) in flow_failures.items():
            label = "Error message"
            failure_str = task_failure.exception_str
            if full_tracebacks:
                label = "Traceback"
                failure_str = task_failure.traceback_str
            else:
                failure_str = task_failure.exception_str
                if isinstance(
                        task_failure.exception,
                        exception.TaskProcessException):
                    # NOTE: TaskProcessException contains a full trace
                    # from the worker service so we must split it:
                    exception_lines = task_failure.exception_str.split('\n')
                    if exception_lines:
                        if len(exception_lines) > 2:
                            failure_str = exception_lines[-2].strip()
                        else:
                            failure_str = exception_lines[-1].strip()
            res = (
                "%s %s for task '%s': %s\n" % (
                    res, label, task_id, failure_str))
        if res:
            # remove extra newline:
            res = res[:-1]

        return res

    def revert(self, *args, **kwargs):
        result = kwargs.get('result')
        if isinstance(result, failure.Failure):
            # it means that this is the task which error'd out:
            LOG.error(
                "Taskflow task '%s' is reverting after errorring out with the "
                "following trace: %s", self.name, result.traceback_str)
        else:
            # else the failures were from other tasks:
            flow_failures = kwargs.get('flow_failures', {})
            LOG.error(
                "Taskflow task '%s' is reverting after the failure of one "
                "or more other tasks (%s) Tracebacks were:\n%s" % (
                    self.name, list(flow_failures.keys()),
                    self._get_error_str_for_flow_failures(
                        flow_failures, full_tracebacks=True)))


class BaseRunWorkerTask(BaseCoriolisTaskflowTask):
    """ Base taskflow.Task implementation for tasks which can be run
    on the worker service.
    This class can be seen as an "adapter" between the current
    coriolis.tasks.TaskRunner classes and taskflow ones.

    :param task_id: ID of the task. This value is declared as a returned value
        from the task and can be set as a requirement for other tasks, thus
        achieving a dependency system.
    :param main_task_runner_class: constants.TASK_TYPE_* referencing the
        main coriolis.tasks.TaskRunner class to be run on a worker service.
    :param cleanup_task_runner_task: constants.TASK_TYPE_* referencing the
        cleanup task to be run on reversion. No cleanup will be performed
        during the task's reversion (apart from Worker Service deallocation)
        otherwise.
    """

    def __init__(
            self, task_name, task_id, task_instance, main_task_runner_type,
            cleanup_task_runner_type=None, depends_on=None,
            raise_on_cleanup_failure=False, **kwargs):
        self._task_id = task_id
        self._task_name = task_name
        self._task_instance = task_instance
        self._main_task_runner_type = main_task_runner_type
        self._cleanup_task_runner_type = cleanup_task_runner_type
        self._raise_on_cleanup_failure = raise_on_cleanup_failure
        self._scheduler_client_instance = None

        super(BaseRunWorkerTask, self).__init__(name=task_name, **kwargs)

    @property
    def _scheduler_client(self):
        if not getattr(self, '_scheduler_client_instance', None):
            self._scheduler_client_instance = (
                rpc_scheduler_client.SchedulerClient())
        return self._scheduler_client_instance

    def _set_provides_for_dependencies(self, kwargs):
        dep = TASK_RETURN_VALUE_FORMAT % self._task_name
        if kwargs.get('provides') is not None:
            kwargs['provides'].append(dep)
        else:
            kwargs['provides'] = [dep]

    def _set_requires_for_dependencies(self, kwargs, depends_on):
        dep_requirements = [
            TASK_RETURN_VALUE_FORMAT % dep_id
            for dep_id in depends_on]
        if kwargs.get('requires') is not None:
            kwargs['requires'].extend(dep_requirements)
        elif dep_requirements:
            kwargs['requires'] = dep_requirements
        return kwargs

    def _set_requires_for_task_info_fields(self, kwargs):
        new_requires = kwargs.get('requires', [])
        main_task_runner = tasks_factory.get_task_runner_class(
            self._main_task_runner_type)
        main_task_deps = main_task_runner.get_required_task_info_properties()
        new_requires.extend(main_task_deps)
        if self._cleanup_task_runner_type:
            cleanup_task_runner = tasks_factory.get_task_runner_class(
                self._cleanup_task_runner_type)
            cleanup_task_deps = list(
                set(
                    cleanup_task_runner.get_required_task_info_properties(
                        )).difference(
                            main_task_runner.get_returned_task_info_properties()))
            new_requires.extend(cleanup_task_deps)

        kwargs['requires'] = new_requires
        return kwargs

    def _set_provides_for_task_info_fields(self, kwargs):
        new_provides = kwargs.get('provides', [])
        main_task_runner = tasks_factory.get_task_runner_class(
            self._main_task_runner_type)
        main_task_res = main_task_runner.get_returned_task_info_properties()
        new_provides.extend(main_task_res)
        if self._cleanup_task_runner_type:
            cleanup_task_runner = tasks_factory.get_task_runner_class(
                self._cleanup_task_runner_type)
            cleanup_task_res = list(
                set(
                    cleanup_task_runner.get_returned_task_info_properties(
                        )).difference(
                            main_task_runner.get_returned_task_info_properties()))
            new_provides.extend(cleanup_task_res)

        kwargs['provides'] = new_provides
        return kwargs

    def _get_worker_service_rpc_for_task(
            self, ctxt, task_id, task_type, origin, destination,
            retry_count=5, retry_period=2,
            rpc_timeout=CONF.taskflow.worker_task_execution_timeout):
        task_info = {
            "id": task_id,
            "task_type": task_type}
        worker_service = self._scheduler_client.get_worker_service_for_task(
            ctxt, task_info, origin, destination, retry_count=retry_count,
            retry_period=retry_period, random_choice=True)
        LOG.debug(
            "[Task '%s'] Was offered the following worker service for executing "
            "Taskflow worker task '%s': %s",
                self._task_name, task_id, worker_service['id'])

        return rpc_worker_client.WorkerClient.from_service_definition(
            worker_service, timeout=rpc_timeout)

    def _execute_task(
            self, ctxt, task_id, task_type, origin, destination, task_info):
        worker_rpc = self._get_worker_service_rpc_for_task(
            ctxt, self._task_id, task_type, origin, destination)

        try:
            LOG.debug(
                "[Task '%s'] Starting to run task '%s' (type '%s') "
                "on worker service." % (
                    self._task_id, self._task_name, task_type))
            res = worker_rpc.run_task(
                ctxt, None, self._task_id, task_type, origin, destination,
                self._task_instance, task_info)
            LOG.debug(
                "[Task '%s'] Taskflow worker task '%s' (type %s) has "
                "successfully run and returned the following info: %s" % (
                    self._task_name, task_id, task_type, res))
            return res
        except Exception as ex:
            LOG.debug(
                "[Task %s] Exception occurred while executing task '%s' "
                "(type '%s') on the worker service: %s", self._task_name,
                task_id, task_type, utils.get_exception_details())
            raise

    def execute(self, context, origin, destination, task_info):
        res = self._execute_task(
            context, self._task_id, self._main_task_runner_type, origin,
            destination, task_info)
        return res

    def revert(self, context, origin, destination, task_info, **kwargs):
        super(BaseRunWorkerTask, self).revert(
            context, origin, destination, task_info, **kwargs)
        if not self._cleanup_task_runner_type:
            LOG.debug(
                "Task '%s' (main type '%s') had no cleanup task runner "
                "associated with it. Skipping any reversion logic",
                self._task_name, self._main_task_runner_type)
            return

        try:
            res = self._execute_task(
                context, self._task_id, self._cleanup_task_runner_type, origin,
                destination, task_info)
        except Exception as ex:
            LOG.warn(
                "Task cleanup for '%s' (main task type '%s', cleanup task type"
                "'%s') has failed with the following trace: %s",
                self._task_name, self._main_task_runner_type,
                self._cleanup_task_runner_type, utils.get_exception_details())
            if self._raise_on_cleanup_failure:
                raise
            return

        LOG.debug(
            "Reversion of taskflow task '%s' (ID '%s') was successfully "
            "executed using task runner '%s' with the following result: %s" % (
                self._task_name, self._task_id, self._cleanup_task_runner_type,
                res))
