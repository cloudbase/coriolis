# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from taskflow import task as taskflow_tasks

from coriolis import constants
from coriolis import exception
from coriolis.tass import factory as tasks_factory


TASK_RETURN_VALUE_FORMAT = "%s-result" % (
        constants.TASK_LOCK_NAME_FORMAT)


class _BaseRunWorkerTask(taskflow_tasks.Task):

    """
    Base taskflow.Task implementation for tasks which can be run
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
            self, worker_service_host, task_id, main_task_runner_type,
            cleanup_task_runner_type, depends_on=None, **kwargs):
        self._main_task_runner_type = main_task_runner_type
        self._cleanup_task_runner_type = cleanup_task_runner_type
        self._worker_service_host = worker_service_host

        super(_BaseRunWorkerTask, self).__init__(name=task_id, **kwargs)

    def _set_provides_for_dependencies(self, kwargs):
        dep = self.TASK_RETURN_VALUE_FORMAT % self.task_id
        if kwargs.get('provides') is not None:
            kwargs['provides'].append(dep)
        else:
            kwargs['provides'] = [dep]

    def _set_requires_for_dependencies(self, kwargs, depends_on):
        dep_requirements = [
            self.TASK_RETURN_VALUE_FORMAT % dep_id
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

    def _get_worker_rpc_for_main_task(self):
        pass

    def _get_worker_rpc_for_cleanup_task(self):
        if self._cleanup_task_runner_type:
            # TODO
            pass

    def pre_execute(self):
        # TODO:
        # add task to Coriolis' DB? (should we do it here or in the conductor?)
        # load up TaskRunner class
        # get worker host (shared among all tasks being run)
        pass

    # @lock_on_task? (Coriolis' notion of a task)
    def execute(self):
        # TODO:
        # actively run the thing on the worker
        # return results_dict_from_worker
        pass

    def post_execute(self):
        # TODO:
        # deallocate worker service
        # save task result to Coriolis' db
        pass

    def pre_revert(self):
        # deallocate original worker service
        # TODO: find worker to run reverting
        pass

    def revert(self):
        # TODO:
        # run reverting task
        pass

    def post_revert(self):
        # TODO:
        # deallocate worker service
        pass
