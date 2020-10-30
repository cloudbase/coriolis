# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from taskflow import task as taskflow_tasks

from coriolis import constants
from coriolis import exception


class BaseMinionManangerTask(taskflow_tasks.Task):

    """
    Base taskflow.Task implementation for Minion Mananger tasks.
    """

    def __init__(
            self, ctxt, db_api, minion_pool_id, **kwargs):
        self._minion_pool_id = minion_pool_id
        self._db_api = db_api

        super(BaseRunWorkerTask, self).__init__(**kwargs)

    # @lock_on_pool
    def pre_execute(self):
        # TODO:
        # check minion pool is ready for the task
        # ask scheduler for worker service
        # update minion pool status accordingly
        pass

    # @lock_on_pool
    def execute(self):
        # TODO:
        # left to child classes
        pass

    def post_execute(self):
        # TODO:
        # update minion pool status accordingly
        # record results (if any)
        pass

    def pre_revert(self):
        # ask scheduler for worker service for reversion
        pass

    def revert(self):
        # TODO:
        # run reverting task
        pass

    def post_revert(self):
        # TODO:
        pass
