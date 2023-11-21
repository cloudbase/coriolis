# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis import exception
from coriolis.tasks import factory
from coriolis.tasks import minion_pool_tasks
from coriolis.tests import test_base


class TasksFactoryTestCase(test_base.CoriolisBaseTestCase):

    def test_get_task_runner(self):
        self.assertRaises(
            exception.NotFound,
            factory.get_task_runner_class,
            "invalid")

        result = factory.get_task_runner_class(
            constants.TASK_TYPE_POWER_ON_SOURCE_MINION)
        self.assertEqual(result, minion_pool_tasks.PowerOnSourceMinionTask)
