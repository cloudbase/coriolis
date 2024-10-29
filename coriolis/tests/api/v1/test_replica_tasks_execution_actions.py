# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import ddt
from webob import exc

from coriolis.api.v1 import replica_tasks_execution_actions as replica_api
from coriolis import exception
from coriolis.replica_tasks_executions import api
from coriolis.tests import test_base
from coriolis.tests import testutils


@ddt.ddt
class ReplicaTasksExecutionActionsControllerTestCase(
    test_base.CoriolisBaseTestCase
):
    """Test suite for the Coriolis Replica Tasks Execution Actions v1 API"""

    def setUp(self):
        super(ReplicaTasksExecutionActionsControllerTestCase, self).setUp()
        self.replica_api = replica_api.ReplicaTasksExecutionActionsController()

    @mock.patch.object(api.API, 'cancel')
    @ddt.file_data('data/replica_task_execution_actions_cancel.yml')
    def test_cancel(
        self,
        mock_cancel,
        config,
        expected_force,
        exception_raised,
        expected_result
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        replica_id = mock.sentinel.transfer_id
        body = config["body"]
        if exception_raised:
            mock_cancel.side_effect = getattr(exception, exception_raised)(
                "err")

        self.assertRaises(
            getattr(exc, expected_result),
            testutils.get_wrapped_function(self.replica_api._cancel),
            mock_req,
            replica_id,
            id,
            body
        )

        mock_context.can.assert_called_once_with(
            "migration:replica_executions:cancel")
        mock_cancel.assert_called_once_with(
            mock_context, replica_id, id, expected_force)
