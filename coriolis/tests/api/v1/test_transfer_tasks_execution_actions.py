# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import ddt
from webob import exc

from coriolis.api.v1 import transfer_tasks_execution_actions as transfer_api
from coriolis import exception
from coriolis.tests import test_base
from coriolis.tests import testutils
from coriolis.transfer_tasks_executions import api


@ddt.ddt
class TransferTasksExecutionActionsControllerTestCase(
    test_base.CoriolisBaseTestCase
):
    """Test suite for the Coriolis Transfer Tasks Execution Actions v1 API"""

    def setUp(self):
        super(TransferTasksExecutionActionsControllerTestCase, self).setUp()
        self.transfer_api = (
            transfer_api.TransferTasksExecutionActionsController())

    @mock.patch.object(api.API, 'cancel')
    @ddt.file_data('data/transfer_task_execution_actions_cancel.yml')
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
        transfer_id = mock.sentinel.transfer_id
        body = config["body"]
        if exception_raised:
            mock_cancel.side_effect = getattr(exception, exception_raised)(
                "err")

        self.assertRaises(
            getattr(exc, expected_result),
            testutils.get_wrapped_function(self.transfer_api._cancel),
            mock_req,
            transfer_id,
            id,
            body
        )

        mock_context.can.assert_called_once_with(
            "migration:transfer_executions:cancel")
        mock_cancel.assert_called_once_with(
            mock_context, transfer_id, id, expected_force)
