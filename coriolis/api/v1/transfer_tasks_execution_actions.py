# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from webob import exc

from coriolis.api import wsgi as api_wsgi
from coriolis import exception
from coriolis.policies import transfer_tasks_executions as execution_policies
from coriolis.transfer_tasks_executions import api


class TransferTasksExecutionActionsController(api_wsgi.Controller):
    def __init__(self):
        self._transfer_tasks_execution_api = api.API()
        super(TransferTasksExecutionActionsController, self).__init__()

    @api_wsgi.action('cancel')
    def _cancel(self, req, transfer_id, id, body):
        context = req.environ['coriolis.context']
        context.can(
            execution_policies.get_transfer_executions_policy_label('cancel'))
        try:
            force = (body["cancel"] or {}).get("force", False)

            self._transfer_tasks_execution_api.cancel(
                context, transfer_id, id, force)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(TransferTasksExecutionActionsController())
