# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from webob import exc

from coriolis import exception
from coriolis.api import wsgi as api_wsgi
from coriolis.policies import replica_tasks_executions as execution_policies
from coriolis.replica_tasks_executions import api


class ReplicaTasksExecutionActionsController(api_wsgi.Controller):
    def __init__(self):
        self._replica_tasks_execution_api = api.API()
        super(ReplicaTasksExecutionActionsController, self).__init__()

    @api_wsgi.action('cancel')
    def _cancel(self, req, replica_id, id, body):
        context = req.environ['coriolis.context']
        context.can(
            execution_policies.get_replica_executions_policy_label('cancel'))
        try:
            force = (body["cancel"] or {}).get("force", False)

            self._replica_tasks_execution_api.cancel(
                context, replica_id, id, force)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(ReplicaTasksExecutionActionsController())
