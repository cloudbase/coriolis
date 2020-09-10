# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from webob import exc

from coriolis import exception
from coriolis.api import wsgi as api_wsgi
from coriolis.policies \
    import minion_pool_tasks_executions as pool_execution_policies
from coriolis.minion_pool_tasks_executions import api


class MinionPoolTasksExecutionActionsController(api_wsgi.Controller):
    def __init__(self):
        self._minion_pool_tasks_executions_api = api.API()
        super(MinionPoolTasksExecutionActionsController, self).__init__()

    @api_wsgi.action('cancel')
    def _cancel(self, req, minion_pool_id, id, body):
        context = req.environ['coriolis.context']
        context.can(
            pool_execution_policies.get_minion_pool_executions_policy_label(
                'cancel'))
        try:
            force = (body["cancel"] or {}).get("force", False)

            self._minion_pool_tasks_executions_api.cancel(
                context, minion_pool_id, id, force)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(MinionPoolTasksExecutionActionsController())
