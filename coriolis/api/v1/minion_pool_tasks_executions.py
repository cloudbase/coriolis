# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from webob import exc

from coriolis.api import wsgi as api_wsgi
from coriolis.api.v1.views import minion_pool_tasks_execution_view
from coriolis import exception
from coriolis.minion_pool_tasks_executions import api
from coriolis.policies \
    import minion_pool_tasks_executions as pool_execution_policies


class MinionPoolTasksExecutionController(api_wsgi.Controller):
    def __init__(self):
        self._pool_tasks_execution_api = api.API()
        super(MinionPoolTasksExecutionController, self).__init__()

    def show(self, req, minion_pool_id, id):
        context = req.environ["coriolis.context"]
        context.can(
            pool_execution_policies.get_minion_pool_executions_policy_label(
                "show"))
        execution = self._pool_tasks_execution_api.get(
            context, minion_pool_id, id)
        if not execution:
            raise exc.HTTPNotFound()

        return minion_pool_tasks_execution_view.single(req, execution)

    def index(self, req, minion_pool_id):
        context = req.environ["coriolis.context"]
        context.can(
            pool_execution_policies.get_minion_pool_executions_policy_label(
                "list"))

        return minion_pool_tasks_execution_view.collection(
            req, self._pool_tasks_execution_api.list(
                context, minion_pool_id, include_tasks=False))

    def detail(self, req, minion_pool_id):
        context = req.environ["coriolis.context"]
        context.can(
            pool_execution_policies.get_minion_pool_executions_policy_label(
                "show"))
        return minion_pool_tasks_execution_view.collection(
            req, self._pool_tasks_execution_api.list(
                req.environ['coriolis.context'], minion_pool_id,
                include_tasks=True))

    def delete(self, req, minion_pool_id, id):
        context = req.environ["coriolis.context"]
        context.can(
            pool_execution_policies.get_minion_pool_executions_policy_label(
                "delete"))

        try:
            self._pool_tasks_execution_api.delete(context, minion_pool_id, id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(MinionPoolTasksExecutionController())
