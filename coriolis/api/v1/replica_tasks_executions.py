# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from webob import exc

from coriolis.api.v1.views import replica_tasks_execution_view
from coriolis.api import wsgi as api_wsgi
from coriolis import exception
from coriolis.replica_tasks_executions import api
from coriolis.policies import replica_tasks_executions as executions_policies


class ReplicaTasksExecutionController(api_wsgi.Controller):
    def __init__(self):
        self._replica_tasks_execution_api = api.API()
        super(ReplicaTasksExecutionController, self).__init__()

    def show(self, req, replica_id, id):
        context = req.environ["coriolis.context"]
        context.can(
            executions_policies.get_replica_executions_policy_label("show"))
        execution = self._replica_tasks_execution_api.get_execution(
            context, replica_id, id)
        if not execution:
            raise exc.HTTPNotFound()

        return replica_tasks_execution_view.single(req, execution)

    def index(self, req, replica_id):
        context = req.environ["coriolis.context"]
        context.can(
            executions_policies.get_replica_executions_policy_label("list"))

        return replica_tasks_execution_view.collection(
            req, self._replica_tasks_execution_api.get_executions(
                context, replica_id, include_tasks=False))

    def detail(self, req, replica_id):
        return replica_tasks_execution_view.collection(
            req, self._replica_tasks_execution_api.get_executions(
                req.environ['coriolis.context'], replica_id,
                include_tasks=True))

    def create(self, req, replica_id, body):
        context = req.environ["coriolis.context"]
        context.can(
            executions_policies.get_replica_executions_policy_label("create"))

        # TODO(alexpilotti): validate body

        execution_body = body.get("execution", {})
        shutdown_instances = execution_body.get("shutdown_instances", False)

        return replica_tasks_execution_view.single(
            req, self._replica_tasks_execution_api.create(
                context, replica_id, shutdown_instances))

    def delete(self, req, replica_id, id):
        context = req.environ["coriolis.context"]
        context.can(
            executions_policies.get_replica_executions_policy_label("delete"))

        try:
            self._replica_tasks_execution_api.delete(context, replica_id, id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(ReplicaTasksExecutionController())
