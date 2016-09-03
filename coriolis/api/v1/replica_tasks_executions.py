# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from webob import exc

from coriolis.api import wsgi as api_wsgi
from coriolis.api.v1.views import replica_tasks_execution_view
from coriolis import exception
from coriolis.replica_tasks_executions import api


class ReplicaTasksExecutionController(api_wsgi.Controller):
    def __init__(self):
        self._replica_tasks_execution_api = api.API()
        super(ReplicaTasksExecutionController, self).__init__()

    def show(self, req, replica_id, id):
        execution = self._replica_tasks_execution_api.get_execution(
            req.environ["coriolis.context"], replica_id, id)
        if not execution:
            raise exc.HTTPNotFound()

        return replica_tasks_execution_view.single(req, execution)

    def index(self, req, replica_id):
        return replica_tasks_execution_view.collection(
            req, self._replica_tasks_execution_api.get_executions(
                req.environ['coriolis.context'], replica_id,
                include_tasks=False))

    def detail(self, req, replica_id):
        return replica_tasks_execution_view.collection(
            req, self._replica_tasks_execution_api.get_executions(
                req.environ['coriolis.context'], replica_id,
                include_tasks=True))

    def create(self, req, replica_id, body):
        # TODO: validate body

        execution_body = body.get("execution", {})
        shutdown_instances = execution_body.get("shutdown_instances", False)

        return replica_tasks_execution_view.single(
            req, self._replica_tasks_execution_api.create(
                req.environ['coriolis.context'], replica_id,
                shutdown_instances))

    def delete(self, req, replica_id, id):
        try:
            self._replica_tasks_execution_api.delete(
                req.environ['coriolis.context'], replica_id, id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(ReplicaTasksExecutionController())
