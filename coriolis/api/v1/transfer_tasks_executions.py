# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import transfer_tasks_execution_view
from coriolis.api import wsgi as api_wsgi
from coriolis import exception
from coriolis.policies import transfer_tasks_executions as executions_policies
from coriolis.transfer_tasks_executions import api

from webob import exc


class TransferTasksExecutionController(api_wsgi.Controller):
    def __init__(self):
        self._transfer_tasks_execution_api = api.API()
        super(TransferTasksExecutionController, self).__init__()

    def show(self, req, transfer_id, id):
        context = req.environ["coriolis.context"]
        context.can(
            executions_policies.get_transfer_executions_policy_label("show"))
        execution = self._transfer_tasks_execution_api.get_execution(
            context, transfer_id, id)
        if not execution:
            raise exc.HTTPNotFound()

        return transfer_tasks_execution_view.single(execution)

    def index(self, req, transfer_id):
        context = req.environ["coriolis.context"]
        context.can(
            executions_policies.get_transfer_executions_policy_label("list"))

        return transfer_tasks_execution_view.collection(
            self._transfer_tasks_execution_api.get_executions(
                context, transfer_id, include_tasks=False))

    def detail(self, req, transfer_id):
        context = req.environ["coriolis.context"]
        context.can(
            executions_policies.get_transfer_executions_policy_label("show"))

        return transfer_tasks_execution_view.collection(
            self._transfer_tasks_execution_api.get_executions(
                context, transfer_id, include_tasks=True))

    def create(self, req, transfer_id, body):
        context = req.environ["coriolis.context"]
        context.can(
            executions_policies.get_transfer_executions_policy_label("create"))

        # TODO(alexpilotti): validate body

        execution_body = body.get("execution", {})
        shutdown_instances = execution_body.get("shutdown_instances", False)

        return transfer_tasks_execution_view.single(
            self._transfer_tasks_execution_api.create(
                context, transfer_id, shutdown_instances))

    def delete(self, req, transfer_id, id):
        context = req.environ["coriolis.context"]
        context.can(
            executions_policies.get_transfer_executions_policy_label("delete"))

        try:
            self._transfer_tasks_execution_api.delete(context, transfer_id, id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(TransferTasksExecutionController())
