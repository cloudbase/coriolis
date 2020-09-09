# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from webob import exc

from coriolis import exception
from coriolis.api.v1.views import minion_pool_tasks_execution_view
from coriolis.api import wsgi as api_wsgi
from coriolis.policies import minion_pools as minion_pool_policies
from coriolis.minion_pools import api


class MinionPoolActionsController(api_wsgi.Controller):
    def __init__(self):
        self.minion_pool_api = api.API()
        super(MinionPoolActionsController, self).__init__()

    @api_wsgi.action('set-up-shared-resources')
    def _set_up_shared_resources(self, req, id, body):
        context = req.environ['coriolis.context']
        context.can(
            minion_pool_policies.get_minion_pools_policy_label(
                "set_up_shared_resources"))
        try:
            return minion_pool_tasks_execution_view.single(
                req, self.minion_pool_api.set_up_shared_pool_resources(
                    context, id))
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)

    @api_wsgi.action('tear-down-shared-resources')
    def _tear_down_shared_resources(self, req, id, body):
        context = req.environ['coriolis.context']
        context.can(
            minion_pool_policies.get_minion_pools_policy_label(
                "tear_down_shared_resources"))
        force = (body["tear-down-shared-resources"] or {}).get(
            "force", False)
        try:
            return minion_pool_tasks_execution_view.single(
                req, self.minion_pool_api.tear_down_shared_pool_resources(
                    context, id, force=force))
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)

    @api_wsgi.action('allocate-machines')
    def _allocate_pool_machines(self, req, id, body):
        context = req.environ['coriolis.context']
        context.can(
            minion_pool_policies.get_minion_pools_policy_label(
                "allocate_machines"))
        try:
            return minion_pool_tasks_execution_view.single(
                req, self.minion_pool_api.allocate_machines(
                    context, id))
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)

    @api_wsgi.action('deallocate-machines')
    def _deallocate_pool_machines(self, req, id, body):
        context = req.environ['coriolis.context']
        context.can(
            minion_pool_policies.get_minion_pools_policy_label(
                "deallocate_machines"))
        force = (body["deallocate-machines"] or {}).get("force", False)
        try:
            return minion_pool_tasks_execution_view.single(
                req, self.minion_pool_api.deallocate_machines(
                    context, id, force=force))
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(MinionPoolActionsController())
