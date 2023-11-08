# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import minion_pool_view
from coriolis.api import wsgi as api_wsgi
from coriolis import exception
from coriolis.minion_pools import api
from coriolis.policies import minion_pools as minion_pool_policies

from webob import exc


class MinionPoolActionsController(api_wsgi.Controller):
    def __init__(self):
        self.minion_pool_api = api.API()
        super(MinionPoolActionsController, self).__init__()

    @api_wsgi.action('allocate')
    def _allocate_pool(self, req, id, body):
        context = req.environ['coriolis.context']
        context.can(
            minion_pool_policies.get_minion_pools_policy_label(
                "allocate"))
        try:
            return minion_pool_view.single(
                self.minion_pool_api.allocate_minion_pool(
                    context, id))
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)

    @api_wsgi.action('refresh')
    def _refresh_pool(self, req, id, body):
        context = req.environ['coriolis.context']
        context.can(
            minion_pool_policies.get_minion_pools_policy_label(
                "refresh"))
        try:
            return minion_pool_view.single(
                self.minion_pool_api.refresh_minion_pool(
                    context, id))
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)

    @api_wsgi.action('deallocate')
    def _deallocate_pool(self, req, id, body):
        context = req.environ['coriolis.context']
        context.can(
            minion_pool_policies.get_minion_pools_policy_label(
                "deallocate"))
        force = (body["deallocate"] or {}).get("force", False)
        try:
            return minion_pool_view.single(
                self.minion_pool_api.deallocate_minion_pool(
                    context, id, force=force))
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(MinionPoolActionsController())
