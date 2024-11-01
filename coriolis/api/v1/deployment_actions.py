# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from webob import exc

from coriolis.api import wsgi as api_wsgi
from coriolis.deployments import api
from coriolis import exception
from coriolis.policies import deployments as deployment_policies


class DeploymentActionsController(api_wsgi.Controller):
    def __init__(self):
        self._deployment_api = api.API()
        super(DeploymentActionsController, self).__init__()

    @api_wsgi.action('cancel')
    def _cancel(self, req, id, body):
        context = req.environ['coriolis.context']
        context.can(deployment_policies.get_deployments_policy_label("cancel"))
        try:
            force = (body["cancel"] or {}).get("force", False)

            self._deployment_api.cancel(context, id, force)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(DeploymentActionsController())
