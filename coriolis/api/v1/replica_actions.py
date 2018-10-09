# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from webob import exc

from coriolis import exception
from coriolis.api.v1.views import replica_tasks_execution_view
from coriolis.api import wsgi as api_wsgi
from coriolis.policies import replicas as replica_policies
from coriolis.replicas import api


class ReplicaActionsController(api_wsgi.Controller):
    def __init__(self):
        self._replica_api = api.API()
        super(ReplicaActionsController, self).__init__()

    @api_wsgi.action('delete-disks')
    def _delete_disks(self, req, id, body):
        context = req.environ['coriolis.context']
        context.can(replica_policies.get_replicas_policy_label("delete_disks"))
        try:
            return replica_tasks_execution_view.single(
                req, self._replica_api.delete_disks(context, id))
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(ReplicaActionsController())
