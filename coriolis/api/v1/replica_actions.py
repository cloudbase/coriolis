# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from webob import exc

from coriolis.api.v1.views import replica_tasks_execution_view
from coriolis.api import wsgi as api_wsgi
from coriolis import exception
from coriolis.replicas import api


class ReplicaActionsController(api_wsgi.Controller):
    def __init__(self):
        self._replica_api = api.API()
        super(ReplicaActionsController, self).__init__()

    @api_wsgi.action('delete-disks')
    def _delete_disks(self, req, id, body):
        try:
            return replica_tasks_execution_view.single(
                req, self._replica_api.delete_disks(
                    req.environ['coriolis.context'], id))
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(ReplicaActionsController())
