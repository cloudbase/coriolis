# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging
from webob import exc

from coriolis.api.v1.views import replica_view
from coriolis.api import wsgi as api_wsgi
from coriolis.endpoints import api as endpoints_api
from coriolis import exception
from coriolis.replicas import api

LOG = logging.getLogger(__name__)


class ReplicaController(api_wsgi.Controller):
    def __init__(self):
        self._replica_api = api.API()
        self._endpoints_api = endpoints_api.API()
        super(ReplicaController, self).__init__()

    def show(self, req, id):
        replica = self._replica_api.get_replica(
            req.environ["coriolis.context"], id)
        if not replica:
            raise exc.HTTPNotFound()

        return replica_view.single(req, replica)

    def index(self, req):
        return replica_view.collection(
            req, self._replica_api.get_replicas(
                req.environ['coriolis.context'],
                include_tasks_executions=False))

    def detail(self, req):
        return replica_view.collection(
            req, self._replica_api.get_replicas(
                req.environ['coriolis.context'],
                include_tasks_executions=True))

    def _validate_create_body(self, body):
        try:
            replica = body["replica"]

            origin_endpoint_id = replica["origin_endpoint_id"]
            destination_endpoint_id = replica["destination_endpoint_id"]
            destination_environment = replica.get("destination_environment")
            instances = replica["instances"]
            notes = replica.get("notes")

            return (origin_endpoint_id, destination_endpoint_id,
                    destination_environment, instances, notes)
        except Exception as ex:
            LOG.exception(ex)
            if hasattr(ex, "message"):
                msg = ex.message
            else:
                msg = str(ex)
            raise exception.InvalidInput(msg)

    def create(self, req, body):
        (origin_endpoint_id, destination_endpoint_id,
         destination_environment, instances,
         notes) = self._validate_create_body(body)

        is_valid, message = (
            self._endpoints_api.validate_target_environment(
                req.environ["coriolis.context"], destination_endpoint_id,
                destination_environment))
        if not is_valid:
            raise exc.HTTPBadRequest(
                explanation="Invalid destination "
                            "environment: %s" % message)

        return replica_view.single(req, self._replica_api.create(
            req.environ['coriolis.context'], origin_endpoint_id,
            destination_endpoint_id, destination_environment, instances,
            notes))

    def delete(self, req, id):
        try:
            self._replica_api.delete(req.environ['coriolis.context'], id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(ReplicaController())
