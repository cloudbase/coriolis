# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging
from webob import exc

from coriolis import exception
from coriolis import schemas
from coriolis.api.v1.views import replica_view
from coriolis.api import wsgi as api_wsgi
from coriolis.endpoints import api as endpoints_api
from coriolis.policies import replicas as replica_policies
from coriolis.replicas import api

LOG = logging.getLogger(__name__)


class ReplicaController(api_wsgi.Controller):
    def __init__(self):
        self._replica_api = api.API()
        self._endpoints_api = endpoints_api.API()
        super(ReplicaController, self).__init__()

    def show(self, req, id):
        context = req.environ["coriolis.context"]
        context.can(replica_policies.get_replicas_policy_label("show"))
        replica = self._replica_api.get_replica(context, id)
        if not replica:
            raise exc.HTTPNotFound()

        return replica_view.single(req, replica)

    def index(self, req):
        context = req.environ["coriolis.context"]
        context.can(replica_policies.get_replicas_policy_label("list"))
        return replica_view.collection(
            req, self._replica_api.get_replicas(
                context, include_tasks_executions=False))

    def detail(self, req):
        context = req.environ["coriolis.context"]
        context.can(
            replica_policies.get_replicas_policy_label("show_executions"))
        return replica_view.collection(
            req, self._replica_api.get_replicas(
                context, include_tasks_executions=True))

    def _validate_create_body(self, body):
        try:
            replica = body["replica"]

            origin_endpoint_id = replica["origin_endpoint_id"]
            destination_endpoint_id = replica["destination_endpoint_id"]
            destination_environment = replica.get("destination_environment")
            instances = replica["instances"]
            notes = replica.get("notes")
            network_map = replica.get("network_map")
            try:
                schemas.validate_value(
                    network_map, schemas.CORIOLIS_NETWORK_MAP_SCHEMA)
            except exception.SchemaValidationException:
                raise exc.HTTPBadRequest(
                    explanation="Invalid network_map "
                                "%s" % network_map)

            # NOTE: until the provider plugin interface is updated to have a
            # separate 'network_map' field, we add it into the destination
            # environment.
            destination_environment["network_map"] = network_map

            return (origin_endpoint_id, destination_endpoint_id,
                    destination_environment, instances, network_map, notes)
        except Exception as ex:
            LOG.exception(ex)
            if hasattr(ex, "message"):
                msg = ex.message
            else:
                msg = str(ex)
            raise exception.InvalidInput(msg)

    def create(self, req, body):
        context = req.environ["coriolis.context"]
        context.can(replica_policies.get_replicas_policy_label("create"))

        (origin_endpoint_id, destination_endpoint_id,
         destination_environment, instances, network_map,
         notes) = self._validate_create_body(body)

        is_valid, message = (
            self._endpoints_api.validate_target_environment(
                context, destination_endpoint_id,
                destination_environment))
        if not is_valid:
            raise exc.HTTPBadRequest(
                explanation="Invalid destination "
                            "environment: %s" % message)

        return replica_view.single(req, self._replica_api.create(
            context, origin_endpoint_id, destination_endpoint_id,
            destination_environment, instances, network_map, notes))

    def delete(self, req, id):
        context = req.environ["coriolis.context"]
        context.can(replica_policies.get_replicas_policy_label("delete"))
        try:
            self._replica_api.delete(context, id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(ReplicaController())
