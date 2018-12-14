# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging
from webob import exc

from coriolis import exception
from coriolis.api.v1 import utils as api_utils
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

    def _validate_create_body(self, context, body):
        try:
            replica = body["replica"]

            origin_endpoint_id = replica["origin_endpoint_id"]
            destination_endpoint_id = replica["destination_endpoint_id"]
            destination_environment = replica.get(
                "destination_environment", {})
            instances = replica["instances"]
            notes = replica.get("notes")

            source_environment = replica.get("source_environment", {})
            self._endpoints_api.validate_source_environment(
                context, origin_endpoint_id, source_environment)

            network_map = replica.get("network_map", {})
            api_utils.validate_network_map(network_map)
            destination_environment['network_map'] = network_map

            # NOTE(aznashwan): we validate the destination environment for the
            # import provider before appending the 'storage_mappings' parameter
            # for plugins with strict property name checks which do not yet
            # support storage mapping features:
            self._endpoints_api.validate_target_environment(
                context, destination_endpoint_id, destination_environment)

            storage_mappings = replica.get("storage_mappings", {})
            api_utils.validate_storage_mappings(storage_mappings)

            # TODO(aznashwan): until the provider plugin interface is updated
            # to have separate 'network_map' and 'storage_mappings' fields,
            # we add them as part of the destination environment:
            destination_environment['storage_mappings'] = storage_mappings

            return (origin_endpoint_id, destination_endpoint_id,
                    source_environment, destination_environment, instances,
                    network_map, storage_mappings, notes)
        except Exception as ex:
            LOG.exception(ex)
            msg = getattr(ex, "message", str(ex))
            raise exception.InvalidInput(msg)

    def create(self, req, body):
        context = req.environ["coriolis.context"]
        context.can(replica_policies.get_replicas_policy_label("create"))

        (origin_endpoint_id, destination_endpoint_id,
         source_environment, destination_environment, instances, network_map,
         storage_mappings, notes) = self._validate_create_body(context, body)

        return replica_view.single(req, self._replica_api.create(
            context, origin_endpoint_id, destination_endpoint_id,
            source_environment, destination_environment, instances,
            network_map, storage_mappings, notes))

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
