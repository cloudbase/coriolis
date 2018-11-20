# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging
from webob import exc

from coriolis import exception
from coriolis.api.v1 import utils as api_utils
from coriolis.api.v1.views import migration_view
from coriolis.api import wsgi as api_wsgi
from coriolis.endpoints import api as endpoints_api
from coriolis.migrations import api
from coriolis.policies import migrations as migration_policies

LOG = logging.getLogger(__name__)


class MigrationController(api_wsgi.Controller):
    def __init__(self):
        self._migration_api = api.API()
        self._endpoints_api = endpoints_api.API()
        super(MigrationController, self).__init__()

    def show(self, req, id):
        context = req.environ["coriolis.context"]
        context.can(migration_policies.get_migrations_policy_label("show"))
        migration = self._migration_api.get_migration(context, id)
        if not migration:
            raise exc.HTTPNotFound()

        return migration_view.single(req, migration)

    def index(self, req):
        context = req.environ["coriolis.context"]
        context.can(migration_policies.get_migrations_policy_label("show"))
        return migration_view.collection(
            req, self._migration_api.get_migrations(
                context, include_tasks=False))

    def detail(self, req):
        context = req.environ["coriolis.context"]
        context.can(
            migration_policies.get_migrations_policy_label("show_execution"))
        return migration_view.collection(
            req, self._migration_api.get_migrations(
                context, include_tasks=True))

    def _validate_migration_input(self, migration):
        try:
            origin_endpoint_id = migration["origin_endpoint_id"]
            destination_endpoint_id = migration["destination_endpoint_id"]
            destination_environment = migration.get("destination_environment")
            instances = migration["instances"]
            notes = migration.get("notes")
            skip_os_morphing = migration.get("skip_os_morphing", False)

            network_map = migration.get("network_map", {})
            api_utils.validate_network_map(network_map)

            storage_mappings = migration.get("storage_mappings", {})
            api_utils.validate_storage_mappings(storage_mappings)

            # TODO(aznashwan): until the provider plugin interface is updated
            # to have separate 'network_map' and 'storage_mappings' fields,
            # we add them as part of the destination environment:
            destination_environment['network_map'] = network_map
            destination_environment['storage_mappings'] = storage_mappings

            return (origin_endpoint_id, destination_endpoint_id,
                    destination_environment, instances, notes,
                    skip_os_morphing, network_map, storage_mappings)
        except Exception as ex:
            LOG.exception(ex)
            msg = getattr(ex, "message", str(ex))
            raise exception.InvalidInput(msg)

    def create(self, req, body):
        # TODO(alexpilotti): validate body
        migration_body = body.get("migration", {})
        context = req.environ['coriolis.context']
        context.can(migration_policies.get_migrations_policy_label("create"))

        replica_id = migration_body.get("replica_id")
        if replica_id:
            clone_disks = migration_body.get("clone_disks", True)
            force = migration_body.get("force", False)
            skip_os_morphing = migration_body.get("skip_os_morphing", False)

            # NOTE: destination environment for replica should have been
            # validated upon its creation.
            migration = self._migration_api.deploy_replica_instances(
                context, replica_id, clone_disks, force, skip_os_morphing)
        else:
            (origin_endpoint_id,
             destination_endpoint_id,
             destination_environment,
             instances,
             notes,
             skip_os_morphing, network_map,
             storage_mappings) = self._validate_migration_input(
                migration_body)
            is_valid, message = (
                self._endpoints_api.validate_target_environment(
                    context, destination_endpoint_id, destination_environment))
            if not is_valid:
                raise exc.HTTPBadRequest(
                    explanation="Invalid destination "
                                "environment: %s" % message)

            migration = self._migration_api.migrate_instances(
                context, origin_endpoint_id, destination_endpoint_id,
                destination_environment, instances, network_map,
                storage_mappings, notes, skip_os_morphing)

        return migration_view.single(req, migration)

    def delete(self, req, id):
        context = req.environ['coriolis.context']
        context.can(migration_policies.get_migrations_policy_label("delete"))
        try:
            self._migration_api.delete(context, id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(MigrationController())
