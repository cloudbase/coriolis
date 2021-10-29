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

    def _list(self, req):
        show_deleted = api_utils._get_show_deleted(
            req.GET.get("show_deleted", None))
        context = req.environ["coriolis.context"]
        context.show_deleted = show_deleted
        context.can(migration_policies.get_migrations_policy_label("show"))
        return migration_view.collection(
            req, self._migration_api.get_migrations(
                context, include_tasks=False))

    def index(self, req):
        return self._list(req)

    def detail(self, req):
        return self._list(req)

    @api_utils.format_keyerror_message(resource='migration', method='create')
    def _validate_migration_input(self, context, body):
        migration = body["migration"]
        origin_endpoint_id = migration["origin_endpoint_id"]
        destination_endpoint_id = migration["destination_endpoint_id"]
        origin_minion_pool_id = migration.get('origin_minion_pool_id')
        destination_minion_pool_id = migration.get(
            'destination_minion_pool_id')
        instance_osmorphing_minion_pool_mappings = migration.get(
            'instance_osmorphing_minion_pool_mappings', {})
        instances = api_utils.validate_instances_list_for_transfer(
            migration.get('instances'))
        extras = [
            instance
            for instance in instance_osmorphing_minion_pool_mappings
            if instance not in instances]
        if extras:
            raise ValueError(
                "One or more instance OSMorphing pool mappings were "
                "provided for instances (%s) which are not part of the "
                "migration's declared instances (%s)" % (extras, instances))

        notes = migration.get("notes")
        skip_os_morphing = migration.get("skip_os_morphing", False)
        shutdown_instances = migration.get(
            "shutdown_instances", False)
        replication_count = int(migration.get("replication_count", 2))
        if replication_count not in range(1, 11):
            raise ValueError(
                "'replication_count' must be an integer between 1 and 10."
                " Got: %s" % replication_count)

        source_environment = migration.get("source_environment", {})
        self._endpoints_api.validate_source_environment(
            context, origin_endpoint_id, source_environment)

        network_map = migration.get("network_map", {})
        api_utils.validate_network_map(network_map)

        # TODO(aznashwan): until the provider plugin interface is updated
        # to have separate 'network_map' and 'storage_mappings' fields,
        # we add them as part of the destination environment:
        destination_environment = migration.get(
            "destination_environment", {})
        destination_environment['network_map'] = network_map
        self._endpoints_api.validate_target_environment(
            context, destination_endpoint_id, destination_environment)

        storage_mappings = migration.get("storage_mappings", {})
        api_utils.validate_storage_mappings(storage_mappings)
        # NOTE(aznashwan): we validate the destination environment for the
        # import provider before appending the 'storage_mappings' parameter
        # for plugins with strict property name checks which do not yet
        # support storage mapping features:
        destination_environment['storage_mappings'] = storage_mappings

        return (origin_endpoint_id, destination_endpoint_id,
                origin_minion_pool_id, destination_minion_pool_id,
                instance_osmorphing_minion_pool_mappings, source_environment,
                destination_environment, instances, notes,
                skip_os_morphing, replication_count,
                shutdown_instances, network_map, storage_mappings)

    def create(self, req, body):
        migration_body = body.get("migration", {})
        context = req.environ['coriolis.context']
        context.can(migration_policies.get_migrations_policy_label("create"))
        user_scripts = migration_body.get('user_scripts', {})
        api_utils.validate_user_scripts(user_scripts)
        user_scripts = api_utils.normalize_user_scripts(
            user_scripts, migration_body.get("instances", []))
        replica_id = migration_body.get("replica_id")
        if replica_id:
            clone_disks = migration_body.get("clone_disks", True)
            force = migration_body.get("force", False)
            skip_os_morphing = migration_body.get("skip_os_morphing", False)
            instance_osmorphing_minion_pool_mappings = migration_body.get(
                'instance_osmorphing_minion_pool_mappings', {})

            # NOTE: destination environment for replica should have been
            # validated upon its creation.
            migration = self._migration_api.deploy_replica_instances(
                context, replica_id, instance_osmorphing_minion_pool_mappings, clone_disks,
                force, skip_os_morphing, user_scripts=user_scripts)
        else:
            (origin_endpoint_id,
             destination_endpoint_id,
             origin_minion_pool_id,
             destination_minion_pool_id,
             instance_osmorphing_minion_pool_mappings,
             source_environment,
             destination_environment,
             instances,
             notes,
             skip_os_morphing,
             replication_count,
             shutdown_instances,
             network_map,
             storage_mappings) = self._validate_migration_input(
                 context, body)
            migration = self._migration_api.migrate_instances(
                context, origin_endpoint_id, destination_endpoint_id,
                origin_minion_pool_id, destination_minion_pool_id,
                instance_osmorphing_minion_pool_mappings,
                source_environment, destination_environment, instances,
                network_map, storage_mappings, replication_count,
                shutdown_instances, notes=notes,
                skip_os_morphing=skip_os_morphing,
                user_scripts=user_scripts)

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
