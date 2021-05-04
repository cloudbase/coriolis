# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging
from webob import exc

from coriolis import exception
from coriolis.api.v1 import utils as api_utils
from coriolis.api.v1.views import replica_view
from coriolis.api.v1.views import replica_tasks_execution_view
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
        show_deleted = api_utils._get_show_deleted(
            req.GET.get("show_deleted", None))
        context = req.environ["coriolis.context"]
        context.show_deleted = show_deleted
        context.can(replica_policies.get_replicas_policy_label("list"))
        return replica_view.collection(
            req, self._replica_api.get_replicas(
                context, include_tasks_executions=False))

    def detail(self, req):
        show_deleted = api_utils._get_show_deleted(
            req.GET.get("show_deleted", None))
        context = req.environ["coriolis.context"]
        context.show_deleted = show_deleted
        context.can(
            replica_policies.get_replicas_policy_label("show_executions"))
        return replica_view.collection(
            req, self._replica_api.get_replicas(
                context, include_tasks_executions=True))

    @api_utils.format_keyerror_message(resource='replica', method='create')
    def _validate_create_body(self, context, body):
        replica = body["replica"]

        origin_endpoint_id = replica["origin_endpoint_id"]
        destination_endpoint_id = replica["destination_endpoint_id"]
        destination_environment = replica.get(
            "destination_environment", {})
        instances = api_utils.validate_instances_list_for_transfer(
            replica.get('instances'))

        notes = replica.get("notes")

        source_environment = replica.get("source_environment", {})
        self._endpoints_api.validate_source_environment(
            context, origin_endpoint_id, source_environment)

        origin_minion_pool_id = replica.get(
            'origin_minion_pool_id')
        destination_minion_pool_id = replica.get(
            'destination_minion_pool_id')
        instance_osmorphing_minion_pool_mappings = replica.get(
            'instance_osmorphing_minion_pool_mappings', {})
        extras = [
            instance
            for instance in instance_osmorphing_minion_pool_mappings
            if instance not in instances]
        if extras:
            raise ValueError(
                "One or more instance OSMorphing pool mappings were "
                "provided for instances (%s) which are not part of the "
                "Replicas's declared instances (%s)" % (extras, instances))

        # TODO(aznashwan): until the provider plugin interface is updated
        # to have separate 'network_map' and 'storage_mappings' fields,
        # we add them as part of the destination environment:
        network_map = replica.get("network_map", {})
        api_utils.validate_network_map(network_map)
        destination_environment['network_map'] = network_map
        self._endpoints_api.validate_target_environment(
            context, destination_endpoint_id, destination_environment)

        user_scripts = replica.get('user_scripts', {})
        api_utils.validate_user_scripts(user_scripts)
        user_scripts = api_utils.normalize_user_scripts(
            user_scripts, instances)

        # NOTE(aznashwan): we validate the destination environment for the
        # import provider before appending the 'storage_mappings' parameter
        # for plugins with strict property name checks which do not yet
        # support storage mapping features:
        storage_mappings = replica.get("storage_mappings", {})
        api_utils.validate_storage_mappings(storage_mappings)

        destination_environment['storage_mappings'] = storage_mappings

        return (origin_endpoint_id, destination_endpoint_id,
                source_environment, destination_environment, instances,
                network_map, storage_mappings, notes,
                origin_minion_pool_id, destination_minion_pool_id,
                instance_osmorphing_minion_pool_mappings, user_scripts)

    def create(self, req, body):
        context = req.environ["coriolis.context"]
        context.can(replica_policies.get_replicas_policy_label("create"))

        (origin_endpoint_id, destination_endpoint_id,
         source_environment, destination_environment, instances, network_map,
         storage_mappings, notes, origin_minion_pool_id,
         destination_minion_pool_id,
         instance_osmorphing_minion_pool_mappings, user_scripts) = (
            self._validate_create_body(context, body))

        return replica_view.single(req, self._replica_api.create(
            context, origin_endpoint_id, destination_endpoint_id,
            origin_minion_pool_id, destination_minion_pool_id,
            instance_osmorphing_minion_pool_mappings, source_environment,
            destination_environment, instances, network_map,
            storage_mappings, notes, user_scripts))

    def delete(self, req, id):
        context = req.environ["coriolis.context"]
        context.can(replica_policies.get_replicas_policy_label("delete"))
        try:
            self._replica_api.delete(context, id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)

    @staticmethod
    def _update_storage_mappings(original_storage_mappings,
                                 new_storage_mappings):

        backend_mappings = original_storage_mappings.get(
            'backend_mappings', [])
        new_backend_mappings = new_storage_mappings.get('backend_mappings', [])
        new_backend_mapping_sources = [mapping['source'] for mapping in
                                       new_backend_mappings]

        disk_mappings = original_storage_mappings.get('disk_mappings', [])
        new_disk_mappings = new_storage_mappings.get('disk_mappings', [])
        new_disk_mappings_disk_ids = [mapping['disk_id'] for mapping in
                                      new_disk_mappings]

        non_duplicates_backend_mapping = []
        for mapping in backend_mappings:
            if mapping['source'] not in new_backend_mapping_sources:
                non_duplicates_backend_mapping.append(mapping)
            else:
                LOG.info("Storage Backend Mapping %s will be overwritten." %
                         mapping)

        non_duplicates_disk_mappings = []
        for mapping in disk_mappings:
            if mapping['disk_id'] not in new_disk_mappings_disk_ids:
                non_duplicates_disk_mappings.append(mapping)
            else:
                LOG.info("Storage Disk Mapping %s will be overwritten" %
                         mapping)

        non_duplicates_backend_mapping.extend(new_backend_mappings)
        non_duplicates_disk_mappings.extend(new_disk_mappings)
        storage_mappings = {
            'backend_mappings': non_duplicates_backend_mapping,
            'disk_mappings': non_duplicates_disk_mappings}

        default_storage_backend = (
            new_storage_mappings.get('default', None) or
            original_storage_mappings.get('default', None))
        if default_storage_backend:
            storage_mappings['default'] = default_storage_backend

        return storage_mappings

    @staticmethod
    def _get_updated_user_scripts(original_user_scripts, new_user_scripts):
        global_scripts = original_user_scripts.get('global', {})
        new_global_scripts = new_user_scripts.get('global', {})
        if new_global_scripts:
            global_scripts.update(new_global_scripts)

        instance_scripts = original_user_scripts.get('instances', {})
        new_instance_scripts = new_user_scripts.get('instances', {})
        if new_instance_scripts:
            instance_scripts.update(new_instance_scripts)

        user_scripts = {
            "global": global_scripts,
            "instances": instance_scripts,
        }

        return user_scripts

    def _get_merged_replica_values(self, replica, updated_values):
        """ Looks for the following keys in the original replica body and
        updated values (preferring the updated values where needed, but using
        `.update()` on dicts):
        "source_environment", "destination_environment", "network_map", "notes"
        Does special merging for the "storage_mappings" and "user_scripts"
        Returns a dict with the merged values (or at least all if the keys
        having a default value of {})
        """
        final_values = {}
        # NOTE: this just replaces options at the top-level and does not do
        # merging of container types (ex: lists, dicts)
        for option in [
                "source_environment", "destination_environment",
                "network_map"]:
            before = replica.get(option)
            after = updated_values.get(option)
            # NOTE: for Replicas created before the separation of these fields
            # in the DB there is the chance that some of these may be NULL:
            if before is None:
                before = {}
            if after is None:
                after = {}
            before.update(after)

            final_values[option] = before

        original_storage_mappings = replica.get('storage_mappings')
        if original_storage_mappings is None:
            original_storage_mappings = {}
        new_storage_mappings = updated_values.get('storage_mappings')
        if new_storage_mappings is None:
            new_storage_mappings = {}
        final_values['storage_mappings'] = self._update_storage_mappings(
            original_storage_mappings, new_storage_mappings)

        original_user_scripts = api_utils.validate_user_scripts(
            replica.get('user_scripts', {}))
        new_user_scripts = api_utils.validate_user_scripts(
            updated_values.get('user_scripts', {}))
        final_values['user_scripts'] = self._get_updated_user_scripts(
            original_user_scripts, new_user_scripts)

        if 'notes' in updated_values:
            final_values['notes'] = updated_values.get('notes', '')
        else:
            final_values['notes'] = replica.get('notes', '')

        # NOTE: until the provider plugin interface is updated
        # to have separate 'network_map' and 'storage_mappings' fields,
        # we add them as part of the destination environment:
        final_storage_mappings = final_values['storage_mappings']
        final_network_map = final_values['network_map']
        if final_storage_mappings:
            final_values['destination_environment'][
                'storage_mappings'] = final_storage_mappings
        if final_network_map:
            final_values['destination_environment'][
                'network_map'] = final_network_map

        minion_pool_fields = [
            "origin_minion_pool_id", "destination_minion_pool_id",
            "instance_osmorphing_minion_pool_mappings"]
        final_values.update({
            mpf: updated_values[mpf]
            for mpf in minion_pool_fields
            if mpf in updated_values})

        return final_values

    @api_utils.format_keyerror_message(resource='replica', method='update')
    def _validate_update_body(self, id, context, body):

        replica = self._replica_api.get_replica(context, id)
        replica_body = body['replica']
        origin_endpoint_id = replica_body.get('origin_endpoint_id', None)
        destination_endpoint_id = replica_body.get(
            'destination_endpoint_id', None)
        instances = body['replica'].get('instances', None)
        if origin_endpoint_id or destination_endpoint_id:
            raise exc.HTTPBadRequest(
                explanation="The source or destination endpoints for a "
                            "Coriolis Replica cannot be updated after its "
                            "creation. If the credentials of any of the "
                            "Replica's endpoints need updating, please update "
                            "the endpoints themselves.")
        if instances:
            raise exc.HTTPBadRequest(
                explanation="The list of instances of a Replica cannot be "
                            "updated")

        merged_body = self._get_merged_replica_values(
            replica, replica_body)

        replica_origin_endpoint_id = replica["origin_endpoint_id"]
        replica_destination_endpoint_id = replica[
            "destination_endpoint_id"]

        self._endpoints_api.validate_source_environment(
            context, replica_origin_endpoint_id,
            merged_body["source_environment"])

        destination_environment = merged_body["destination_environment"]
        self._endpoints_api.validate_target_environment(
            context, replica_destination_endpoint_id,
            destination_environment)

        api_utils.validate_network_map(merged_body["network_map"])

        api_utils.validate_storage_mappings(
            merged_body["storage_mappings"])

        user_scripts = merged_body['user_scripts']
        api_utils.validate_user_scripts(user_scripts)
        merged_body['user_scripts'] = api_utils.normalize_user_scripts(
            user_scripts, replica.get('instances', []))

        return merged_body

    def update(self, req, id, body):
        context = req.environ["coriolis.context"]
        context.can(replica_policies.get_replicas_policy_label("update"))

        updated_values = self._validate_update_body(id, context, body)
        try:
            return replica_tasks_execution_view.single(
                req, self._replica_api.update(req.environ['coriolis.context'],
                                              id, updated_values))
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)
        except exception.InvalidParameterValue as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(ReplicaController())
