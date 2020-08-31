# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import abc
import itertools

from oslo_log import log as logging
from six import with_metaclass

from coriolis import exception

LOG = logging.getLogger(__name__)


class BaseProvider(object, with_metaclass(abc.ABCMeta)):

    @property
    def platform(self):
        raise NotImplementedError("Missing provider platform attribute.")


class BaseEndpointProvider(BaseProvider):

    @abc.abstractmethod
    def validate_connection(self, ctxt, connection_info):
        pass

    @abc.abstractmethod
    def get_connection_info_schema(self):
        pass


class BaseEndpointInstancesProvider(BaseEndpointProvider):
    """Defines operations for listing instances off of Endpoints."""

    @abc.abstractmethod
    def get_instances(self, ctxt, connection_info, source_environment,
                      limit=None, last_seen_id=None,
                      instance_name_pattern=None):
        """Returns a list of instances."""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_instance(
            self, ctxt, connection_info, source_environment, instance_name):
        """Returns detailed info for a given instance."""
        raise NotImplementedError()


class BaseEndpointNetworksProvider(object, with_metaclass(abc.ABCMeta)):
    """Defines operations for endpoints networks."""

    @abc.abstractmethod
    def get_networks(self, ctxt, connection_info, env):
        """Returns a list of networks """
        raise NotImplementedError()


class BaseProviderSetupExtraLibsMixin(object, with_metaclass(abc.ABCMeta)):
    """ ABC mixin for providers which require extra libraries loaded. """

    @abc.abstractmethod
    def get_shared_library_directories(self, ctxt, connection_info):
        """ Should return a list of string paths to directories somewhere in
        the worker filesystem where extra libraries required for the provider
        are located.
        """
        return []


class BaseEndpointDestinationOptionsProvider(
        object, with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def get_target_environment_options(
            self, ctxt, connection_info, env=None, option_names=None):
        """ Returns all possible values for the target environment options, as
        well as any settings the options might have in the configuration files.

        param env: dict: optional target environment options
        param option_names: list(str): optional list of parameter names to show
        values for

        Example returned values for the following options:
        schema = {
            "properties": {
                "migr_network": {
                    "type": "string"
                },
                "security_groups": {
                    "type": "array",
                    "items": { "type": "string" }
                },
                "migr_image": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "name": { "type": "integer" }
                    }
                }
            }
        }
        The provider should return:
        options = [
            {
                "name": "migr_network",
                "values": ["net1", "net2", "net3"],
                "config_default": "net2"},
            {
                "name": "security_groups",
                "values": ["secgroup1", "secgroup2", "secgroup3"],
                "config_default": ["secgroup2", "secgroup3"]},
            {
                "name": "migr_image",
                "values": [
                    {"name": "testimage1", "id": 101},
                    {"name": "testimg2", "id": 4}],
                "config_default": {"name": "testimg2", "id": 4}}}
        ]
        Observations:
            - base types such as 'integer' or 'string' are preserved
            - 'array' types will return an array with all the options which are
              settable through that paramter (any, all or none may be set)
            - for fields where both a name or ID may be returned, returning the
              name will be preferred. The provider must ensure that, if there
              are objects with the same name, the IDs of those objects are
              offered as an option instead of two identical names.
        """
        pass


class BaseEndpointSourceOptionsProvider(
        object, with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def get_source_environment_options(
            self, ctxt, connection_info, env=None, option_names=None):
        """ Returns all possible values for the source environment options, as
        well as any settings the options might have in the configuration files.

        param env: dict: optional target environment options
        param option_names: list(str): optional list of parameter names to show
        values for

        Example returned values for the following options:
        schema = {
            "properties": {
                "migr_network": {
                    "type": "string"
                },
                "security_groups": {
                    "type": "array",
                    "items": { "type": "string" }
                },
                "migr_image": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "name": { "type": "integer" }
                    }
                }
            }
        }
        The provider should return:
        options = [
            {
                "name": "migr_network",
                "values": ["net1", "net2", "net3"],
                "config_default": "net2"},
            {
                "name": "security_groups",
                "values": ["secgroup1", "secgroup2", "secgroup3"],
                "config_default": ["secgroup2", "secgroup3"]},
            {
                "name": "migr_image",
                "values": [
                    {"name": "testimage1", "id": 101},
                    {"name": "testimg2", "id": 4}],
                "config_default": {"name": "testimg2", "id": 4}}}
        ]
        Observations:
            - base types such as 'integer' or 'string' are preserved
            - 'array' types will return an array with all the options which are
              settable through that paramter (any, all or none may be set)
            - for fields where both a name or ID may be returned, returning the
              name will be preferred. The provider must ensure that, if there
              are objects with the same name, the IDs of those objects are
              offered as an option instead of two identical names.
        """
        pass


class BaseInstanceProvider(BaseProvider):

    @abc.abstractmethod
    def get_os_morphing_tools(self, os_type, osmorphing_info):
        """ Returns a list of possible OSMorphing classes for the given
        os type and osmorphing info.
        The OSMorphing classes will be asked to validate compatibility
        in order using their `check_os` method in order, so any classes whose
        `check_os` classmethods might both return a positive result should be
        placed in the correct order (from more specific to less specific).
        """
        raise exception.OSMorphingToolsNotFound(os_type=os_type)


    def get_custom_os_detect_tools(self, os_type, osmorphing_info):
        """ Returns a list of custom OSDetect classes which inherit from
        coriolis.osmorphing.osdetect.base.BaseOSDetectTools.
        These detect tools will be run before the standard ones already
        present in the standard coriolis.osmorphing.osdetect module in case
        there will be any provider-specific supported OS releases.
        """
        return []


class BaseImportInstanceProvider(BaseInstanceProvider):

    @abc.abstractmethod
    def get_target_environment_schema(self):
        pass

    def _get_destination_instance_name(self, export_info, instance_name):
        dest_instance_name = export_info.get("name", instance_name)
        LOG.debug('Destination instance name for "%(instance_name)s": '
                  '"%(dest_instance_name)s"',
                  {"instance_name": instance_name,
                   "dest_instance_name": dest_instance_name})
        return dest_instance_name

    @abc.abstractmethod
    def deploy_os_morphing_resources(
            self, ctxt, connection_info, target_environment,
            instance_deployment_info):
        pass

    @abc.abstractmethod
    def delete_os_morphing_resources(
            self, ctxt, connection_info, target_environment,
            os_morphing_resources):
        pass


class BaseMigrationExportValidationProvider(
        object, with_metaclass(abc.ABCMeta)):
    """ Defines methods to be called for migration export input validation """

    @abc.abstractmethod
    def validate_migration_export_input(
            self, ctxt, connection_info, instance_name, source_environment):
        """ Should verify the provided 'connection_info' and
        'source_environment' and return the expected Migration
        export info for the given VM. """
        return {}


class BaseReplicaExportValidationProvider(
        object, with_metaclass(abc.ABCMeta)):
    """ Defines methods to be called for replica export input validation """

    @abc.abstractmethod
    def validate_replica_export_input(
            self, ctxt, connection_info, instance_name, source_environment):
        """ Should verify the provided 'connection_info' and
        'source_environment' and return the expected Migration
        export info for the given VM. """
        return {}


class BaseMigrationImportValidationProvider(
        object, with_metaclass(abc.ABCMeta)):
    """ Defines methods to be called for migration import input validation """

    @abc.abstractmethod
    def validate_migration_import_input(
            self, ctxt, connection_info, target_environment, export_info):
        """ Validates the provided Migration parameters """
        pass


class BaseReplicaImportValidationProvider(
        object, with_metaclass(abc.ABCMeta)):
    """ Defines methods to be called for replica import input validation """

    @abc.abstractmethod
    def validate_replica_import_input(
            self, ctxt, connection_info, target_environment, export_info,
            check_os_morphing_resources=False, check_final_vm_params=False):
        """ Validates the provided Replica parameters """
        pass

    @abc.abstractmethod
    def validate_replica_deployment_input(
            self, ctxt, connection_info, target_environment, export_info):
        """ Validates the provided Replica deployment parameters """
        pass


class BaseImportProvider(BaseImportInstanceProvider):

    @abc.abstractmethod
    def import_instance(self, ctxt, connection_info, target_environment,
                        instance_name, export_info):
        """Imports the given instance.

        Imports the instance given by its name to the specified target
        environment within the destination cloud based on the provided
        connection and export info.
        """
        pass

    @abc.abstractmethod
    def deploy_disk_copy_resources(
            self, ctxt, connection_info, target_environment, volumes_info):
        pass

    @abc.abstractmethod
    def delete_disk_copy_resources(
            self, ctxt, connection_info, target_environment,
            target_resources_dict):
        pass

    @abc.abstractmethod
    def finalize_import_instance(
            self, ctxt, connection_info, target_environment,
            instance_deployment_info):
        """ Should return a dict with the info of the migrated VM on the
        destination platform in the same format as offered by
        'BaseExportProvider.export_instance()'.
        """
        return {}

    @abc.abstractmethod
    def cleanup_failed_import_instance(
            self, ctxt, connection_info, target_environment,
            instance_deployment_info):
        pass


class BaseReplicaImportProvider(BaseImportInstanceProvider):

    @abc.abstractmethod
    def deploy_replica_instance(
            self, ctxt, connection_info, target_environment,
            instance_name, export_info, volumes_info, clone_disks):
        pass

    @abc.abstractmethod
    def finalize_replica_instance_deployment(
            self, ctxt, connection_info, target_environment,
            instance_deployment_info):
        """ Should return a dict with the info of the migrated VM on the
        destination platform in the same format as offered by
        'BaseExportProvider.export_instance()'.
        """
        return {}

    @abc.abstractmethod
    def cleanup_failed_replica_instance_deployment(
            self, ctxt, connection_info, target_environment,
            instance_deployment_info):
        pass

    @abc.abstractmethod
    def deploy_replica_disks(
            self, ctxt, connection_info, target_environment, instance_name,
            export_info, volumes_info):
        pass

    @abc.abstractmethod
    def deploy_replica_target_resources(
            self, ctxt, connection_info, target_environment, volumes_info):
        pass

    @abc.abstractmethod
    def delete_replica_target_resources(
            self, ctxt, connection_info, target_environment,
            migr_resources_dict):
        pass

    @abc.abstractmethod
    def delete_replica_disks(
            self, ctxt, connection_info, target_environment, volumes_info):
        pass

    @abc.abstractmethod
    def create_replica_disk_snapshots(
            self, ctxt, connection_info, target_environment, volumes_info):
        pass

    @abc.abstractmethod
    def delete_replica_target_disk_snapshots(
            self, ctxt, connection_info, target_environment, volumes_info):
        pass

    @abc.abstractmethod
    def restore_replica_disk_snapshots(
            self, ctxt, connection_info, target_environment, volumes_info):
        pass


class BaseExportInstanceProvider(BaseInstanceProvider):

    @abc.abstractmethod
    def get_source_environment_schema(self):
        pass


class BaseExportProvider(BaseExportInstanceProvider):

    @abc.abstractmethod
    def export_instance(self, ctxt, connection_info, source_environment,
                        instance_name, export_path):
        """Exports the given instance.

         Exports the instance given by its name from the given source cloud
        to the provided export directory path using the given connection info.
        """
        pass


class BaseReplicaExportProvider(BaseExportInstanceProvider):

    @abc.abstractmethod
    def get_replica_instance_info(self, ctxt, connection_info,
                                  source_environment, instance_name):
        pass

    @abc.abstractmethod
    def deploy_replica_source_resources(self, ctxt, connection_info,
                                        export_info, source_environment):
        pass

    @abc.abstractmethod
    def delete_replica_source_resources(self, ctxt, connection_info,
                                        source_environment,
                                        migr_resources_dict):
        pass

    @abc.abstractmethod
    def replicate_disks(self, ctxt, connection_info, source_environment,
                        instance_name, source_resources, source_conn_info,
                        target_conn_info, volumes_info, incremental):
        pass

    @abc.abstractmethod
    def delete_replica_source_snapshots(
            self, ctxt, connection_info, source_environment, volumes_info):
        pass

    @abc.abstractmethod
    def shutdown_instance(self, ctxt, connection_info, source_environment,
                          instance_name):
        pass


class BaseInstanceFlavorProvider(BaseProvider):
    @abc.abstractmethod
    def get_optimal_flavor(self, ctxt, connection_info, target_environment,
                           export_info):
        pass


def get_os_morphing_tools_helper(conn, os_morphing_tools_clss,
                                 hypervisor_type, os_type, os_root_dir,
                                 os_root_dev, event_manager):
    if os_type and os_type not in os_morphing_tools_clss:
        raise exception.OSMorphingToolsNotFound(
            "Unsupported OS type: %s" % os_type)

    for cls in os_morphing_tools_clss.get(
            os_type, itertools.chain(*os_morphing_tools_clss.values())):
        LOG.debug("Checking using OSMorphing class: %s", cls)
        tools = cls(
            conn, os_root_dir, os_root_dev, hypervisor_type, event_manager)
        LOG.debug("Testing OS morphing tools: %s", cls.__name__)
        os_info = tools.check_os()
        if os_info:
            return (tools, os_info)
    raise exception.OSMorphingToolsNotFound(os_type=os_type)


class BaseEndpointStorageProvider(object, with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def get_storage(self, ctxt, connection_info, target_environment):
        """ Returns all the storage options available to the given
        credentials within the provided target_environment.
        """
        pass


class BaseUpdateSourceReplicaProvider(object, with_metaclass(abc.ABCMeta)):
    """ Class for replica export providers which offer the functionality
    of updating the parameters for a replica.
    """
    @abc.abstractmethod
    def check_update_source_environment_params(
            self, ctxt, connection_info, instance_name, volumes_info,
            old_params, new_params):
        """ Checks that any existing replica resources for the VM given by its
        `export_info` which were replicated using the `old_params` is
        compatible with the `new_params`. The `old_params` and `new_params`
        refer to either the `source_environment` replica fields.
        This method should raise and error given incompatible `new_params` and
        also perform the necessary update procedures if they are compatible.

        return on success: updated `volumes_info`

        NOTE: If there is no `volumes_info` present due to the replica never
        having been executed or the replica disks having been deleted, this
        method should simply return the empty `volumes_info` it was given.
        """


class BaseUpdateDestinationReplicaProvider(
        object, with_metaclass(abc.ABCMeta)):
    """ Class for replica import providers which offer the functionality
    of updating the parameters for a replica.
    """
    @abc.abstractmethod
    def check_update_destination_environment_params(
            self, ctxt, connection_info, export_info, volumes_info,
            old_params, new_params):
        """ Checks that any existing replica resources for the VM given by its
        `export_info` which were replicated using the `old_params` is
        compatible with the `new_params`. The `old_params` and `new_params`
        refer to the `destination_environment` replica fields.
        This method should raise and error given incompatible `new_params` and
        also perform the necessary update procedures if they are compatible.

        return on success: updated `volumes_info`

        NOTE: If there is no `volumes_info` present due to the replica never
        having been executed or the replica disks having been deleted, this
        method should simply return the empty `volumes_info` it was given.
        """


class BaseMinionPoolProvider(
        object, with_metaclass(abc.ABCMeta)):
    """ Class for providers which offer Minion Pool management functionality.
    """

    @abc.abstractmethod
    def get_minion_pool_environment_schema(self):
        """ Returns the schema for the minion pool options. """
        pass

    @abc.abstractmethod
    def get_minion_pool_options(
            self, ctxt, connection_info, env=None, option_names=None):
        """ Returns possible environment options for minion pools. """
        pass

    @abc.abstractmethod
    def validate_minion_compatibility_for_transfer(
            self, ctxt, connection_info, environment_options,
            transfer_options, storage_mappings):
        """ Validates compatibility between the pool's options and the options
        selected for a given transfer. Should raise if any options related to
        the minions in the pool might be deemed incompatible with the desited
        transfer options.
        """
        pass

    @abc.abstractmethod
    def validate_pool_options(
            self, ctxt, connection_info, environment_options):
        """ Validates the provided pool options. """
        pass

    @abc.abstractmethod
    def setup_pool_supporting_resources(
            self, ctxt, connection_info, environment_options, pool_identifier):
        """ Sets up supporting resources which can be re-used amongst the
        machines which will be spawned within the pool (e.g. a shared network)
        """
        pass

    @abc.abstractmethod
    def teardown_pool_supporting_resources(
            self, ctxt, connection_info, environment_options,
            pool_supporting_resources):
        """ Tears down all pool supporting resources. """
        pass

    @abc.abstractmethod
    def create_minion(
            self, ctxt, connection_info, environment_options,
            new_minion_identifier):
        pass

    @abc.abstractmethod
    def delete_minion(
            self, ctxt, connection_info, environment_options,
            minion_properties):
        pass

    @abc.abstractmethod
    def shutdown_minion(
            self, ctxt, connection_info, environment_options,
            minion_properties):
        pass

    @abc.abstractmethod
    def start_minion(
            self, ctxt, connection_info, environment_options,
            minion_properties):
        pass

    @abc.abstractmethod
    def attach_volume_to_minion(
            self, ctxt, connection_info, environment_options,
            minion_properties, volume_info):
        pass

    @abc.abstractmethod
    def detach_volume_from_minion(
            self, ctxt, connection_info, environment_options,
            minion_properties, volume_info):
        pass
