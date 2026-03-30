# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import abc
import itertools

from oslo_log import log as logging
from six import with_metaclass

from coriolis import context
from coriolis import exception
from coriolis.osmorphing import base as base_osmorphing
from coriolis.osmorphing.osdetect import base as base_osdetect

LOG = logging.getLogger(__name__)


class BaseProvider(object, with_metaclass(abc.ABCMeta)):

    @property
    def platform(self) -> str:
        """Platform type."""
        raise NotImplementedError("Missing provider platform attribute.")


class BaseEndpointProvider(BaseProvider):

    @abc.abstractmethod
    def validate_connection(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
    ):
        """Validate the endpoint connection info.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters

        Schema validation is not performed by the caller and must
        be handled by the provider.
        """
        pass

    @abc.abstractmethod
    def get_connection_info_schema(self) -> str:
        """Get the provider specific connection info schema."""
        pass


class BaseEndpointInstancesProvider(BaseEndpointProvider):
    """Defines operations for listing instances off of Endpoints."""

    @abc.abstractmethod
    def get_instances(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        source_environment: dict,
        limit: int | None = None,
        last_seen_id: str | None = None,
        instance_name_pattern: str | None = None,
        refresh: bool = False,
    ) -> list[dict]:
        """Returns a list of instances.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param source_environment: provider specific source environment
                                   parameters
        :param limit: the maximum number of instances to retrieve
        :param last_seen_id: the last seen instance id, used for pagination
        :param instance_name_pattern: a name pattern used for filtering
                                      instances
        :param refresh: if True, forces a refresh of any cached instance data
        :returns: a list of dicts conforming to vm_instance_info_schema.json
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_instance(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        source_environment: dict,
        instance_name: str,
    ) -> dict:
        """Returns detailed info for a given instance.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param source_environment: provider specific source environment
                                   parameters
        :param instance_name: the name of the instance to retrieve.
        :returns: a dict conforming to vm_instance_info_schema.json

        TODO: Identical to "get_replica_instance_info", one of them should be
        deprecated.
        """
        raise NotImplementedError()


class BaseEndpointNetworksProvider(object, with_metaclass(abc.ABCMeta)):
    """Defines operations for endpoint networks."""

    @abc.abstractmethod
    def get_networks(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        env: dict,
    ) -> list[dict]:
        """Returns a list of network identifiers.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param env: provider specific environment options
        :returns: a list of dicts containing the "name" and "id" fields
        """
        raise NotImplementedError()


class BaseProviderSetupExtraLibsMixin(object, with_metaclass(abc.ABCMeta)):
    """ABC mix-in for providers which require extra libraries loaded."""

    @abc.abstractmethod
    def get_shared_library_directories(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
    ) -> list[str]:
        """Get directories containing shared libraries needed by the provider.

        The specified directories are expected to reside on the worker
        filesystem.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        """
        return []


class BaseEndpointDestinationOptionsProvider(
        object, with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def get_target_environment_options(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        env: dict | None = None,
        option_names: list[str] | None = None,
    ) -> list[dict]:
        """Returns all possible values for the target environment options, as
        well as any settings the options might have in the configuration files.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param env: dict: optional target environment options
        :param option_names: optional list of parameter names to
                             show values for

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
              settable through that parameter (any, all or none may be set)
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
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        env: dict | None = None,
        option_names: list[str] | None = None,
    ) -> list[dict]:
        """Returns all possible values for the source environment options, as
        well as any settings the options might have in the configuration files.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param env: dict: optional target environment options
        :param option_names: optional list of parameter names
                             to show values for

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
              settable through that parameter (any, all or none may be set)
            - for fields where both a name or ID may be returned, returning the
              name will be preferred. The provider must ensure that, if there
              are objects with the same name, the IDs of those objects are
              offered as an option instead of two identical names.
        """
        pass


class BaseInstanceProvider(BaseProvider):

    @abc.abstractmethod
    def get_os_morphing_tools(
        self,
        os_type: str,
        osmorphing_info: dict,
    ) -> list[base_osmorphing.BaseOSMorphingTools]:
        """Returns a list of possible OSMorphing classes for the given
        os type and osmorphing info.

        :param os_type: an operating system type such as "linux" or "windows".
                        See the list of constants for other OS types.
        :param osmorphing_info: a set of parameters controlling the OS morphing
                                process. See os_morphing_resources_schema.json.

        The OSMorphing classes will be asked to validate compatibility
        in order using their `check_os` method in order, so any classes whose
        `check_os` class methods might both return a positive result should be
        placed in the correct order (from more specific to less specific).
        """
        raise exception.OSMorphingToolsNotFound(os_type=os_type)

    def get_custom_os_detect_tools(
        self,
        os_type: str,
        osmorphing_info: dict,
    ) -> list[base_osdetect.BaseOSDetectTools]:
        """Returns a list of custom OSDetect classes.

        :param os_type: an operating system type such as "linux" or "windows".
                        See the list of constants for other OS types.
        :param osmorphing_info: a set of parameters controlling the OS morphing
                                process. See os_morphing_resources_schema.json.

        These detect tools will be run before the standard ones already
        present in the standard coriolis.osmorphing.osdetect module in case
        there will be any provider-specific supported OS releases.
        """
        return []


class BaseImportInstanceProvider(BaseInstanceProvider):

    @abc.abstractmethod
    def get_target_environment_schema(self) -> str:
        """Retrieve the provider specific target environment schema.

        Users can specify target environment parameters to control the way
        in which replicas and minions get deployed on the destination side by
        the import provider.

        These settings are provided at run time and usually take precedence
        over the coriolis-worker configuration.
        """
        pass

    def _get_destination_instance_name(
        self,
        export_info: dict,
        instance_name: str,
    ) -> str:
        """Helper to determine the preferred destination instance name.

        :param export_info: a dict describing the exported instance,
                            conforming to vm_export_info_schema.json
        :param instance_name: fallback instance name
        """
        dest_instance_name = export_info.get("name", instance_name)
        LOG.debug('Destination instance name for "%(instance_name)s": '
                  '"%(dest_instance_name)s"',
                  {"instance_name": instance_name,
                   "dest_instance_name": dest_instance_name})
        return dest_instance_name

    @abc.abstractmethod
    def deploy_os_morphing_resources(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        instance_deployment_info: dict,
    ) -> dict:
        """Deploy resources used as part of the OS morphing process.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: user provided target environment parameters
        :param instance_deployment_info: target instance information returned
                                         by the provider through the
                                         "deploy_replica_instance" method
        :returns: a dict containing the following fields:
            * os_morphing_resources
            * osmorphing_connection_info
            * osmorphing_info
            See os_morphing_resource_schema.json for the full schema.

        The provider will deploy a minion instance and attach the volumes
        specified in the "instance_deployment_info". The returned connection
        info will be used by the caller to initiate the OS morphing process,
        leveraging the OS morphing manager.

        In case of failure, any created resources must be cleaned up by the
        provider.
        """
        pass

    @abc.abstractmethod
    def delete_os_morphing_resources(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        os_morphing_resources: dict,
    ):
        """Cleanup resources used as part of the OS morphing process.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: user provided target environment parameters
        :param os_morphing_resources: the dict returned by the provider
                                      through "deploy_os_morphing_resources"
        """
        pass


class BaseReplicaExportValidationProvider(
        object, with_metaclass(abc.ABCMeta)):
    """Validate replica export parameters."""

    @abc.abstractmethod
    def validate_replica_export_input(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        instance_name: str,
        source_environment: dict,
    ) -> dict:
        """Validate the parameters and return the replica export info.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param instance_name: the name of the instance to export
        :param source_environment: provider specific source environment
                                   parameters
        :returns: the export info for the given instance,
                  conforming to vm_export_info_schema.json
        """
        return {}


class BaseReplicaImportValidationProvider(
        object, with_metaclass(abc.ABCMeta)):
    """Validate replica import parameters."""

    @abc.abstractmethod
    def validate_replica_import_input(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        export_info: dict,
        check_os_morphing_resources: bool = False,
        check_final_vm_params: bool = False,
    ):
        """Validates the provided replica import parameters.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: provider specific target environment
                                   parameters
        :param export_info: a dict describing the exported instance,
                            conforming to vm_export_info_schema.json
        :param check_os_morphing_resources: if set, the provider is expected to
                                            validate OS morphing parameters
        :param check_final_vm_params: if set, the provider is expected to
                                      validate replica instance parameters
        """
        pass

    @abc.abstractmethod
    def validate_replica_deployment_input(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        export_info: dict,
    ):
        """Validates the provided replica import parameters.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: provider specific target environment
                                   parameters
        :param export_info: a dict describing the exported instance,
                            conforming to vm_export_info_schema.json

        Same as "validate_replica_import_input", except that it doesn't
        explicitly state which set of parameters to validate.

        TODO: consider deprecating one of those two.
        """
        pass


class BaseReplicaImportProvider(BaseImportInstanceProvider):

    @abc.abstractmethod
    def deploy_replica_instance(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        instance_name: str,
        export_info: dict,
        volumes_info: list[dict],
        clone_disks: bool,
    ) -> dict:
        """Deploy replica instance resources.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: provider specific target environment
                                   parameters
        :param instance_name: the migrated instance name. Use the
                              "_get_destination_instance_name" to determine
                              the expected destination name.
        :param export_info: a dict describing the exported instance,
                            conforming to vm_export_info_schema.json
        :param volumes_info: a provider specific list of volumes that must
                             include the fields defined by
                             "volumes_info_schema.json".
                             Populated by "deploy_replica_disks".
        :param clone_disks: whether the specified volumes should be cloned or
                            attached directly to the instance.
        :returns: a provider specific dict defining instance characteristics

        At this point, the volumes specified in "volumes_info" are expected to
        contain the transferred data.

        "finalize_replica_instance_deployment" will be called afterwards to
        finalize the deployment.
        """
        pass

    @abc.abstractmethod
    def finalize_replica_instance_deployment(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        instance_deployment_info: dict,
    ) -> dict:
        """Finalize the replica instance deployment.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: provider specific target environment
                                   parameters
        :param instance_deployment_info: instance info returned by the provider
                                         through "deploy_replica_instance"
        :returns: a dict describing the migrated instance on the destination
                  platform, expected to have the same format as the export
                  info declared in "vm_export_info_schema.json".
        """
        return {}

    @abc.abstractmethod
    def cleanup_failed_replica_instance_deployment(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        instance_deployment_info: dict,
    ):
        """Cleanup a failed replica instance deployment.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: provider specific target environment
                                   parameters
        :param instance_deployment_info: instance info returned by the provider
                                         through "deploy_replica_instance"
        """
        pass

    @abc.abstractmethod
    def deploy_replica_disks(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        instance_name: str,
        export_info: dict,
        volumes_info: list[dict],
    ) -> list[dict]:
        """Create or update the replica disks.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: provider specific target environment
                                   parameters
        :param instance_name: the migrated instance name. Use
                              "_get_destination_instance_name" to determine
                              the expected destination name.
        :param export_info: a dict describing the exported instance,
                            conforming to vm_export_info_schema.json
        :param volumes_info: a provider specific list of volumes that must
                             include the fields defined by
                             "volumes_info_schema.json".
                             It starts as an empty list and gets populated
                             by subsequent "deploy_replica_disks" provider
                             calls, performed as part of disk transfer
                             operations.
        :returns: the updated list of volumes.

        The disks defined by the export info must be replicated on the
        destination cloud. The provider will recreate the volumes based on the
        size specified in the export info and other user provided
        "target_environment" parameters.

        The resulting volume info is expected to contain the original disk
        id from the export info, along with any other provider specific volume
        properties.

        "deploy_replica_disks" will be called whenever the user initiates a
        disk transfer. Be aware that the exported instance may be modified
        between Coriolis transfers.

        As such, the provider is expected to compare the "volumes_info"
        that it returned last time (now received as input parameter) with the
        current export info. It should then determine which disks need to be
        created, deleted or resized.

        Note that "deploy_replica_disks" is not expected to transfer data. That
        will be handled separately.

        If multiple storage backends are available, the provider may report
        them through the "get_storage" method. The "target_environment"
        parameter will then contain "storage_mappings", associating source
        storage backend identifiers with destination backend identifiers. The
        export provider will use this information to place the volumes
        (also called disks throughout the API) on the backend requested by
        the user.
        """
        pass

    @abc.abstractmethod
    def deploy_replica_target_resources(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        volumes_info: list[dict],
    ) -> dict:
        """Create a minion instance used for disk transfers.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: provider specific target environment
                                   parameters
        :param volumes_info: a provider specific list of volumes that must
                             include the fields defined by
                             "volumes_info_schema.json".
                             Populated by "deploy_replica_disks".
        :returns: a dict containing the following fields:
            * migr_resources: provider specific dict describing the minion
                              instance
            * volumes_info: updated list of volumes, containing
                            attachment path. See the below explanation for
                            setting "volume_dev".
            * connection_info: a dict containing minion connection details,
                conforming to disk_sync_resources_conn_info_schema.json and
                containing the following:
                * backend: backup writer type, one of:
                    * "ssh_backup_writer"
                    * "http_backup_writer"
                    * "file_backup_writer"
                * connection_details:
                    connection details returned by the writer, conforming to
                    disk_sync_resources_conn_info_schema.json.
                    See backup_writers.py.

        The provider will need to initialize the backup writer, passing it a
        connection info dict that usually contains the minion ip, a port used
        by the backup writer, credentials and/or a Paramiko keypair.

        The volumes contained by "volumes_info" must include the "volume_dev"
        field, specifying the attachment path as seen by the VM. Disk paths
        such as "/dev/sdb" must be handled carefully since the device naming
        can change depending on the order in which the disks are identified by
        the guest.

        Providers that can reliably determine the address or serial ID of the
        disk are encouraged to use udev links such as:
            * /dev/disk/by-path/pci-0000:18:00.0-scsi-0:2:0:0
            * /dev/disk/by-id/wwn-0x6d094660793802002afcbbe61cfbcd38
            * /dev/disk/by-id/scsi-36d094660793802002afcbbe61cfbcd38
            * /dev/disk/by-id/virtio-98fa455fc976

        Note that some providers need to rely on device names and leverage the
        replicator service to determine the paths identified by the guest. The
        workflow consists in attaching one disk at a time and identifying the
        last attached disk.

        Providers that support minion pools must still implement this method,
        which will be called if the user chooses not to use a minion pool.

        In case of failure, the provider is expected to perform cleanup.
        """
        pass

    @abc.abstractmethod
    def delete_replica_target_resources(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        migr_resources_dict: dict,
    ):
        """Cleanup any resources created by "deploy_replica_target_resources".

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: provider specific target environment
                                   parameters
        :param migr_resources_dict: provider specific dict returned by
                                    'deploy_replica_target_resources"
        """
        pass

    @abc.abstractmethod
    def delete_replica_disks(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        volumes_info: list[dict],
    ):
        """Delete replica disks created as part of the transfer process.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: provider specific target environment
                                   parameters
        :param volumes_info: a provider specific list of volumes that must
                             include the fields defined by
                             "volumes_info_schema.json".
                             Populated by "deploy_replica_disks".
        """
        pass

    @abc.abstractmethod
    def create_replica_disk_snapshots(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        volumes_info: list[dict],
    ) -> list[dict]:
        """Snapshot replica disks created as part of the transfer process.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: provider specific target environment
                                   parameters
        :param volumes_info: a provider specific list of volumes that must
                             include the fields defined by
                             "volumes_info_schema.json".
                             Populated by "deploy_replica_disks".
        :returns: updated volume info, containing snapshot information.

        Snapshots are meant to speed up replica deployments. Before initiating
        the deployment, Coriolis will snapshot the replica disks, which will
        then be attached to the replica instance.

        The replica disks will be recreated based on the snapshots, to be used
        by subsequent disk transfers or replica deployments.

        If snapshots are unsupported, volume cloning will be used instead.
        """
        pass

    @abc.abstractmethod
    def delete_replica_target_disk_snapshots(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        volumes_info: list[dict],
    ):
        """Delete all replica disk snapshots.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: provider specific target environment
                                   parameters
        :param volumes_info: a provider specific list of volumes that must
                             include the fields defined by
                             "volumes_info_schema.json".
                             Populated by "deploy_replica_disks".
        :returns: updated volume info, containing snapshot changes.

        Used to delete snapshots that were created while deploying the replica
        instance.
        """
        pass

    @abc.abstractmethod
    def restore_replica_disk_snapshots(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        volumes_info: list[dict],
    ) -> list[dict]:
        """Create new volumes using the snapshots.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: provider specific target environment
                                   parameters
        :param volumes_info: a provider specific list of volumes that must
                             include the fields defined by
                             "volumes_info_schema.json".
                             Populated by "deploy_replica_disks".
        :returns: updated volume info, containing the resulting volumes.

        The snapshots created by "create_replica_disk_snapshots" will be used
        to create new volumes. The original volumes are expected to be attached
        to replica instances at the time of this call.
        """
        pass


class BaseExportInstanceProvider(BaseInstanceProvider):

    @abc.abstractmethod
    def get_source_environment_schema(self) -> str:
        """Retrieve the provider specific source environment schema.

        Users can specify source environment parameters to control the way
        in which the instances are exported.

        These settings are provided at run time and usually take precedence
        over the coriolis-worker configuration.
        """
        pass


class BaseReplicaExportProvider(BaseExportInstanceProvider):

    @abc.abstractmethod
    def get_replica_instance_info(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        source_environment: dict,
        instance_name: str,
    ) -> dict:
        """Get exportable instance info.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param source_environment: provider specific source environment
                                   parameters
        :param instance_name: the name of the instance to retrieve
        :returns: a dict conforming to vm_export_info_schema.json

        TODO: identical to "get_instance", one of them should be deprecated.
        """
        pass

    @abc.abstractmethod
    def deploy_replica_source_resources(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        export_info: dict,
        source_environment: dict,
    ) -> dict:
        """Deploys a minion instance to facilitate disk transfers.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param export_info: a dict describing the exported instance,
                            conforming to vm_export_info_schema.json
        :param source_environment: provider specific source environment
                                   parameters
        :returns: a dict containing the following fields:
            * migr_resources: provider specific dict describing the created
                              resources
            * connection_info: minion connection information, conforming to
                               replication_worker_conn_info_schema.json

        Providers that do not use minion instances to perform disk transfers
        can omit this information.
        """
        pass

    @abc.abstractmethod
    def delete_replica_source_resources(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        source_environment: dict,
        migr_resources_dict: dict,
    ):
        """Cleanup any resources created by "deploy_replica_source_resources".

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param source_environment: provider specific source environment
                                   parameters
        :param migr_resources_dict: dict returned by
                                    "deploy_replica_source_resources"
        """
        pass

    @abc.abstractmethod
    def replicate_disks(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        source_environment: dict,
        instance_name: str,
        source_resources,
        source_conn_info: dict,
        target_conn_info: dict,
        volumes_info: list[bool],
        incremental: bool,
    ) -> list[dict]:
        """Replicate instance disks to the destination platform.

        :param ctxt: Coriolis request context
        :param connection_info: source endpoint connection parameters
        :param source_environment: provider specific source environment
                                   parameters
        :param instance_name: the name of the replicated instance
        :param source_resources: source minion information returned by
                                 "deploy_replica_source_resources"
        :param source_conn_info: source minion connection info, conforming to
                                 replication_worker_conn_info_schema.json
        :param target_conn_info: target minion connection info, conforming to
                                 disk_sync_resources_conn_info_schema.json.
                                 Usually returned by the backup writer,
                                 see backup_writers.py.
        :param volumes_info: destination volumes conforming to
                             disk_sync_resources_info_schema.json
        :param incremental: perform an incremental transfer, if possible.
                            Always enabled at the moment.
        :returns: the updated destination volume information

        If incremental transfers are supported, consider annotating the
        resulting volume info with transfer information.

        WARNING: if minion instances are used on the source side, the provider
        is expected to attach the volumes as part of this call, even if it
        supports minion pools. "attach_volumes_to_minion" is never called for
        source minions.
        """
        pass

    @abc.abstractmethod
    def delete_replica_source_snapshots(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        source_environment: dict,
        volumes_info: list[dict],
    ):
        """Delete any snapshots created during disk replication.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param source_environment: provider specific source environment
                                   parameters
        :param volumes_info: destination volumes conforming to
                             disk_sync_resources_info_schema.json
        """
        pass

    @abc.abstractmethod
    def shutdown_instance(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        source_environment: dict,
        instance_name: str,
    ):
        """Shutdown the specified instance.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param source_environment: provider specific source environment
                                   parameters
        :param instance_name: the name of the instance to be stopped.
        """
        pass


class BaseInstanceFlavorProvider(BaseProvider):
    @abc.abstractmethod
    def get_optimal_flavor(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        export_info: dict,
    ) -> str:
        """Get the optimal flavor for the exported instance.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: provider specific target environment
                                   parameters
        :param export_info: a dict describing the exported instance,
                            conforming to vm_export_info_schema.json
        :returns: the destination flavor name

        Some platforms have the concept of flavors, defining the amount of
        resources allocated to an instance.

        This method will determine the best suited flavor based on the
        specifications of the exported instance.
        """
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
    def get_storage(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
    ) -> dict:
        """Retrieve information about the available storage backends.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: provider specific target environment
                                   parameters
        :returns: a dict conforming to vm_storage_schema.json

        Each entry is expected to contain the name and id of the storage
        backend. The user will be allowed to associate source storage backends
        with destination storage backends, determining the location and
        characteristics of the transferred disks.
        """
        pass


class BaseUpdateSourceReplicaProvider(object, with_metaclass(abc.ABCMeta)):
    """ Class for replica export providers which offer the functionality
    of updating the parameters for a replica.
    """
    @abc.abstractmethod
    def check_update_source_environment_params(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        instance_name: str,
        volumes_info: list[dict],
        old_params: dict,
        new_params: dict,
    ) -> list[dict]:
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
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        export_info: dict,
        volumes_info: list[dict],
        old_params: dict,
        new_params: dict,
    ) -> list[dict]:
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


class _BaseMinionPoolProvider(
        object, with_metaclass(abc.ABCMeta)):
    """Minion Pool management functionality.

    Coriolis users will choose whether minion pools should be used or not.
    Even if a provider supports the minion pool functionality, it is still
    expected to implement the other methods used for minion management, such
    as:
        * Import providers (destination side):
            * deploy_replica_target_resources
            * delete_replica_target_resources
            * deploy_os_morphing_resources
            * delete_os_morphing_resources
        * Export providers (source side):
            * deploy_replica_source_resources
            * delete_replica_source_resources

    TODO: consider unifying the APIs.
    """

    @abc.abstractmethod
    def get_minion_pool_environment_schema(self) -> str:
        """Returns the schema for the minion pool options."""
        pass

    @abc.abstractmethod
    def get_minion_pool_options(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        env: dict | None = None,
        option_names: list[str] | None = None,
    ) -> list[dict]:
        """Returns possible environment options for minion pools.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param env: dict: optional target environment options
        :param option_names: optional list of parameter names to
                             show values for
        :returns: a list of dicts describing minion pool options.
                  See "get_target_environment_options" for examples.
        """
        pass

    @abc.abstractmethod
    def validate_minion_compatibility_for_transfer(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        export_info: dict,
        environment_options: list[dict],
        minion_properties: dict,
    ):
        """Validates compatibility between the pool's options and the options
        selected for a given transfer. Should raise if any options related to
        the minions in the pool might be deemed incompatible with the desited
        transfer options.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param export_info: a dict describing the exported instance,
                            conforming to vm_export_info_schema.json
        :param environment_options: provider specific pool options
        :param minion_properties: minion information
                                  returned by "create_minion"
        """
        pass

    @abc.abstractmethod
    def validate_minion_pool_environment_options(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        environment_options: list[dict],
    ):
        """Validates the provided pool options.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param environment_options: provider specific pool options
        """
        pass

    @abc.abstractmethod
    def set_up_pool_shared_resources(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        environment_options: list[dict],
        pool_identifier: str,
    ) -> dict:
        """Sets up supporting resources which can be re-used amongst the
        machines which will be spawned within the pool (e.g. a shared network)

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param environment_options: provider specific pool options
        :param pool_identifier: Coriolis allocated pool UUID
        :returns: provider specific dict describing the pool shared resources
        """
        pass

    @abc.abstractmethod
    def tear_down_pool_shared_resources(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        environment_options: list[dict],
        pool_shared_resources: dict,
    ):
        """Tears down all pool supporting resources.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param environment_options: provider specific pool options
        :param pool_shared_resources: dict returned by
                                      "set_up_pool_shared_resources"
        """
        pass

    @abc.abstractmethod
    def create_minion(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        environment_options: list[dict],
        pool_identifier: str,
        pool_os_type: str,
        pool_shared_resources: dict,
        new_minion_identifier: str,
    ) -> dict:
        """Create a minion instance.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param environment_options: provider specific pool options
        :param pool_identifier: Coriolis allocated pool UUID
        :param pool_os_type: an operating system type such as "linux" or
                             "windows". See the list of constants for other
                             OS types.
        :param pool_shared_resources: dict returned by
                                      "set_up_pool_shared_resources"
        :param new_minion_identifier: Coriolis allocated minion UUID

        :returns: a dict containing the following fields:
            * connection_info - minion connection information, conforming to
                                replication_worker_conn_info_schema.json
            * minion_provider_properties - provider specific properties
            * backup_writer_connection_info - a dict conforming to
                disk_sync_resources_conn_info_schema.json

        The provider is expected to initialize the backup writer.
        """
        pass

    @abc.abstractmethod
    def delete_minion(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        minion_properties: dict,
    ):
        """Delete the minion instance.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param minion_properties: provider specific properties returned by
                                  "create_minion" through
                                  "minion_provider_properties".
        """
        pass

    @abc.abstractmethod
    def shutdown_minion(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        minion_properties: dict,
    ):
        """Shutdown the minion instance.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param minion_properties: provider specific properties returned by
                                  "create_minion" through
                                  "minion_provider_properties".
        """
        pass

    @abc.abstractmethod
    def start_minion(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        minion_properties: dict,
    ):
        """Start the minion instance.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param minion_properties: provider specific properties returned by
                                  "create_minion" through
                                  "minion_provider_properties".
        """
        pass

    @abc.abstractmethod
    def attach_volumes_to_minion(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        minion_properties: dict,
        volumes_info: list[dict],
    ) -> dict:
        """Attach volumes to minion instance.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param minion_properties: provider specific properties returned by
                                  "create_minion" through
                                  "minion_provider_properties".
        :param volumes_info: provider specific list of volumes, conforming
                             to volumes_info_schema.json
        :returns: a dict containing the following fields
            * minion_properties - updated minion properties
            * volumes_info - updated volume info, specifying the attachment
                             location through "volume_dev"

        See the "deploy_replica_target_resources" description for more details
        about the "volume_dev" field.

        WARNING: currently unused on source side, the export providers are
        expected to attach the volumes as part of "replicate_disks".
        """
        pass

    @abc.abstractmethod
    def detach_volumes_from_minion(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        minion_properties: dict,
        volumes_info: list[dict],
    ):
        """Detach volumes from minion instance.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param minion_properties: provider specific properties returned by
                                  "create_minion" through
                                  "minion_provider_properties".
        :param volumes_info: provider specific list of volumes, conforming
                             to volumes_info_schema.json
        """
        pass

    @abc.abstractmethod
    def healthcheck_minion(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        minion_properties: dict,
        minion_connection_info: dict,
    ):
        """Verify the state of the minion instance.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param minion_properties: provider specific properties returned by
                                  "create_minion" through
                                  "minion_provider_properties".
        :param minion_connection_info: minion connection info returned by
                                       "create_minion"

        The provider must raise an exception if the minion instance is not
        reachable.
        """
        pass


class BaseSourceMinionPoolProvider(_BaseMinionPoolProvider):
    pass


class BaseDestinationMinionPoolProvider(_BaseMinionPoolProvider):

    @abc.abstractmethod
    def validate_osmorphing_minion_compatibility_for_transfer(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        export_info: dict,
        environment_options: list[dict],
        minion_properties: dict,
    ):
        """ Validates compatibility between the OSMorphing pool's options and
        the options selected for a given transfer. Should raise if any options
        of the minions in the pool might be deemed incompatible with the
        desired transfer options.

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param export_info: a dict describing the exported instance,
                            conforming to vm_export_info_schema.json
        :param environment_options: provider specific pool options
        :param minion_properties: provider specific properties returned by
                                  "create_minion" through
                                  "minion_provider_properties".
        """
        pass

    @abc.abstractmethod
    def get_additional_os_morphing_info(
        self,
        ctxt: context.RequestContext,
        connection_info: dict,
        target_environment: dict,
        instance_deployment_info: dict,
    ) -> dict:
        """ This method should return any additional 'osmorphing_info'
        as defined in coriolis.schemas.CORIOLIS_OS_MORPHING_RESOURCES_SCHEMA

        :param ctxt: Coriolis request context
        :param connection_info: endpoint connection parameters
        :param target_environment: user provided target environment parameters
        :param instance_deployment_info: target instance information returned
                                         by the provider through the
                                         "deploy_replica_instance" method

        """
        pass


# Unused deprecated classes, kept for backwards compatibility in case there are
# providers that still inherit them.
class BaseMigrationExportValidationProvider:
    pass


class BaseMigrationImportValidationProvider:
    pass


class BaseImportProvider(BaseImportInstanceProvider):
    pass


class BaseExportProvider(BaseExportInstanceProvider):
    pass
