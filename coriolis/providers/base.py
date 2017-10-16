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
    def get_instances(self, ctxt, connection_info, limit=None,
                      last_seen_id=None, instance_name_pattern=None):
        """Returns a list of instances."""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_instance(self, ctxt, connection_info, instance_name):
        """Returns detailed info for a given instance."""
        raise NotImplementedError()


class BaseEndpointNetworksProvider(object, with_metaclass(abc.ABCMeta)):
    """Defines operations for endpoints networks."""

    @abc.abstractmethod
    def get_networks(self, ctxt, connection_info, env):
        """Returns a list of networks """
        raise NotImplementedError()


class BaseInstanceProvider(BaseProvider):

    def get_os_morphing_tools(self, conn, osmorphing_info):
        raise exception.OSMorphingToolsNotFound()


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
    def deploy_os_morphing_resources(self, ctxt, connection_info,
                                     instance_deployment_info):
        pass

    @abc.abstractmethod
    def delete_os_morphing_resources(self, ctxt, connection_info,
                                     os_morphing_resources):
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
    def deploy_disk_copy_resources(self, ctxt, connection_info,
                                   target_environment, volumes_info):
        pass

    @abc.abstractmethod
    def delete_disk_copy_resources(self, ctxt, connection_info,
                                   target_resources_dict):
        pass

    @abc.abstractmethod
    def finalize_import_instance(self, ctxt, connection_info,
                                 instance_deployment_info):
        pass

    @abc.abstractmethod
    def cleanup_failed_import_instance(self, ctxt, connection_info,
                                       instance_deployment_info):
        pass


class BaseReplicaImportProvider(BaseImportInstanceProvider):

    @abc.abstractmethod
    def deploy_replica_instance(self, ctxt, connection_info,
                                target_environment, instance_name, export_info,
                                volumes_info, clone_disks):
        pass

    @abc.abstractmethod
    def finalize_replica_instance_deployment(self, ctxt, connection_info,
                                             instance_deployment_info):
        pass

    @abc.abstractmethod
    def cleanup_failed_replica_instance_deployment(self, ctxt, connection_info,
                                                   instance_deployment_info):
        pass

    @abc.abstractmethod
    def deploy_replica_disks(self, ctxt, connection_info, target_environment,
                             instance_name, export_info, volumes_info):
        pass

    @abc.abstractmethod
    def deploy_replica_target_resources(self, ctxt, connection_info,
                                        target_environment, volumes_info):
        pass

    @abc.abstractmethod
    def delete_replica_target_resources(self, ctxt, connection_info,
                                        migr_resources_dict):
        pass

    @abc.abstractmethod
    def delete_replica_disks(self, ctxt, connection_info, volumes_info):
        pass

    @abc.abstractmethod
    def create_replica_disk_snapshots(self, ctxt, connection_info,
                                      volumes_info):
        pass

    @abc.abstractmethod
    def delete_replica_disk_snapshots(self, ctxt, connection_info,
                                      volumes_info):
        pass

    @abc.abstractmethod
    def restore_replica_disk_snapshots(self, ctxt, connection_info,
                                       volumes_info):
        pass


class BaseExportProvider(BaseInstanceProvider):

    @abc.abstractmethod
    def export_instance(self, ctxt, connection_info, instance_name,
                        export_path):
        """Exports the given instance.

         Exports the instance given by its name from the given source cloud
        to the provided export directory path using the given connection info.
        """
        pass


class BaseReplicaExportProvider(BaseInstanceProvider):

    @abc.abstractmethod
    def get_replica_instance_info(self, ctxt, connection_info, instance_name):
        pass

    @abc.abstractmethod
    def deploy_replica_source_resources(self, ctxt, connection_info):
        pass

    @abc.abstractmethod
    def delete_replica_source_resources(self, ctxt, connection_info,
                                        migr_resources_dict):
        pass

    @abc.abstractmethod
    def replicate_disks(self, ctxt, connection_info, instance_name,
                        source_conn_info, target_conn_info, volumes_info,
                        incremental):
        pass

    @abc.abstractmethod
    def shutdown_instance(self, ctxt, connection_info, instance_name):
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
        LOG.debug("Loading osmorphing instance: %s", cls)
        tools = cls(
            conn, os_root_dir, os_root_dev, hypervisor_type, event_manager)
        LOG.debug("Testing OS morphing tools: %s", cls.__name__)
        os_info = tools.check_os()
        if os_info:
            return (tools, os_info)
    raise exception.OSMorphingToolsNotFound()
