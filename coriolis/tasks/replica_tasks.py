# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis import constants
from coriolis import events
from coriolis import exception
from coriolis.providers import backup_writers
from coriolis.providers import factory as providers_factory
from coriolis import schemas
from coriolis.tasks import base
from coriolis import utils

LOG = logging.getLogger(__name__)


def _get_volumes_info(task_info):
    volumes_info = task_info.get("volumes_info", [])
    if not volumes_info:
        raise exception.InvalidActionTasksExecutionState(
            "No volumes information present")
    return volumes_info


def _check_ensure_volumes_info_ordering(export_info, volumes_info):
    """ Returns a new list of volumes_info, ensuring that the order of
    the disks in 'volumes_info' is consistent with the order that the
    disks appear in 'export_info[devices][disks]'
    """
    instance = export_info.get(
        'instance_name',
        export_info.get('name', export_info['id']))

    vol_info_cpy = utils.sanitize_task_info(
        {"volumes_info": volumes_info}).get("volumes_info", [])

    ordered_volumes_info = []
    for disk in export_info['devices']['disks']:
        disk_id = disk['id']
        matching_volumes = [
            vol for vol in volumes_info if vol['disk_id'] == disk_id]
        if not matching_volumes:
            LOG.error(
                "Could not find source disk '%s' (ID '%s') in Replica "
                "volumes info: %s", disk, disk_id, vol_info_cpy)
            raise exception.InvalidActionTasksExecutionState(
                "Source disk with ID '%s' not recognized. If this disk is "
                "newly added to the instance, please make sure to Execute the "
                "Replica before Updating. Check logs for more "
                "information." % disk_id)
        elif len(matching_volumes) > 1:
            LOG.error(
                "Multiple disks with ID '%s' found in Replica volumes "
                "info: %s", disk_id, vol_info_cpy)
            raise exception.InvalidActionTasksExecutionState(
                "Multiple disks with ID '%s' found in Replica volumes info. "
                "Please check that the instance doesn't have the same volume "
                "attached twice, or whether there's a UUID collision between "
                "its disks. Check the logs for more information." % disk_id)

        ordered_volumes_info.append(matching_volumes[0])

    ordered_vol_info_cpy = utils.sanitize_task_info(
        {"volumes_info": ordered_volumes_info}).get("volumes_info", [])

    LOG.debug(
        "volumes_info returned by provider for instance "
        "'%s': %s", instance, vol_info_cpy)
    LOG.debug(
        "volumes_info for instance '%s' after "
        "reordering: %s", instance, ordered_vol_info_cpy)

    return ordered_volumes_info


class GetInstanceInfoTask(base.TaskRunner):
    """ Task which gathers the export info for a VM.  """

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_SOURCE

    @classmethod
    def get_required_task_info_properties(cls):
        return ["source_environment"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["export_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_SOURCE: [
                constants.PROVIDER_TYPE_REPLICA_EXPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_REPLICA_EXPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, origin)

        source_environment = task_info['source_environment']
        export_info = provider.get_replica_instance_info(
            ctxt, connection_info, source_environment, instance)

        # Validate the output
        schemas.validate_value(
            export_info, schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA)

        return {
            'export_info': export_info}


class ShutdownInstanceTask(base.TaskRunner):
    """ Task which shuts down a VM. """

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_SOURCE

    @classmethod
    def get_required_task_info_properties(cls):
        return ["source_environment"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return []

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_SOURCE: [
                constants.PROVIDER_TYPE_REPLICA_EXPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_REPLICA_EXPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, origin)

        source_environment = task_info['source_environment']
        provider.shutdown_instance(ctxt, connection_info, source_environment,
                                   instance)
        return {}


class ReplicateDisksTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        # NOTE: considering Replication reads from one end (be it PMR minion
        # or otherwise) to the disk writer minion on the destination,
        # replicate_disks would need access to both:
        return constants.TASK_PLATFORM_BILATERAL

    @classmethod
    def get_required_task_info_properties(cls):
        return [
            "export_info", "volumes_info", "source_environment",
            "source_resources",
            "source_resources_connection_info",
            "target_resources_connection_info"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["volumes_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_SOURCE: [
                constants.PROVIDER_TYPE_REPLICA_EXPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_REPLICA_EXPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, origin)
        export_info = task_info["export_info"]

        volumes_info = _get_volumes_info(task_info)
        schemas.validate_value(
            {"volumes_info": volumes_info},
            schemas.CORIOLIS_DISK_SYNC_RESOURCES_INFO_SCHEMA)

        migr_source_conn_info = task_info["source_resources_connection_info"]
        if migr_source_conn_info:
            schemas.validate_value(
                migr_source_conn_info,
                schemas.CORIOLIS_REPLICATION_WORKER_CONN_INFO_SCHEMA)
            migr_source_conn_info = base.unmarshal_migr_conn_info(
                migr_source_conn_info)

        migr_target_conn_info = task_info["target_resources_connection_info"]
        if migr_target_conn_info:
            schemas.validate_value(
                migr_target_conn_info,
                schemas.CORIOLIS_DISK_SYNC_RESOURCES_CONN_INFO_SCHEMA)
            migr_target_conn_info['connection_details'] = (
                base.unmarshal_migr_conn_info(
                    migr_target_conn_info['connection_details']))
        incremental = task_info.get("incremental", True)

        source_environment = task_info['source_environment']

        source_resources = task_info.get('source_resources', {})
        volumes_info = provider.replicate_disks(
            ctxt, connection_info, source_environment, instance,
            source_resources, migr_source_conn_info, migr_target_conn_info,
            volumes_info, incremental)
        schemas.validate_value(
            volumes_info, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)

        volumes_info = _check_ensure_volumes_info_ordering(
            export_info, volumes_info)

        return {
            'volumes_info': volumes_info}


class DeployReplicaDisksTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return [
            "export_info", "volumes_info", "target_environment"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["volumes_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_REPLICA_IMPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        target_environment = task_info['target_environment']
        export_info = task_info["export_info"]

        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = task_info.get("volumes_info", [])
        volumes_info = provider.deploy_replica_disks(
            ctxt, connection_info, target_environment, instance, export_info,
            volumes_info)
        schemas.validate_value(
            volumes_info, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)

        volumes_info = _check_ensure_volumes_info_ordering(
            export_info, volumes_info)

        return {
            'volumes_info': volumes_info}


class DeleteReplicaSourceDiskSnapshotsTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_SOURCE

    @classmethod
    def get_required_task_info_properties(cls):
        return [
            "volumes_info", "source_environment"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["volumes_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_SOURCE: [
                constants.PROVIDER_TYPE_REPLICA_EXPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        event_manager = events.EventManager(event_handler)
        if not task_info.get("volumes_info"):
            LOG.debug(
                "No volumes_info present. Skipping source snapshot deletion.")
            event_manager.progress_update(
                "No previous volumes information present, nothing to delete")
            return {'volumes_info': []}

        provider = providers_factory.get_provider(
            origin['type'], constants.PROVIDER_TYPE_REPLICA_EXPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, origin)
        source_environment = task_info['source_environment']
        volumes_info = _get_volumes_info(task_info)

        volumes_info = provider.delete_replica_source_snapshots(
            ctxt, connection_info, source_environment, volumes_info)

        return {
            'volumes_info': volumes_info}


class DeleteReplicaDisksTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return [
            "volumes_info", "target_environment"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["volumes_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_REPLICA_IMPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        event_manager = events.EventManager(event_handler)
        if not task_info.get("volumes_info"):
            LOG.debug(
                "No volumes_info present. Skipping disk deletion.")
            event_manager.progress_update(
                "No previous volumes information present, nothing to delete")
            return {'volumes_info': []}

        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = _get_volumes_info(task_info)
        target_environment = task_info['target_environment']

        volumes_info = provider.delete_replica_disks(
            ctxt, connection_info, target_environment, volumes_info)
        if volumes_info:
            LOG.warn(
                "'volumes_info' should have been void after disk "
                "deletion task but it is: %s" % (
                    utils.sanitize_task_info({
                        'volumes_info': volumes_info})))

        return {
            'volumes_info': []}


class DeployReplicaSourceResourcesTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_SOURCE

    @classmethod
    def get_required_task_info_properties(cls):
        return ["source_environment", "export_info"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["source_resources", "source_resources_connection_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_SOURCE: [
                constants.PROVIDER_TYPE_REPLICA_EXPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_REPLICA_EXPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, origin)

        source_environment = task_info.get('source_environment', {})
        export_info = task_info['export_info']
        replica_resources_info = provider.deploy_replica_source_resources(
            ctxt, connection_info, export_info, source_environment)

        migr_connection_info = replica_resources_info.get(
            "connection_info", {})
        if 'connection_info' not in replica_resources_info:
            LOG.warn(
                "Replica source provider for '%s' did NOT return any "
                "'connection_info'. Defaulting to '%s'",
                origin["type"], migr_connection_info)
        else:
            migr_connection_info = replica_resources_info['connection_info']
            if migr_connection_info:
                migr_connection_info = base.marshal_migr_conn_info(
                    migr_connection_info)
                schemas.validate_value(
                    migr_connection_info,
                    schemas.CORIOLIS_REPLICATION_WORKER_CONN_INFO_SCHEMA,
                    # NOTE: we avoid raising so that the cleanup task
                    # can [try] to deal with the temporary resources.
                    raise_on_error=False)
            else:
                LOG.warn(
                    "Replica source provider for '%s' returned empty "
                    "'connection_info' in source resources deployment: %s",
                    origin["type"], migr_connection_info)

        migr_resources = {}
        if 'migr_resources' not in replica_resources_info:
            LOG.warn(
                "Replica source provider for '%s' did NOT return any "
                "'migr_resources'. Defaulting to %s",
                origin["type"], migr_resources)
        else:
            migr_resources = replica_resources_info['migr_resources']

        return {
            "source_resources": migr_resources,
            "source_resources_connection_info": migr_connection_info}


class DeleteReplicaSourceResourcesTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_SOURCE

    @classmethod
    def get_required_task_info_properties(cls):
        return ["source_environment", "source_resources"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["source_resources", "source_resources_connection_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_SOURCE: [
                constants.PROVIDER_TYPE_REPLICA_EXPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_REPLICA_EXPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, origin)

        migr_resources = task_info["source_resources"]
        source_environment = origin["source_environment"]

        if migr_resources:
            provider.delete_replica_source_resources(
                ctxt, connection_info, source_environment, migr_resources)

        return {
            "source_resources": None,
            "source_resources_connection_info": None}


class DeployReplicaTargetResourcesTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["export_info", "volumes_info", "target_environment"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return [
            "volumes_info", "target_resources",
            "target_resources_connection_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_REPLICA_IMPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        target_environment = task_info["target_environment"]
        export_info = task_info['export_info']

        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = _get_volumes_info(task_info)

        replica_resources_info = provider.deploy_replica_target_resources(
            ctxt, connection_info, target_environment, volumes_info)
        schemas.validate_value(
            replica_resources_info,
            schemas.CORIOLIS_DISK_SYNC_RESOURCES_INFO_SCHEMA,
            # NOTE: we avoid raising so that the cleanup task
            # can [try] to deal with the temporary resources.
            raise_on_error=False)

        if "volumes_info" in replica_resources_info:
            volumes_info = replica_resources_info["volumes_info"]
            volumes_info = _check_ensure_volumes_info_ordering(
                export_info, volumes_info)
        else:
            LOG.warn(
                "Replica target provider for '%s' did not return any "
                "'volumes_info'. Using the previous value of it.")

        migr_connection_info = {}
        if 'connection_info' in replica_resources_info:
            migr_connection_info = replica_resources_info['connection_info']
            try:
                backup_writers.BackupWritersFactory(
                    migr_connection_info, None).get_writer()
            except Exception as err:
                LOG.warn(
                    "Seemingly invalid backup writer conn info. Replica will "
                    "likely fail during disk Replication. Error is: %s" % (
                        str(err)))

            if migr_connection_info:
                if 'connection_details' in migr_connection_info:
                    migr_connection_info['connection_details'] = (
                        base.marshal_migr_conn_info(
                            migr_connection_info['connection_details']))
                schemas.validate_value(
                    migr_connection_info,
                    schemas.CORIOLIS_DISK_SYNC_RESOURCES_CONN_INFO_SCHEMA,
                    # NOTE: we avoid raising so that the cleanup task
                    # can [try] to deal with the temporary resources.
                    raise_on_error=False)
        else:
            LOG.warn(
                "Replica target provider for '%s' did NOT return any "
                "'connection_info'. Defaulting to %s",
                destination["type"], migr_connection_info)

        target_resources = {}
        if 'migr_resources' not in replica_resources_info:
            LOG.warn(
                "Replica target provider for '%s' did NOT return any "
                "'migr_resources'. Defaulting to %s",
                destination["type"], target_resources)
        else:
            target_resources = replica_resources_info["migr_resources"]

        return {
            "volumes_info": volumes_info,
            "target_resources": target_resources,
            "target_resources_connection_info": migr_connection_info}


class DeleteReplicaTargetResourcesTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["target_resources", "target_environment"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return [
            "target_resources", "target_resources_connection_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_REPLICA_IMPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        migr_resources = task_info.get("target_resources")
        target_environment = task_info["target_environment"]

        if migr_resources:
            provider.delete_replica_target_resources(
                ctxt, connection_info, target_environment, migr_resources)

        return {
            "target_resources": None,
            "target_resources_connection_info": None}


class DeployReplicaInstanceResourcesTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return [
            "export_info", "target_environment", "clone_disks", "volumes_info"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["instance_deployment_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_REPLICA_IMPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        target_environment = task_info["target_environment"]
        export_info = task_info["export_info"]

        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = _get_volumes_info(task_info)
        clone_disks = task_info.get("clone_disks", True)
        LOG.debug("Clone disks: %s", clone_disks)

        import_info = provider.deploy_replica_instance(
            ctxt, connection_info, target_environment, instance,
            export_info, volumes_info, clone_disks)

        return {
            "instance_deployment_info": import_info[
                'instance_deployment_info']}


class FinalizeReplicaInstanceDeploymentTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["target_environment", "instance_deployment_info"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["transfer_result"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_REPLICA_IMPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        target_environment = task_info["target_environment"]
        instance_deployment_info = task_info["instance_deployment_info"]

        result = provider.finalize_replica_instance_deployment(
            ctxt, connection_info, target_environment,
            instance_deployment_info)
        if result is None:
            LOG.warn(
                "'None' was returned as result for Finalize Replica Instance "
                "deployment task '%s'.", task_info)

        return {
            "transfer_result": result}


class CleanupFailedReplicaInstanceDeploymentTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["target_environment", "instance_deployment_info"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["instance_deployment_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_REPLICA_IMPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        target_environment = task_info["target_environment"]
        instance_deployment_info = task_info["instance_deployment_info"]

        provider.cleanup_failed_replica_instance_deployment(
            ctxt, connection_info, target_environment,
            instance_deployment_info)

        return {
            "instance_deployment_info": None}


class CreateReplicaDiskSnapshotsTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["target_environment", "export_info", "volumes_info"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["volumes_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_REPLICA_IMPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        export_info = task_info['export_info']
        target_environment = task_info["target_environment"]

        volumes_info = _get_volumes_info(task_info)

        volumes_info = provider.create_replica_disk_snapshots(
            ctxt, connection_info, target_environment, volumes_info)
        schemas.validate_value(
            volumes_info, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)

        volumes_info = _check_ensure_volumes_info_ordering(
            export_info, volumes_info)

        return {
            "volumes_info": volumes_info}


class DeleteReplicaTargetDiskSnapshotsTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["target_environment", "export_info", "volumes_info"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["volumes_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_REPLICA_IMPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        export_info = task_info['export_info']
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = _get_volumes_info(task_info)
        target_environment = task_info["target_environment"]

        volumes_info = provider.delete_replica_target_disk_snapshots(
            ctxt, connection_info, target_environment, volumes_info)
        schemas.validate_value(
            volumes_info, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)

        volumes_info = _check_ensure_volumes_info_ordering(
            export_info, volumes_info)

        return {
            "volumes_info": volumes_info}


class RestoreReplicaDiskSnapshotsTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["target_environment", "export_info", "volumes_info"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["volumes_info"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_REPLICA_IMPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        export_info = task_info['export_info']
        target_environment = task_info["target_environment"]

        volumes_info = _get_volumes_info(task_info)

        volumes_info = provider.restore_replica_disk_snapshots(
            ctxt, connection_info, target_environment, volumes_info)
        schemas.validate_value(
            volumes_info, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)

        volumes_info = _check_ensure_volumes_info_ordering(
            export_info, volumes_info)

        return {
            "volumes_info": volumes_info}


class ValidateReplicaExecutionSourceInputsTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_SOURCE

    @classmethod
    def get_required_task_info_properties(cls):
        return ["source_environment"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return []

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_SOURCE: [
                constants.PROVIDER_TYPE_VALIDATE_REPLICA_EXPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        event_manager = events.EventManager(event_handler)
        origin_type = origin["type"]
        source_provider = providers_factory.get_provider(
            origin_type, constants.PROVIDER_TYPE_VALIDATE_REPLICA_EXPORT,
            event_handler, raise_if_not_found=False)
        origin_connection_info = base.get_connection_info(ctxt, origin)
        if not source_provider:
            event_manager.progress_update(
                "Replica Export Provider for platform '%s' does not support "
                "Replica input validation" % origin_type)
        else:
            source_provider.validate_replica_export_input(
                ctxt, origin_connection_info, instance,
                source_environment=task_info["source_environment"])

        return {}


class ValidateReplicaExecutionDestinationInputsTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["export_info", "target_environment"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return []

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_VALIDATE_REPLICA_IMPORT]
        }

    def _validate_provider_replica_import_input(
            self, provider, ctxt, conn_info, target_environment, export_info):
        provider.validate_replica_import_input(
            ctxt, conn_info, target_environment, export_info,
            check_os_morphing_resources=False,
            check_final_vm_params=False)

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        event_manager = events.EventManager(event_handler)
        destination_type = destination["type"]

        destination_connection_info = base.get_connection_info(
            ctxt, destination)
        destination_provider = providers_factory.get_provider(
            destination_type,
            constants.PROVIDER_TYPE_VALIDATE_REPLICA_IMPORT, event_handler,
            raise_if_not_found=False)
        if not destination_provider:
            event_manager.progress_update(
                "Replica Import Provider for platform '%s' does not support "
                "Replica input validation" % destination_type)
            return {}

        export_info = task_info.get("export_info")
        if not export_info:
            raise exception.InvalidActionTasksExecutionState(
                "Instance export info is not set. Cannot perform "
                "Replica Import validation for destination platform "
                "'%s'" % destination_type)

        target_environment = task_info["target_environment"]
        self._validate_provider_replica_import_input(
            destination_provider, ctxt, destination_connection_info,
            target_environment, export_info)

        return {}


class ValidateReplicaDeploymentParametersTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["export_info", "target_environment"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return []

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_VALIDATE_REPLICA_IMPORT]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        event_manager = events.EventManager(event_handler)
        destination_connection_info = base.get_connection_info(
            ctxt, destination)
        destination_type = destination["type"]
        export_info = task_info["export_info"]
        # validate Export info:
        schemas.validate_value(
            export_info, schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA)

        # validate destination params:
        destination_provider = providers_factory.get_provider(
            destination_type,
            constants.PROVIDER_TYPE_VALIDATE_REPLICA_IMPORT, event_handler,
            raise_if_not_found=False)
        if not destination_provider:
            event_manager.progress_update(
                "Replica Deployment Provider for platform '%s' does not "
                "support Replica Deployment input validation" % (
                    destination_type))
            return {}

        # NOTE: the target environment JSON schema should have been validated
        # upon accepting the Replica API creation request.
        target_environment = task_info['target_environment']
        destination_provider.validate_replica_deployment_input(
            ctxt, destination_connection_info, target_environment, export_info)

        return {}


class UpdateSourceReplicaTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_SOURCE

    @classmethod
    def get_required_task_info_properties(cls):
        return ["volumes_info", "source_environment"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["volumes_info", "source_environment"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_SOURCE: [
                constants.PROVIDER_TYPE_SOURCE_REPLICA_UPDATE]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        event_manager = events.EventManager(event_handler)

        volumes_info = task_info.get("volumes_info", [])
        new_source_env = task_info.get('source_environment', {})
        # NOTE: the `source_environment` in the `origin` is the one set
        # in the dedicated DB column of the Replica and thus stores
        # the previous value of it:
        old_source_env = origin.get('source_environment')
        if not new_source_env:
            event_manager.progress_update(
                "No new source environment options provided")
            return {
                'volumes_info': volumes_info,
                'source_environment': old_source_env}

        source_provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_SOURCE_REPLICA_UPDATE,
            event_handler, raise_if_not_found=False)
        if not source_provider:
            raise exception.InvalidActionTasksExecutionState(
                "Replica source provider plugin for '%s' does not support"
                " updating Replicas" % origin["type"])

        origin_connection_info = base.get_connection_info(ctxt, origin)

        LOG.info("Checking source provider environment params")
        volumes_info = (
            source_provider.check_update_source_environment_params(
                ctxt, origin_connection_info, instance, volumes_info,
                old_source_env, new_source_env))
        if volumes_info:
            schemas.validate_value(
                volumes_info, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)
        else:
            LOG.warn(
                "Source update method for '%s' source provider did NOT "
                "return any volumes info. Defaulting to old value.",
                origin["type"])
            volumes_info = task_info.get("volumes_info", [])

        return {
            "volumes_info": volumes_info,
            "source_environment": new_source_env}


class UpdateDestinationReplicaTask(base.TaskRunner):

    @classmethod
    def get_required_platform(cls):
        return constants.TASK_PLATFORM_DESTINATION

    @classmethod
    def get_required_task_info_properties(cls):
        return ["export_info", "volumes_info", "target_environment"]

    @classmethod
    def get_returned_task_info_properties(cls):
        return ["volumes_info", "target_environment"]

    @classmethod
    def get_required_provider_types(cls):
        return {
            constants.PROVIDER_PLATFORM_DESTINATION: [
                constants.PROVIDER_TYPE_DESTINATION_REPLICA_UPDATE]
        }

    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        event_manager = events.EventManager(event_handler)

        volumes_info = task_info.get("volumes_info", [])
        new_destination_env = task_info.get('target_environment', {})
        # NOTE: the `target_environment` in the `destination` is the one
        # set in the dedicated DB column of the Replica and thus stores
        # the previous value of it:
        old_destination_env = destination.get('target_environment', {})
        if not new_destination_env:
            event_manager.progress_update(
                "No new destination environment options provided")
            return {
                "target_environment": old_destination_env,
                "volumes_info": volumes_info}

        destination_provider = providers_factory.get_provider(
            destination["type"],
            constants.PROVIDER_TYPE_DESTINATION_REPLICA_UPDATE,
            event_handler, raise_if_not_found=False)
        if not destination_provider:
            raise exception.InvalidActionTasksExecutionState(
                "Replica destination provider plugin for '%s' does not "
                "support updating Replicas" % destination["type"])

        destination_connection_info = base.get_connection_info(
            ctxt, destination)
        export_info = task_info.get("export_info", {})

        LOG.info("Checking destination provider environment params")
        volumes_info = (
            destination_provider.check_update_destination_environment_params(
                ctxt, destination_connection_info, export_info, volumes_info,
                old_destination_env, new_destination_env))

        if volumes_info:
            schemas.validate_value(
                volumes_info, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)
            volumes_info = _check_ensure_volumes_info_ordering(
                export_info, volumes_info)
        else:
            LOG.warn(
                "Destination update method for '%s' dest provider did NOT "
                "return any volumes info. Defaulting to old value.",
                destination["type"])
            volumes_info = task_info.get("volumes_info", [])

        return {
            "volumes_info": volumes_info,
            "target_environment": new_destination_env}
