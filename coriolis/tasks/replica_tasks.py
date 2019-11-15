# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis import constants
from coriolis import events
from coriolis import exception
from coriolis.providers import factory as providers_factory
from coriolis import schemas
from coriolis.tasks import base

LOG = logging.getLogger(__name__)


def _get_volumes_info(task_info):
    volumes_info = task_info.get("volumes_info")
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
    ordered_volumes_info = []
    for disk in export_info['devices']['disks']:
        disk_id = disk['id']
        matching_volumes = [
            vol for vol in volumes_info if vol['disk_id'] == disk_id]
        if not matching_volumes:
            raise exception.CoriolisException(
                "Could not find source disk '%s' (ID '%s') in Replica "
                "volumes info: %s" % (disk, disk_id, volumes_info))
        elif len(matching_volumes) > 1:
            raise exception.CoriolisException(
                "Multiple disks with ID '%s' foind in Replica "
                "volumes info: %s" % (disk_id, volumes_info))

        ordered_volumes_info.append(matching_volumes[0])

    LOG.debug(
        "volumes_info returned by provider for instance "
        "'%s': %s", instance, volumes_info)
    LOG.debug(
        "volumes_info for instance '%s' after "
        "reordering: %s", instance, ordered_volumes_info)

    return ordered_volumes_info


class GetInstanceInfoTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_REPLICA_EXPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, origin)

        source_environment = origin.get('source_environment') or {}
        export_info = provider.get_replica_instance_info(
            ctxt, connection_info, source_environment, instance)

        # Validate the output
        schemas.validate_value(
            export_info, schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA)
        task_info["export_info"] = export_info

        return task_info


class ShutdownInstanceTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_REPLICA_EXPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, origin)

        source_environment = origin.get('source_environment') or {}
        provider.shutdown_instance(ctxt, connection_info, source_environment,
                                   instance)

        return task_info


class ReplicateDisksTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
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

        migr_source_conn_info = task_info["migr_source_connection_info"]
        if migr_source_conn_info:
            schemas.validate_value(
                migr_source_conn_info,
                schemas.CORIOLIS_DISK_SYNC_RESOURCES_CONN_INFO_SCHEMA)
        migr_source_conn_info = base.unmarshal_migr_conn_info(
            migr_source_conn_info)

        migr_target_conn_info = task_info["migr_target_connection_info"]
        schemas.validate_value(
            migr_target_conn_info,
            schemas.CORIOLIS_DISK_SYNC_RESOURCES_CONN_INFO_SCHEMA)
        migr_target_conn_info = base.unmarshal_migr_conn_info(
            migr_target_conn_info)

        incremental = task_info.get("incremental", True)

        source_environment = origin.get('source_environment') or {}

        volumes_info = provider.replicate_disks(
            ctxt, connection_info, source_environment, instance,
            migr_source_conn_info, migr_target_conn_info, volumes_info,
            incremental)
        schemas.validate_value(
            volumes_info, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)

        volumes_info = _check_ensure_volumes_info_ordering(
            export_info, volumes_info)

        task_info["volumes_info"] = volumes_info

        return task_info


class DeployReplicaDisksTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        target_environment = destination.get("target_environment") or {}
        export_info = task_info["export_info"]

        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = task_info.get("volumes_info", [])
        if volumes_info is None:
            # In case Replica disks were deleted:
            volumes_info = []

        volumes_info = provider.deploy_replica_disks(
            ctxt, connection_info, target_environment, instance, export_info,
            volumes_info)
        schemas.validate_value(
            volumes_info, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)

        volumes_info = _check_ensure_volumes_info_ordering(
            export_info, volumes_info)

        task_info["volumes_info"] = volumes_info

        return task_info


class DeleteReplicaDisksTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        if not task_info.get("volumes_info"):
            LOG.debug(
                "No volumes_info present. Skipping disk deletion.")
            return task_info

        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = _get_volumes_info(task_info)

        volumes_info = provider.delete_replica_disks(
            ctxt, connection_info, volumes_info)
        if volumes_info:
            LOG.warn(
                "'volumes_info' should have been void after disk "
                "deletion: %s" % volumes_info)

        task_info["volumes_info"] = []

        return task_info


class DeployReplicaSourceResourcesTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_REPLICA_EXPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, origin)

        source_environment = origin.get('source_environment') or {}
        replica_resources_info = provider.deploy_replica_source_resources(
            ctxt, connection_info, source_environment)

        task_info["migr_source_resources"] = replica_resources_info[
            "migr_resources"]
        migr_connection_info = replica_resources_info.get("connection_info")
        if migr_connection_info:
            migr_connection_info = base.marshal_migr_conn_info(
                migr_connection_info)
            schemas.validate_value(
                migr_connection_info,
                schemas.CORIOLIS_DISK_SYNC_RESOURCES_CONN_INFO_SCHEMA,
                # NOTE: we avoid raising so that the cleanup task
                # can [try] to deal with the temporary resources.
                raise_on_error=False)

        task_info["migr_source_connection_info"] = migr_connection_info

        return task_info


class DeleteReplicaSourceResourcesTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_REPLICA_EXPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, origin)

        migr_resources = task_info.get("migr_source_resources")

        source_environment = origin.get("source_environment", {})

        if migr_resources:
            provider.delete_replica_source_resources(
                ctxt, connection_info, source_environment, migr_resources)

        task_info["migr_source_resources"] = None
        task_info["migr_source_connection_info"] = None

        return task_info


class DeployReplicaTargetResourcesTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        target_environment = destination.get("target_environment") or {}
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

        volumes_info = _check_ensure_volumes_info_ordering(
            export_info, replica_resources_info["volumes_info"])

        task_info["volumes_info"] = volumes_info
        task_info["migr_target_resources"] = replica_resources_info[
            "migr_resources"]

        migr_connection_info = replica_resources_info["connection_info"]
        migr_connection_info = base.marshal_migr_conn_info(
            migr_connection_info)
        schemas.validate_value(
            migr_connection_info,
            schemas.CORIOLIS_DISK_SYNC_RESOURCES_CONN_INFO_SCHEMA,
            # NOTE: we avoid raising so that the cleanup task
            # can [try] to deal with the temporary resources.
            raise_on_error=False)

        task_info["migr_target_connection_info"] = migr_connection_info

        return task_info


class DeleteReplicaTargetResourcesTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        migr_resources = task_info.get("migr_target_resources")

        if migr_resources:
            provider.delete_replica_target_resources(
                ctxt, connection_info, migr_resources)

        task_info["migr_target_resources"] = None
        task_info["migr_target_connection_info"] = None

        return task_info


class DeployReplicaInstanceTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        target_environment = destination.get("target_environment") or {}
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

        if task_info.get("instance_deployment_info") is None:
            task_info["instance_deployment_info"] = {}
        task_info["instance_deployment_info"].update(import_info[
            "instance_deployment_info"])

        task_info[
            "origin_provider_type"] = constants.PROVIDER_TYPE_REPLICA_EXPORT
        task_info[
            "destination_provider_type"
        ] = constants.PROVIDER_TYPE_REPLICA_IMPORT

        return task_info


class FinalizeReplicaInstanceDeploymentTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        instance_deployment_info = task_info["instance_deployment_info"]

        result = provider.finalize_replica_instance_deployment(
            ctxt, connection_info, instance_deployment_info)
        if result is not None:
            task_info["transfer_result"] = result
        else:
            LOG.warn(
                "'None' was returned as result for Finalize Replica Instance "
                "deployment task '%s'.", task_info)

        return task_info


class CleanupFailedReplicaInstanceDeploymentTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        instance_deployment_info = task_info.get(
            "instance_deployment_info", {})

        provider.cleanup_failed_replica_instance_deployment(
            ctxt, connection_info, instance_deployment_info)

        return task_info


class CreateReplicaDiskSnapshotsTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        export_info = task_info['export_info']

        volumes_info = _get_volumes_info(task_info)

        volumes_info = provider.create_replica_disk_snapshots(
            ctxt, connection_info, volumes_info)
        schemas.validate_value(
            volumes_info, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)

        volumes_info = _check_ensure_volumes_info_ordering(
            export_info, volumes_info)

        task_info["volumes_info"] = volumes_info

        return task_info


class DeleteReplicaDiskSnapshotsTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        export_info = task_info['export_info']
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = _get_volumes_info(task_info)

        volumes_info = provider.delete_replica_disk_snapshots(
            ctxt, connection_info, volumes_info)
        schemas.validate_value(
            volumes_info, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)

        volumes_info = _check_ensure_volumes_info_ordering(
            export_info, volumes_info)

        task_info["volumes_info"] = volumes_info

        return task_info


class RestoreReplicaDiskSnapshotsTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        export_info = task_info['export_info']

        volumes_info = _get_volumes_info(task_info)

        volumes_info = provider.restore_replica_disk_snapshots(
            ctxt, connection_info, volumes_info)
        schemas.validate_value(
            volumes_info, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)

        volumes_info = _check_ensure_volumes_info_ordering(
            export_info, volumes_info)

        task_info["volumes_info"] = volumes_info

        return task_info


class ValidateReplicaExecutionSourceInputsTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
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
                source_environment=origin.get("source_environment", {}))

        return task_info


class ValidateReplicaExecutionDestinationInputsTask(base.TaskRunner):
    def _validate_provider_replica_import_input(
            self, provider, ctxt, conn_info, target_environment, export_info):
        provider.validate_replica_import_input(
            ctxt, conn_info, target_environment, export_info,
            check_os_morphing_resources=False,
            check_final_vm_params=False)

    def run(self, ctxt, instance, origin, destination, task_info,
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
            return task_info

        export_info = task_info.get("export_info")
        if not export_info:
            raise exception.CoriolisException(
                "Instance export info is not set. Cannot perform "
                "Replica Import validation for destination platform "
                "'%s'" % destination_type)

        # NOTE: the target environment JSON schema should have been validated
        # upon accepting the Replica API creation request.
        target_environment = destination.get("target_environment", {})
        self._validate_provider_replica_import_input(
            destination_provider, ctxt, destination_connection_info,
            target_environment, export_info)

        return task_info


class ValidateReplicaDeploymentParametersTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
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
            return task_info

        # NOTE: the target environment JSON schema should have been validated
        # upon accepting the Replica API creation request.
        target_environment = destination.get("target_environment", {})
        destination_provider.validate_replica_deployment_input(
            ctxt, destination_connection_info, target_environment, export_info)

        return task_info


class UpdateSourceReplicaTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        event_manager = events.EventManager(event_handler)
        new_source_env = task_info.get('source_environment', {})
        if not new_source_env:
            event_manager.progress_update(
                "No new source environment options provided")
            return task_info

        source_provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_SOURCE_REPLICA_UPDATE,
            event_handler, raise_if_not_found=False)
        if not source_provider:
            raise exception.CoriolisException(
                "Replica source provider plugin for '%s' does not support"
                " updating Replicas" % origin["type"])

        origin_connection_info = base.get_connection_info(ctxt, origin)
        volumes_info = task_info.get("volumes_info", [])

        LOG.info("Checking source provider environment params")
        # NOTE: the `source_environment` in the `origin` is the one set
        # in the dedicated DB column of the Replica and thus stores
        # the previous value of it:
        old_source_env = origin.get('source_environment', {})
        volumes_info = (
            source_provider.check_update_source_environment_params(
                ctxt, origin_connection_info, instance, volumes_info,
                old_source_env, new_source_env))
        if volumes_info:
            schemas.validate_value(
                volumes_info, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)

        task_info['volumes_info'] = volumes_info

        return task_info


class UpdateDestinationReplicaTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        event_manager = events.EventManager(event_handler)
        new_destination_env = task_info.get('destination_environment', {})
        if not new_destination_env:
            event_manager.progress_update(
                "No new destination environment options provided")
            return task_info

        destination_provider = providers_factory.get_provider(
            destination["type"],
            constants.PROVIDER_TYPE_DESTINATION_REPLICA_UPDATE,
            event_handler, raise_if_not_found=False)
        if not destination_provider:
            raise exception.CoriolisException(
                "Replica destination provider plugin for '%s' does not "
                "support updating Replicas" % destination["type"])

        destination_connection_info = base.get_connection_info(
            ctxt, destination)
        export_info = task_info.get("export_info", {})
        volumes_info = task_info.get("volumes_info", [])

        LOG.info("Checking destination provider environment params")
        # NOTE: the `target_environment` in the `destination` is the one
        # set in the dedicated DB column of the Replica and thus stores
        # the previous value of it:
        old_destination_env = destination.get('target_environment', {})
        volumes_info = (
            destination_provider.check_update_destination_environment_params(
                ctxt, destination_connection_info, export_info, volumes_info,
                old_destination_env, new_destination_env))

        if volumes_info:
            schemas.validate_value(
                volumes_info, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)
            volumes_info = _check_ensure_volumes_info_ordering(
                export_info, volumes_info)

        task_info['volumes_info'] = volumes_info

        return task_info
