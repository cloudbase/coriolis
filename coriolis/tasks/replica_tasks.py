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


class GetInstanceInfoTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_REPLICA_EXPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, origin)

        export_info = provider.get_replica_instance_info(
            ctxt, connection_info, instance)

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

        provider.shutdown_instance(ctxt, connection_info, instance)

        return task_info


class ReplicateDisksTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_REPLICA_EXPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, origin)

        volumes_info = _get_volumes_info(task_info)

        migr_source_conn_info = base.unmarshal_migr_conn_info(
            task_info["migr_source_connection_info"])

        migr_target_conn_info = base.unmarshal_migr_conn_info(
            task_info["migr_target_connection_info"])

        incremental = task_info.get("incremental", True)

        volumes_info = provider.replicate_disks(
            ctxt, connection_info, instance, migr_source_conn_info,
            migr_target_conn_info, volumes_info, incremental)

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

        volumes_info = task_info.get("volumes_info") or []

        volumes_info = provider.deploy_replica_disks(
            ctxt, connection_info, target_environment, instance, export_info,
            volumes_info)

        task_info["volumes_info"] = volumes_info

        return task_info


class DeleteReplicaDisksTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = _get_volumes_info(task_info)

        provider.delete_replica_disks(
            ctxt, connection_info, volumes_info)

        task_info["volumes_info"] = None

        return task_info


class DeployReplicaSourceResourcesTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_REPLICA_EXPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, origin)

        replica_resources_info = provider.deploy_replica_source_resources(
            ctxt, connection_info)

        task_info["migr_source_resources"] = replica_resources_info[
            "migr_resources"]
        migr_connection_info = base.marshal_migr_conn_info(
            replica_resources_info["connection_info"])
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

        if migr_resources:
            provider.delete_replica_source_resources(
                ctxt, connection_info, migr_resources)

        task_info["migr_source_resources"] = None
        task_info["migr_source_connection_info"] = None

        return task_info


class DeployReplicaTargetResourcesTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        target_environment = destination.get("target_environment") or {}

        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = _get_volumes_info(task_info)

        replica_resources_info = provider.deploy_replica_target_resources(
            ctxt, connection_info, target_environment, volumes_info)

        task_info["volumes_info"] = replica_resources_info["volumes_info"]
        task_info["migr_target_resources"] = replica_resources_info[
            "migr_resources"]

        migr_connection_info = base.marshal_migr_conn_info(
            replica_resources_info["connection_info"])
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

        volumes_info = _get_volumes_info(task_info)

        volumes_info = provider.create_replica_disk_snapshots(
            ctxt, connection_info, volumes_info)

        task_info["volumes_info"] = volumes_info

        return task_info


class DeleteReplicaDiskSnapshotsTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = _get_volumes_info(task_info)

        volumes_info = provider.delete_replica_disk_snapshots(
            ctxt, connection_info, volumes_info)

        task_info["volumes_info"] = volumes_info

        return task_info


class RestoreReplicaDiskSnapshotsTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_REPLICA_IMPORT,
            event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = _get_volumes_info(task_info)

        volumes_info = provider.restore_replica_disk_snapshots(
            ctxt, connection_info, volumes_info)

        task_info["volumes_info"] = volumes_info

        return task_info


class ValidateReplicaExecutionParametersTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        event_manager = events.EventManager(event_handler)
        # validate source params:
        origin_type = origin["type"]
        origin_connection_info = base.get_connection_info(ctxt, origin)
        destination_connection_info = base.get_connection_info(
            ctxt, destination)
        destination_type = destination["type"]
        source_provider = providers_factory.get_provider(
            origin_type, constants.PROVIDER_TYPE_VALIDATE_REPLICA_EXPORT,
            event_handler, raise_if_not_found=False)
        export_info = None
        if source_provider:
            export_info = source_provider.validate_replica_export_input(
                ctxt, base.get_connection_info(ctxt, origin), instance,
                source_environment=origin.get("source_environment", {}))
        else:
            event_manager.progress_update(
                "Replica Export Provider for platform '%s' does not support "
                "Replica input validation" % origin_type)

        if export_info is None:
            source_endpoint_provider = providers_factory.get_provider(
                origin_type, constants.PROVIDER_TYPE_ENDPOINT_INSTANCES,
                event_handler, raise_if_not_found=False)
            if not source_endpoint_provider:
                event_manager.progress_update(
                    "Replica Export Provider for platform '%s' does not "
                    "support querying instance export info. Cannot perform "
                    "Replica Import validation for destination platform "
                    "'%s'" % (origin_type, destination_type))
                return task_info
            export_info = source_endpoint_provider.get_instance(
                ctxt, origin_connection_info, instance)

        # validate Export info:
        schemas.validate_value(
            export_info, schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA)
        # NOTE: this export info will get overriden with updated values
        # and disk paths after the ExportInstanceTask.
        task_info["export_info"] = export_info

        # validate destination params:
        destination_provider = providers_factory.get_provider(
            destination_type,
            constants.PROVIDER_TYPE_VALIDATE_REPLICA_IMPORT, event_handler,
            raise_if_not_found=False)
        if not destination_provider:
            event_manager.progress_update(
                "Replica Import Provider for platform '%s' does not support "
                "Replica input validation" % destination_type)
            return task_info

        # NOTE: the target environment JSON schema should have been validated
        # upon accepting the Replica API creation request.
        target_environment = destination.get("target_environment", {})
        destination_provider.validate_replica_import_input(
            ctxt, destination_connection_info, target_environment, export_info)

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
