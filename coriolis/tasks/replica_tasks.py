# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis import exception
from coriolis.providers import factory as providers_factory
from coriolis import schemas
from coriolis.tasks import base

from oslo_log import log as logging

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

        task_info["instance_deployment_info"] = import_info[
            "instance_deployment_info"]
        task_info["osmorphing_info"] = import_info.get("osmorphing_info", {})
        task_info["osmorphing_connection_info"] = base.marshal_migr_conn_info(
            import_info["osmorphing_connection_info"])

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

        provider.finalize_replica_instance_deployment(
            ctxt, connection_info, instance_deployment_info)

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
