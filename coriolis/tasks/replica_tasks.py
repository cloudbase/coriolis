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

        volumes_info = _get_volumes_info(task_info)

        migr_source_conn_info = base.unmarshal_migr_conn_info(
            task_info["migr_source_connection_info"])

        migr_target_conn_info = base.unmarshal_migr_conn_info(
            task_info["migr_target_connection_info"])

        incremental = task_info.get("incremental", True)

        source_environment = origin.get('source_environment') or {}

        volumes_info = provider.replicate_disks(
            ctxt, connection_info, source_environment, instance,
            migr_source_conn_info, migr_target_conn_info, volumes_info,
            incremental)

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

        source_environment = origin.get('source_environment') or {}
        replica_resources_info = provider.deploy_replica_source_resources(
            ctxt, connection_info, source_environment)

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
        destination_provider.validate_replica_import_input(
            ctxt, destination_connection_info, target_environment,
            export_info)

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


class UpdateReplicaTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        destination_provider = None
        source_provider = None
        new_source_environment = task_info.get('source_environment')
        new_destination_environment = task_info.get('destination_environment')

        if new_source_environment:
            source_provider = providers_factory.get_provider(
                origin["type"], constants.PROVIDER_TYPE_REPLICA_UPDATE,
                event_handler, raise_if_not_found=False)
            if not source_provider:
                raise exception.CoriolisException(
                    "Replica source provider plugin for '%s' does not support"
                    " updating Replicas." % origin["type"])

        if new_destination_environment:
            destination_provider = providers_factory.get_provider(
                destination["type"], constants.PROVIDER_TYPE_REPLICA_UPDATE,
                event_handler, raise_if_not_found=False)
            if not destination_provider:
                raise exception.CoriolisException(
                    "Replica destination provider plugin for '%s' does not "
                    "support updating Replicas." % destination["type"])

        origin_connection_info = base.get_connection_info(ctxt, origin)
        destination_connection_info = base.get_connection_info(
            ctxt, destination)
        export_info = task_info.get("export_info", {})
        volumes_info = task_info.get("volumes_info", {})

        if source_provider:
            LOG.info("Checking source provider environment params")
            # NOTE: the `source_environment` in the `origin` is the one set
            # in the dedicated DB column of the Replica and thus stores
            # the previous value of it:
            old_source_environment = origin.get('source_environment', {})
            new_source_environment = task_info.get('source_environment', {})
            source_provider.check_update_environment_params(
                ctxt, origin_connection_info, export_info, volumes_info,
                old_source_environment, new_source_environment)

        if destination_provider:
            LOG.info("Checking destination provider environment params")
            # NOTE: the `target_environment` in the `destination` is the one
            # set in the dedicated DB column of the Replica and thus stores
            # the previous value of it:
            old_destination_environment = destination.get(
                'target_environment', {})
            new_destination_environment = task_info.get(
                'destination_environment', {})

            volumes_info = (
                destination_provider.check_update_environment_params(
                    ctxt, destination_connection_info, export_info,
                    volumes_info, old_destination_environment,
                    new_destination_environment))

            task_info['volumes_info'] = volumes_info

        return task_info
