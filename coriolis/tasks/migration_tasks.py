# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis import events
from coriolis.providers import factory as providers_factory
from coriolis import schemas
from coriolis import exception
from coriolis.migrations import manager
from coriolis.tasks import base

from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class ExportInstanceTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_EXPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, origin)
        export_path = task_info["export_path"]

        export_info = provider.export_instance(
            ctxt, connection_info, instance, export_path)

        # Validate the output
        schemas.validate_value(
            export_info, schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA)
        task_info["export_info"] = export_info
        task_info["retain_export_path"] = True

        return task_info


class ImportInstanceTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        target_environment = destination.get("target_environment") or {}
        export_info = task_info["export_info"]

        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        import_info = provider.import_instance(
            ctxt, connection_info, target_environment, instance, export_info)

        if task_info.get("instance_deployment_info") is None:
            task_info["instance_deployment_info"] = {}
        task_info["instance_deployment_info"].update(import_info[
            "instance_deployment_info"])

        task_info["origin_provider_type"] = constants.PROVIDER_TYPE_EXPORT
        task_info["destination_provider_type"] = constants.PROVIDER_TYPE_IMPORT
        # We need to retain export info until after disk sync
        # TODO (gsamfira): remove this when we implement multi-worker, and by
        # extension some external storage for needed resources (like swift)
        task_info["retain_export_path"] = True

        return task_info


class DeployDiskCopyResources(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        target_environment = destination.get("target_environment") or {}
        instance_deployment_info = task_info["instance_deployment_info"]

        resources_info = provider.deploy_disk_copy_resources(
            ctxt, connection_info, target_environment,
            instance_deployment_info)

        conn_info = resources_info[
            "instance_deployment_info"]["disk_sync_connection_info"]
        conn_info = base.marshal_migr_conn_info(conn_info)
        task_info["instance_deployment_info"] = resources_info[
            "instance_deployment_info"]
        task_info["instance_deployment_info"][
            "disk_sync_connection_info"] = conn_info
        # We need to retain export info until after disk sync
        # TODO (gsamfira): remove this when we implement multi-worker, and by
        # extension some external storage for needed resources (like swift)
        task_info["retain_export_path"] = True

        return task_info


class CopyDiskData(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        instance_deployment_info = task_info["instance_deployment_info"]
        volumes_info = instance_deployment_info["volumes_info"]
        LOG.info("Volumes info is: %r" % volumes_info)

        image_paths = [i.get("disk_image_uri") for i in volumes_info]
        if None in image_paths:
            raise exception.InvalidActionTasksExecutionState(
                "disk_image_uri must be part of volumes_info for"
                " standard migrations")

        target_conn_info = base.unmarshal_migr_conn_info(
            instance_deployment_info["disk_sync_connection_info"])
        manager.copy_disk_data(
            target_conn_info, volumes_info, event_handler)

        return task_info


class DeleteDiskCopyResources(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        instance_deployment_info = task_info.get(
            "instance_deployment_info", {})
        provider.delete_disk_copy_resources(
            ctxt, connection_info, instance_deployment_info)

        if instance_deployment_info.get("disk_sync_connection_info"):
            del instance_deployment_info["disk_sync_connection_info"]
        if instance_deployment_info.get("disk_sync_tgt_resources"):
            del instance_deployment_info["disk_sync_tgt_resources"]

        return task_info


class FinalizeImportInstanceTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        instance_deployment_info = task_info["instance_deployment_info"]

        provider.finalize_import_instance(
            ctxt, connection_info, instance_deployment_info)

        return task_info


class CleanupFailedImportInstanceTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, destination)
        instance_deployment_info = task_info.get(
            "instance_deployment_info", {})

        provider.cleanup_failed_import_instance(
            ctxt, connection_info, instance_deployment_info)

        return task_info


class GetOptimalFlavorTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_INSTANCE_FLAVOR,
            event_handler)

        connection_info = base.get_connection_info(ctxt, destination)
        target_environment = destination.get("target_environment") or {}
        export_info = task_info["export_info"]

        flavor = provider.get_optimal_flavor(
            ctxt, connection_info, target_environment, export_info)

        if task_info.get("instance_deployment_info") is None:
            task_info["instance_deployment_info"] = {}
        task_info["instance_deployment_info"]["selected_flavor"] = flavor

        events.EventManager(event_handler).progress_update(
            "Selected flavor: %s" % flavor)

        task_info["retain_export_path"] = True

        return task_info
