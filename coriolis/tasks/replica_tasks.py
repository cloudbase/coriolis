# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis.providers import factory as providers_factory
from coriolis import schemas
from coriolis.tasks import base
from coriolis import utils

from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class GetInstanceInfoTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_EXPORT, event_handler)
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
            origin["type"], constants.PROVIDER_TYPE_EXPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, origin)

        provider.shutdown_instance(ctxt, connection_info, instance)

        return task_info


class ReplicateDisksTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_EXPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, origin)

        volumes_info = task_info["volumes_info"]

        migr_conn_info = task_info["migr_connection_info"]
        pkey_str = migr_conn_info["pkey"]
        migr_conn_info["pkey"] = utils.deserialize_key(pkey_str)

        incremental = task_info.get("incremental", True)

        volumes_info = provider.replicate_disks(
            ctxt, connection_info, instance, migr_conn_info,
            volumes_info, incremental)

        task_info["migr_connection_info"] = pkey_str
        task_info["volumes_info"] = volumes_info

        return task_info


class DeployReplicaDisksTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        target_environment = destination.get("target_environment") or {}
        export_info = task_info["export_info"]

        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = task_info.get("volumes_info", [])

        volumes_info = provider.deploy_replica_disks(
            ctxt, connection_info, target_environment, instance, export_info,
            volumes_info)

        task_info["volumes_info"] = volumes_info

        return task_info


class DeleteReplicaDisksTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = task_info["volumes_info"]

        provider.delete_replica_disks(
            ctxt, connection_info, volumes_info)

        del task_info["volumes_info"]

        return task_info


class DeployReplicaResourcesTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        target_environment = destination.get("target_environment") or {}

        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = task_info["volumes_info"]

        replica_resources_info = provider.deploy_replica_resources(
            ctxt, connection_info, target_environment, volumes_info)

        task_info["volumes_info"] = replica_resources_info["volumes_info"]
        task_info["migr_resources"] = replica_resources_info["migr_resources"]

        migr_connection_info = replica_resources_info["connection_info"]
        migr_connection_info["pkey"] = utils.serialize_key(
            migr_connection_info["pkey"])
        task_info["migr_connection_info"] = migr_connection_info

        return task_info


class DeleteReplicaResourcesTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        migr_resources = task_info["migr_resources"]

        provider.delete_replica_resources(
            ctxt, connection_info, migr_resources)

        del task_info["migr_resources"]
        del task_info["migr_connection_info"]

        return task_info


class DeployReplicaInstanceTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        target_environment = destination.get("target_environment") or {}
        export_info = task_info["export_info"]

        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = task_info["volumes_info"]

        provider.deploy_replica_instance(
            ctxt, connection_info, target_environment, instance,
            export_info, volumes_info)

        return task_info


class CreateReplicaDiskSnapshotsTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = task_info["volumes_info"]

        volumes_info = provider.create_replica_disk_snapshots(
            ctxt, connection_info, volumes_info)

        task_info["volumes_info"] = volumes_info

        return task_info


class DeleteReplicaDiskSnapshotsTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = task_info["volumes_info"]

        volumes_info = provider.delete_replica_disk_snapshots(
            ctxt, connection_info, volumes_info)

        task_info["volumes_info"] = volumes_info

        return task_info
