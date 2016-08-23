# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis.providers import factory as providers_factory
from coriolis import schemas
from coriolis.tasks import base
from coriolis import utils

from oslo_config import cfg
from oslo_log import log as logging

serialization_opts = [
    cfg.StrOpt('temp_keypair_password',
               default=None,
               help='Password to be used when serializing temporary keys'),
]

CONF = cfg.CONF
CONF.register_opts(serialization_opts, 'serialization')

LOG = logging.getLogger(__name__)


def _marshal_migr_conn_info(migr_connection_info):
    if migr_connection_info and "pkey" in migr_connection_info:
        migr_connection_info = migr_connection_info.copy()
        migr_connection_info["pkey"] = utils.serialize_key(
            migr_connection_info["pkey"],
            CONF.serialization.temp_keypair_password)
    return migr_connection_info


def _unmarshal_migr_conn_info(migr_connection_info):
    if migr_connection_info and "pkey" in migr_connection_info:
        migr_connection_info = migr_connection_info.copy()
        pkey_str = migr_connection_info["pkey"]
        migr_connection_info["pkey"] = utils.deserialize_key(
            pkey_str, CONF.serialization.temp_keypair_password)
    return migr_connection_info


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

        migr_source_conn_info = _unmarshal_migr_conn_info(
            task_info["migr_source_connection_info"])

        migr_target_conn_info = _unmarshal_migr_conn_info(
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

        task_info["volumes_info"] = None

        return task_info


class DeployReplicaSourceResourcesTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_EXPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, origin)

        replica_resources_info = provider.deploy_replica_source_resources(
            ctxt, connection_info)

        task_info["migr_source_resources"] = replica_resources_info[
            "migr_resources"]
        migr_connection_info = _marshal_migr_conn_info(
            replica_resources_info["connection_info"])
        task_info["migr_source_connection_info"] = migr_connection_info

        return task_info


class DeleteReplicaSourceResourcesTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_EXPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, origin)

        migr_resources = task_info["migr_source_resources"]

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
            destination["type"], constants.PROVIDER_TYPE_IMPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        volumes_info = task_info["volumes_info"]

        replica_resources_info = provider.deploy_replica_target_resources(
            ctxt, connection_info, target_environment, volumes_info)

        task_info["volumes_info"] = replica_resources_info["volumes_info"]
        task_info["migr_target_resources"] = replica_resources_info[
            "migr_resources"]

        migr_connection_info = _marshal_migr_conn_info(
            replica_resources_info["connection_info"])
        task_info["migr_target_connection_info"] = migr_connection_info

        return task_info


class DeleteReplicaTargetResourcesTask(base.TaskRunner):
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_IMPORT, event_handler)
        connection_info = base.get_connection_info(ctxt, destination)

        migr_resources = task_info["migr_target_resources"]

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
