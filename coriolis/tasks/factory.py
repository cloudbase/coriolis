# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis import exception
from coriolis.tasks import migration_tasks
from coriolis.tasks import osmorphing_tasks
from coriolis.tasks import replica_tasks

_TASKS_MAP = {
    constants.TASK_TYPE_DEPLOY_MIGRATION_SOURCE_RESOURCES: {
        "task_platform": constants.TASK_PLATFORM_SOURCE,
        "task_runner_class": migration_tasks.DeployMigrationSourceResourcesTask},
    constants.TASK_TYPE_DEPLOY_MIGRATION_TARGET_RESOURCES: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": migration_tasks.DeployMigrationTargetResourcesTask},
    constants.TASK_TYPE_DELETE_MIGRATION_SOURCE_RESOURCES: {
        "task_platform": constants.TASK_PLATFORM_SOURCE,
        "task_runner_class": migration_tasks.DeleteMigrationSourceResourcesTask},
    constants.TASK_TYPE_DELETE_MIGRATION_TARGET_RESOURCES: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": migration_tasks.DeleteMigrationTargetResourcesTask},
    constants.TASK_TYPE_DEPLOY_INSTANCE_RESOURCES: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": migration_tasks.DeployInstanceResourcesTask},
    constants.TASK_TYPE_FINALIZE_INSTANCE_DEPLOYMENT: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": migration_tasks.FinalizeInstanceDeploymentTask},
    constants.TASK_TYPE_CREATE_INSTANCE_DISKS: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": migration_tasks.CreateInstanceDisksTask},
    constants.TASK_TYPE_CLEANUP_FAILED_INSTANCE_DEPLOYMENT: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": migration_tasks.CleanupFailedInstanceDeploymentTask},
    constants.TASK_TYPE_CLEANUP_INSTANCE_TARGET_STORAGE: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": migration_tasks.CleanupInstanceTargetStorageTask},
    constants.TASK_TYPE_CLEANUP_INSTANCE_SOURCE_STORAGE: {
        "task_platform": constants.TASK_PLATFORM_SOURCE,
        "task_runner_class": migration_tasks.CleanupInstanceSourceStorageTask},
    constants.TASK_TYPE_GET_OPTIMAL_FLAVOR: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": migration_tasks.GetOptimalFlavorTask},
    constants.TASK_TYPE_VALIDATE_MIGRATION_SOURCE_INPUTS: {
        "task_platform": constants.TASK_PLATFORM_SOURCE,
        "task_runner_class": migration_tasks.ValidateMigrationSourceInputsTask},
    constants.TASK_TYPE_VALIDATE_MIGRATION_DESTINATION_INPUTS: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": migration_tasks.ValidateMigrationDestinationInputsTask},
    constants.TASK_TYPE_DEPLOY_OS_MORPHING_RESOURCES: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": osmorphing_tasks.DeployOSMorphingResourcesTask},
    constants.TASK_TYPE_OS_MORPHING: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": osmorphing_tasks.OSMorphingTask},
    constants.TASK_TYPE_DELETE_OS_MORPHING_RESOURCES: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": osmorphing_tasks.DeleteOSMorphingResourcesTask},
    constants.TASK_TYPE_GET_INSTANCE_INFO: {
        "task_platform": constants.TASK_PLATFORM_SOURCE,
        "task_runner_class": replica_tasks.GetInstanceInfoTask},
    constants.TASK_TYPE_REPLICATE_DISKS: {
        "task_platform": constants.TASK_PLATFORM_SOURCE,
        "task_runner_class": replica_tasks.ReplicateDisksTask},
    constants.TASK_TYPE_SHUTDOWN_INSTANCE: {
        "task_platform": constants.TASK_PLATFORM_SOURCE,
        "task_runner_class": replica_tasks.ShutdownInstanceTask},
    constants.TASK_TYPE_DEPLOY_REPLICA_DISKS: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": replica_tasks.DeployReplicaDisksTask},
    constants.TASK_TYPE_DELETE_REPLICA_SOURCE_DISK_SNAPSHOTS: {
        "task_platform": constants.TASK_PLATFORM_SOURCE,
        "task_runner_class": replica_tasks.DeleteReplicaSourceDiskSnapshotsTask},
    constants.TASK_TYPE_DELETE_REPLICA_DISKS: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": replica_tasks.DeleteReplicaDisksTask},
    constants.TASK_TYPE_DEPLOY_REPLICA_TARGET_RESOURCES: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": replica_tasks.DeployReplicaTargetResourcesTask},
    constants.TASK_TYPE_DELETE_REPLICA_TARGET_RESOURCES: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": replica_tasks.DeleteReplicaTargetResourcesTask},
    constants.TASK_TYPE_DEPLOY_REPLICA_SOURCE_RESOURCES: {
        "task_platform": constants.TASK_PLATFORM_SOURCE,
        "task_runner_class": replica_tasks.DeployReplicaSourceResourcesTask},
    constants.TASK_TYPE_DELETE_REPLICA_SOURCE_RESOURCES: {
        "task_platform": constants.TASK_PLATFORM_SOURCE,
        "task_runner_class": replica_tasks.DeleteReplicaSourceResourcesTask},
    constants.TASK_TYPE_DEPLOY_REPLICA_INSTANCE_RESOURCES: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": replica_tasks.DeployReplicaInstanceResourcesTask},
    constants.TASK_TYPE_FINALIZE_REPLICA_INSTANCE_DEPLOYMENT: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": replica_tasks.FinalizeReplicaInstanceDeploymentTask},
    constants.TASK_TYPE_CLEANUP_FAILED_REPLICA_INSTANCE_DEPLOYMENT: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": replica_tasks.CleanupFailedReplicaInstanceDeploymentTask},
    constants.TASK_TYPE_CREATE_REPLICA_DISK_SNAPSHOTS: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": replica_tasks.CreateReplicaDiskSnapshotsTask},
    constants.TASK_TYPE_DELETE_REPLICA_TARGET_DISK_SNAPSHOTS: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": replica_tasks.DeleteReplicaTargetDiskSnapshotsTask},
    constants.TASK_TYPE_RESTORE_REPLICA_DISK_SNAPSHOTS: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": replica_tasks.RestoreReplicaDiskSnapshotsTask},
    constants.TASK_TYPE_VALIDATE_REPLICA_SOURCE_INPUTS: {
        "task_platform": constants.TASK_PLATFORM_SOURCE,
        "task_runner_class": replica_tasks.ValidateReplicaExecutionSourceInputsTask},
    constants.TASK_TYPE_VALIDATE_REPLICA_DESTINATION_INPUTS: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": replica_tasks.ValidateReplicaExecutionDestinationInputsTask},
    constants.TASK_TYPE_VALIDATE_REPLICA_DEPLOYMENT_INPUTS: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": replica_tasks.ValidateReplicaDeploymentParametersTask},
    constants.TASK_TYPE_UPDATE_SOURCE_REPLICA: {
        "task_platform": constants.TASK_PLATFORM_SOURCE,
        "task_runner_class": replica_tasks.UpdateSourceReplicaTask},
    constants.TASK_TYPE_UPDATE_DESTINATION_REPLICA: {
        "task_platform": constants.TASK_PLATFORM_DESTINATION,
        "task_runner_class": replica_tasks.UpdateDestinationReplicaTask}
}


def _get_task_class_info(task_type):
    match = _TASKS_MAP.get(task_type)
    if not match:
        raise exception.NotFound(
            "TaskRunner info not found for task type: %s" % task_type)
    return match


def get_task_runner_class(task_type):
    return _get_task_class_info(task_type)["task_runner_class"]


def get_task_platform(task_type):
    return _get_task_class_info(task_type)["task_platform"]
