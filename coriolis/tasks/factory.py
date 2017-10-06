# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis import exception
from coriolis.tasks import migration_tasks
from coriolis.tasks import osmorphing_tasks
from coriolis.tasks import replica_tasks

_TASKS_MAP = {
    constants.TASK_TYPE_EXPORT_INSTANCE:
        migration_tasks.ExportInstanceTask,
    constants.TASK_TYPE_IMPORT_INSTANCE:
        migration_tasks.ImportInstanceTask,
    constants.TASK_TYPE_FINALIZE_IMPORT_INSTANCE:
        migration_tasks.FinalizeImportInstanceTask,
    constants.TASK_TYPE_DEPLOY_DISK_COPY_RESOURCES:
        migration_tasks.DeployDiskCopyResources,
    constants.TASK_TYPE_COPY_DISK_DATA:
        migration_tasks.CopyDiskData,
    constants.TASK_TYPE_DELETE_DISK_COPY_RESOURCES:
        migration_tasks.DeleteDiskCopyResources,
    constants.TASK_TYPE_CLEANUP_FAILED_IMPORT_INSTANCE:
        migration_tasks.CleanupFailedImportInstanceTask,
    constants.TASK_TYPE_GET_OPTIMAL_FLAVOR:
        migration_tasks.GetOptimalFlavorTask,
    constants.TASK_TYPE_DEPLOY_OS_MORPHING_RESOURCES:
        osmorphing_tasks.DeployOSMorphingResourcesTask,
    constants.TASK_TYPE_OS_MORPHING:
        osmorphing_tasks.OSMorphingTask,
    constants.TASK_TYPE_DELETE_OS_MORPHING_RESOURCES:
        osmorphing_tasks.DeleteOSMorphingResourcesTask,
    constants.TASK_TYPE_GET_INSTANCE_INFO:
        replica_tasks.GetInstanceInfoTask,
    constants.TASK_TYPE_REPLICATE_DISKS:
        replica_tasks.ReplicateDisksTask,
    constants.TASK_TYPE_SHUTDOWN_INSTANCE:
        replica_tasks.ShutdownInstanceTask,
    constants.TASK_TYPE_DEPLOY_REPLICA_DISKS:
        replica_tasks.DeployReplicaDisksTask,
    constants.TASK_TYPE_DELETE_REPLICA_DISKS:
        replica_tasks.DeleteReplicaDisksTask,
    constants.TASK_TYPE_DEPLOY_REPLICA_TARGET_RESOURCES:
        replica_tasks.DeployReplicaTargetResourcesTask,
    constants.TASK_TYPE_DELETE_REPLICA_TARGET_RESOURCES:
        replica_tasks.DeleteReplicaTargetResourcesTask,
    constants.TASK_TYPE_DEPLOY_REPLICA_SOURCE_RESOURCES:
        replica_tasks.DeployReplicaSourceResourcesTask,
    constants.TASK_TYPE_DELETE_REPLICA_SOURCE_RESOURCES:
        replica_tasks.DeleteReplicaSourceResourcesTask,
    constants.TASK_TYPE_DEPLOY_REPLICA_INSTANCE:
        replica_tasks.DeployReplicaInstanceTask,
    constants.TASK_TYPE_FINALIZE_REPLICA_INSTANCE_DEPLOYMENT:
        replica_tasks.FinalizeReplicaInstanceDeploymentTask,
    constants.TASK_TYPE_CLEANUP_FAILED_REPLICA_INSTANCE_DEPLOYMENT:
        replica_tasks.CleanupFailedReplicaInstanceDeploymentTask,
    constants.TASK_TYPE_CREATE_REPLICA_DISK_SNAPSHOTS:
        replica_tasks.CreateReplicaDiskSnapshotsTask,
    constants.TASK_TYPE_DELETE_REPLICA_DISK_SNAPSHOTS:
        replica_tasks.DeleteReplicaDiskSnapshotsTask,
    constants.TASK_TYPE_RESTORE_REPLICA_DISK_SNAPSHOTS:
        replica_tasks.RestoreReplicaDiskSnapshotsTask,
}


def get_task_runner(task_type):
    cls = _TASKS_MAP.get(task_type)
    if not cls:
        raise exception.NotFound(
            "TaskRunner not found for task type: %s" % task_type)
    return cls()
