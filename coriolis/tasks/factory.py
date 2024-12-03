# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis import exception
from coriolis.tasks import migration_tasks
from coriolis.tasks import minion_pool_tasks
from coriolis.tasks import osmorphing_tasks
from coriolis.tasks import replica_tasks

_TASKS_MAP = {
    constants.TASK_TYPE_FINALIZE_INSTANCE_DEPLOYMENT:
        migration_tasks.FinalizeInstanceDeploymentTask,
    constants.TASK_TYPE_CLEANUP_FAILED_INSTANCE_DEPLOYMENT:
        migration_tasks.CleanupFailedInstanceDeploymentTask,
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
    constants.TASK_TYPE_DEPLOY_TRANSFER_DISKS:
        replica_tasks.DeployReplicaDisksTask,
    constants.TASK_TYPE_DELETE_TRANSFER_SOURCE_DISK_SNAPSHOTS:
        replica_tasks.DeleteReplicaSourceDiskSnapshotsTask,
    constants.TASK_TYPE_DELETE_TRANSFER_DISKS:
        replica_tasks.DeleteReplicaDisksTask,
    constants.TASK_TYPE_DEPLOY_TRANSFER_TARGET_RESOURCES:
        replica_tasks.DeployReplicaTargetResourcesTask,
    constants.TASK_TYPE_DELETE_TRANSFER_TARGET_RESOURCES:
        replica_tasks.DeleteReplicaTargetResourcesTask,
    constants.TASK_TYPE_DEPLOY_TRANSFER_SOURCE_RESOURCES:
        replica_tasks.DeployReplicaSourceResourcesTask,
    constants.TASK_TYPE_DELETE_TRANSFER_SOURCE_RESOURCES:
        replica_tasks.DeleteReplicaSourceResourcesTask,
    constants.TASK_TYPE_DEPLOY_INSTANCE_RESOURCES:
        replica_tasks.DeployReplicaInstanceResourcesTask,
    constants.TASK_TYPE_CREATE_TRANSFER_DISK_SNAPSHOTS:
        replica_tasks.CreateReplicaDiskSnapshotsTask,
    constants.TASK_TYPE_DELETE_TRANSFER_TARGET_DISK_SNAPSHOTS:
        replica_tasks.DeleteReplicaTargetDiskSnapshotsTask,
    constants.TASK_TYPE_RESTORE_TRANSFER_DISK_SNAPSHOTS:
        replica_tasks.RestoreReplicaDiskSnapshotsTask,
    constants.TASK_TYPE_VALIDATE_TRANSFER_SOURCE_INPUTS:
        replica_tasks.ValidateReplicaExecutionSourceInputsTask,
    constants.TASK_TYPE_VALIDATE_TRANSFER_DESTINATION_INPUTS:
        replica_tasks.ValidateReplicaExecutionDestinationInputsTask,
    constants.TASK_TYPE_VALIDATE_DEPLOYMENT_INPUTS:
        replica_tasks.ValidateReplicaDeploymentParametersTask,
    constants.TASK_TYPE_UPDATE_SOURCE_TRANSFER:
        replica_tasks.UpdateSourceReplicaTask,
    constants.TASK_TYPE_UPDATE_DESTINATION_TRANSFER:
        replica_tasks.UpdateDestinationReplicaTask,
    constants.TASK_TYPE_VALIDATE_SOURCE_MINION_POOL_OPTIONS:
        minion_pool_tasks.ValidateSourceMinionPoolOptionsTask,
    constants.TASK_TYPE_VALIDATE_DESTINATION_MINION_POOL_OPTIONS:
        minion_pool_tasks.ValidateDestinationMinionPoolOptionsTask,
    constants.TASK_TYPE_CREATE_SOURCE_MINION_MACHINE:
        minion_pool_tasks.CreateSourceMinionMachineTask,
    constants.TASK_TYPE_CREATE_DESTINATION_MINION_MACHINE:
        minion_pool_tasks.CreateDestinationMinionMachineTask,
    constants.TASK_TYPE_DELETE_SOURCE_MINION_MACHINE:
        minion_pool_tasks.DeleteSourceMinionMachineTask,
    constants.TASK_TYPE_DELETE_DESTINATION_MINION_MACHINE:
        minion_pool_tasks.DeleteDestinationMinionMachineTask,
    constants.TASK_TYPE_SET_UP_SOURCE_POOL_SHARED_RESOURCES:
        minion_pool_tasks.SetUpSourcePoolSupportingResourcesTask,
    constants.TASK_TYPE_SET_UP_DESTINATION_POOL_SHARED_RESOURCES:
        minion_pool_tasks.SetUpDestinationPoolSupportingResources,
    constants.TASK_TYPE_TEAR_DOWN_SOURCE_POOL_SHARED_RESOURCES:
        minion_pool_tasks.TearDownSourcePoolSupportingResourcesTask,
    constants.TASK_TYPE_TEAR_DOWN_DESTINATION_POOL_SHARED_RESOURCES:
        minion_pool_tasks.TearDownDestinationPoolSupportingResources,
    constants.TASK_TYPE_ATTACH_VOLUMES_TO_SOURCE_MINION:
        minion_pool_tasks.AttachVolumesToSourceMinionTask,
    constants.TASK_TYPE_DETACH_VOLUMES_FROM_SOURCE_MINION:
        minion_pool_tasks.DetachVolumesFromSourceMinionTask,
    constants.TASK_TYPE_ATTACH_VOLUMES_TO_DESTINATION_MINION:
        minion_pool_tasks.AttachVolumesToDestinationMinionTask,
    constants.TASK_TYPE_DETACH_VOLUMES_FROM_DESTINATION_MINION:
        minion_pool_tasks.DetachVolumesFromDestinationMinionTask,
    constants.TASK_TYPE_ATTACH_VOLUMES_TO_OSMORPHING_MINION:
        minion_pool_tasks.AttachVolumesToOSMorphingMinionTask,
    constants.TASK_TYPE_DETACH_VOLUMES_FROM_OSMORPHING_MINION:
        minion_pool_tasks.DetachVolumesFromOSMorphingMinionTask,
    constants.TASK_TYPE_VALIDATE_SOURCE_MINION_POOL_COMPATIBILITY:
        minion_pool_tasks.ValidateSourceMinionCompatibilityTask,
    constants.TASK_TYPE_VALIDATE_DESTINATION_MINION_POOL_COMPATIBILITY:
        minion_pool_tasks.ValidateDestinationMinionCompatibilityTask,
    constants.TASK_TYPE_VALIDATE_OSMORPHING_MINION_POOL_COMPATIBILITY:
        minion_pool_tasks.ValidateOSMorphingMinionCompatibilityTask,
    constants.TASK_TYPE_RELEASE_SOURCE_MINION:
        minion_pool_tasks.ReleaseSourceMinionTask,
    constants.TASK_TYPE_RELEASE_DESTINATION_MINION:
        minion_pool_tasks.ReleaseDestinationMinionTask,
    constants.TASK_TYPE_RELEASE_OSMORPHING_MINION:
        minion_pool_tasks.ReleaseOSMorphingMinionTask,
    constants.TASK_TYPE_COLLECT_OSMORPHING_INFO:
        minion_pool_tasks.CollectOSMorphingInfoTask,
    constants.TASK_TYPE_HEALTHCHECK_SOURCE_MINION:
        minion_pool_tasks.HealthcheckSourceMinionMachineTask,
    constants.TASK_TYPE_HEALTHCHECK_DESTINATION_MINION:
        minion_pool_tasks.HealthcheckDestinationMinionTask,
    constants.TASK_TYPE_POWER_ON_SOURCE_MINION:
        minion_pool_tasks.PowerOnSourceMinionTask,
    constants.TASK_TYPE_POWER_OFF_SOURCE_MINION:
        minion_pool_tasks.PowerOffSourceMinionTask,
    constants.TASK_TYPE_POWER_ON_DESTINATION_MINION:
        minion_pool_tasks.PowerOnDestinationMinionTask,
    constants.TASK_TYPE_POWER_OFF_DESTINATION_MINION:
        minion_pool_tasks.PowerOffDestinationMinionTask
}


def get_task_runner_class(task_type):
    cls = _TASKS_MAP.get(task_type)
    if not cls:
        raise exception.NotFound(
            "TaskRunner not found for task type: %s" % task_type)
    return cls
