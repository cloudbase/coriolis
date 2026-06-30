# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Integration test for failure handling and cleanup.

Tests two symmetrical error-path scenarios:

- deploy_replica_target_resources is delayed then raises -> source task
  completes first (creating real source resources) -> execution errors ->
  source cleanup (DELETE_TRANSFER_SOURCE_RESOURCES) must run,
  source_resources zeroed out.

- deploy_replica_source_resources is delayed then raises -> target task
  completes first (creating real target resources) -> execution errors ->
  destination cleanup (DELETE_TRANSFER_TARGET_RESOURCES) must run,
  target_resources zeroed out.

Must be run as root; requires the scsi_debug kernel module.
"""

import time
from unittest import mock

from coriolis import constants
from coriolis.db import api as db_api
from coriolis.tests.integration import base


class TransferFailureIntegrationTest(base.ReplicaIntegrationTestBase):
    """Error path and resource cleanup."""

    def _assertResourcesCleaned(self, execution_id, task_type, resource_key):
        ctxt = self._get_db_context()
        execution = db_api.get_tasks_execution(ctxt, execution_id)

        cleanup_task = next(
            (t for t in execution.tasks if t.task_type == task_type),
            None,
        )
        self.assertIsNotNone(
            cleanup_task,
            "%s task not found in execution %s" % (task_type, execution_id),
        )

        self.assertIn(
            cleanup_task.status,
            constants.CLEANUP_TASK_TRIGGER_STATUSES,
            "%s task did not complete (status: %s); %s may have leaked"
            % (task_type, cleanup_task.status, resource_key),
        )

        action = db_api.get_action(ctxt, execution.action_id, include_task_info=True)
        for instance in execution.action.instances:
            self.assertIsNone(
                action.info.get(instance, {}).get(resource_key),
                "%s not cleared for instance '%s' after cleanup task completed"
                % (resource_key, instance),
            )

    def assertSourceResourcesCleaned(self, execution_id):
        self._assertResourcesCleaned(
            execution_id,
            constants.TASK_TYPE_DELETE_TRANSFER_SOURCE_RESOURCES,
            "source_resources",
        )

    def assertTargetResourcesCleaned(self, execution_id):
        self._assertResourcesCleaned(
            execution_id,
            constants.TASK_TYPE_DELETE_TRANSFER_TARGET_RESOURCES,
            "target_resources",
        )

    def test_source_resources_cleaned_on_target_failure(self):
        """Source cleanup runs when target resource deployment fails.

        deploy_replica_target_resources is delayed so that
        deploy_replica_source_resources has time to finish (creating real
        source resources) before the target task eventually raises. The
        conductor must schedule DELETE_TRANSFER_SOURCE_RESOURCES and zero out
        source_resources in the action info.
        """
        injected_error = Exception("injected target resource failure")

        def _slow_then_fail(self_provider, *args, **kwargs):
            time.sleep(15)
            raise injected_error

        with mock.patch.object(
            self._harness.imp_provider_class,
            "deploy_replica_target_resources",
            _slow_then_fail,
        ):
            execution = self._client.transfer_executions.create(
                self._transfer.id, shutdown_instances=False
            )
            self.assertExecutionErrored(execution.id)

        self.assertSourceResourcesCleaned(execution.id)

    def test_target_resources_cleaned_on_source_failure(self):
        """Target cleanup runs when source resource deployment fails.

        deploy_replica_source_resources is delayed so that
        deploy_replica_target_resources has time to finish (creating real
        target resources) before the source task eventually raises. The
        conductor must schedule DELETE_TRANSFER_TARGET_RESOURCES and zero out
        target_resources in the action info.
        """
        injected_error = Exception("injected source resource failure")

        def _slow_then_fail(self_provider, *args, **kwargs):
            time.sleep(15)
            raise injected_error

        with mock.patch.object(
            self._harness.exp_provider_class,
            "deploy_replica_source_resources",
            _slow_then_fail,
        ):
            execution = self._client.transfer_executions.create(
                self._transfer.id, shutdown_instances=False
            )
            self.assertExecutionErrored(execution.id)

        self.assertTargetResourcesCleaned(execution.id)
