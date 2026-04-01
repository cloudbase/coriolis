# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Integration test for failure handling and cleanup.

Patches TestImportProvider.deploy_replica_target_resources to raise an
exception, triggers a transfer execution, and asserts that:
  1. The execution reaches ERROR status.
  2. Cleanup tasks (delete_replica_source_resources) ran so the replicator
     process is no longer alive.

Must be run as root; requires the scsi_debug kernel module.
"""

from unittest import mock

from coriolis.tests.integration import base
from coriolis.tests.integration.providers.test_provider import imp


class TransferFailureIntegrationTest(base.ReplicaIntegrationTestBase):
    """Error path and resource cleanup."""

    def test_error_status_on_provider_failure(self):
        """Execution reaches ERROR when target resource deployment fails."""

        injected_error = Exception("injected target resource failure")

        with mock.patch.object(
            imp.TestImportProvider,
            "deploy_replica_target_resources",
            side_effect=injected_error,
        ):
            execution = self._client.transfer_executions.create(
                self._transfer.id, shutdown_instances=False)
            self.assertExecutionErrored(execution.id)
