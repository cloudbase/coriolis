# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Integration tests for the replica transfer pipeline.

The test has the following steps:
  - Write a known byte pattern to the source loop device.
  - Create source / destination endpoints and a Replica transfer via the
    Coriolis REST API (using coriolisclient).
  - Execute the transfer and wait for it to complete.
  - Assert that the destination device contains the same data as the source.
  - Overwrite a single chunk on the source device.
  - Execute a second transfer run (incremental=True).
  - Assert that the destination now matches the updated source.

Must be run as root.
"""

from coriolis.tests.integration import base
from coriolis.tests.integration import utils as test_utils


class ReplicaTransferIntegrationTest(base.ReplicaIntegrationTestBase):
    """Full-pipeline replica transfer integration tests."""

    def test_incremental_replica_transfer(self):
        """Full transfer followed by incremental after source modification."""
        # First run: full transfer
        self._execute_and_wait(self._transfer.id)

        self.assertTrue(
            test_utils.devices_match(self._src_device, self._dst_device),
            "Devices do not match after initial full transfer",
        )

        # Mutate source: write a different pattern at the second chunk
        test_utils.write_bytes_at_offset(
            self._src_device,
            offset=4096,
            data=b"\xff\xfe\xfd\xfc" * 1024,
        )
        self.assertFalse(
            test_utils.devices_match(self._src_device, self._dst_device),
            "Devices should differ after mutating the source",
        )

        # Second run: incremental
        self._execute_and_wait(self._transfer.id)

        self.assertTrue(
            test_utils.devices_match(self._src_device, self._dst_device),
            "Destination does not match source after incremental transfer",
        )
