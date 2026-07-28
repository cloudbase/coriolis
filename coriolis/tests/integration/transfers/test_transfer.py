# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Integration tests for the replica transfer pipeline.

Must be run as root.
"""

import gzip
import http.server
import os
import shutil
import socketserver
import tempfile
import threading
from unittest import mock
import zlib

from oslo_config import cfg

from coriolis import data_transfer
from coriolis.providers import backup_writers
from coriolis.tests.integration import base
from coriolis.tests.integration import utils as test_utils

CONF = cfg.CONF

_COMPRESS_FUNC = {
    "gzip": gzip.compress,
    "zlib": zlib.compress,
}


class _ReplicaTransferTestsMixin:

    def test_transfer(self):
        # List the transfer
        transfers = self._client.transfers.list(detail=True)
        ids = [t.id for t in transfers]
        self.assertIn(self._transfer.id, ids)

        self._execute_and_wait(self._transfer.id)

        # Update the transfer
        execution = self._client.transfers.update(
            self._transfer.id, {"notes": "updated by integration test"})
        self.assertExecutionCompleted(execution.id)

        updated = self._client.transfers.get(self._transfer.id)
        self.assertEqual("updated by integration test", updated.notes)

        # Delete the disk
        execution = self._client.transfers.delete_disks(self._transfer.id)
        self.assertExecutionCompleted(execution.id)

    def test_incremental_replica_transfer(self):
        """Full transfer followed by incremental after source modification.

        - Write a known byte pattern to the source loop device.
        - Create source / destination endpoints and a Replica transfer via the
          Coriolis REST API (using coriolisclient).
        - Execute the transfer and wait for it to complete.
        - Overwrite a single chunk on the source device.
        - Update the transfer to enable disk integrity verification.
        - Execute a second transfer run (incremental=True).
        - Assert that the destination disk checksum was computed.

        The content is verified only if the test import provider is being used.
        """
        # First run: full transfer
        self._execute_and_wait(self._transfer.id)

        if self._harness.uses_core_test_import_provider():
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
        if self._harness.uses_core_test_import_provider():
            self.assertFalse(
                test_utils.devices_match(self._src_device, self._dst_device),
                "Devices should differ after mutating the source",
            )

        # Enable disk integrity verification for the incremental run.
        # Note that the HTTP backup writer is the only one implementing
        # get_disk_checksum().
        execution = self._client.transfers.update(
            self._transfer.id,
            {
                "source_environment": {"verify_disk_integrity": True},
                "destination_environment": {
                    "data_transfer_mechanism": (
                        backup_writers.DATA_TRANSFER_MECHANISM_HTTPS),
                },
            })
        self.assertExecutionCompleted(execution.id)

        checksummed_disks = []
        original = backup_writers.HTTPBackupWriterImpl.get_disk_checksum

        def _recording_get_disk_checksum(writer, *args, **kwargs):
            checksummed_disks.append(writer._disk_id)
            return original(writer, *args, **kwargs)

        # Second run: incremental
        with mock.patch.object(
                backup_writers.HTTPBackupWriterImpl, "get_disk_checksum",
                _recording_get_disk_checksum):
            self._execute_and_wait(self._transfer.id)

        self.assertEqual(
            [os.path.basename(self._src_device)],
            checksummed_disks,
            "The checksum was not computed for the transferred disk")

        if self._harness.uses_core_test_import_provider():
            self.assertTrue(
                test_utils.devices_match(self._src_device, self._dst_device),
                "Destination does not match source after incremental transfer",
            )


class ReplicaTransferIntegrationTest(
    base.ReplicaIntegrationTestBase, _ReplicaTransferTestsMixin):
    """Full-pipeline replica transfer integration tests."""

    def test_transfer_with_ssh_backup_writer(self):
        # NOTE: for minion pools, updating the data_transfer_mechanism will not
        # set up the new transfer mechanism into existing minions.
        execution = self._client.transfers.update(
            self._transfer.id,
            {
                "destination_environment": {
                    "data_transfer_mechanism": (
                        backup_writers.DATA_TRANSFER_MECHANISM_SSH),
                },
            })
        self.assertExecutionCompleted(execution.id)

        # Record the writers handed out during the run.
        writer_types = set()
        original = backup_writers.BackupWritersFactory.get_writer

        def _recording_get_writer(factory, *args, **kwargs):
            writer = original(factory, *args, **kwargs)
            writer_types.add(type(writer))
            return writer

        with mock.patch.object(
                backup_writers.BackupWritersFactory, "get_writer",
                _recording_get_writer):
            self._execute_and_wait(self._transfer.id)

        self.assertEqual(
            {backup_writers.SSHBackupWriter},
            writer_types,
            "The transfer did not use the expected SSH backup writer")

    def test_transfer_with_external_compressor(self):
        # Compression is typically done in-process. For this test, point
        # compressor_address at a unix-socket HTTP service.
        socket_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, socket_dir, ignore_errors=True)
        socket_path = os.path.join(socket_dir, "compressor.sock")

        call_count = 0

        class _CompressorHandler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                nonlocal call_count
                call_count += 1

                length = int(self.headers["Content-Length"])
                body = self.rfile.read(length)
                fmt = self.headers["X-Compression-Format"]
                compressed = _COMPRESS_FUNC[fmt](body)

                self.send_response(200)
                self.send_header("Content-Length", str(len(compressed)))
                self.end_headers()
                self.wfile.write(compressed)

            def address_string(self):
                # Default impl indexes client_address, which is empty '' for
                # a unix socket, causing an IndexError while logging.
                return self.client_address

        server = socketserver.UnixStreamServer(
            socket_path, _CompressorHandler)
        server_thread = threading.Thread(
            target=server.serve_forever, daemon=True)
        server_thread.start()
        self.addCleanup(server_thread.join)
        self.addCleanup(server.shutdown)

        CONF.set_override("compressor_address", socket_path)
        self.addCleanup(CONF.clear_override, "compressor_address")

        with mock.patch.object(data_transfer.LOG, "exception") as mock_exc:
            self._execute_and_wait(self._transfer.id)

        self.assertGreater(
            call_count, 0,
            "External compressor service was never invoked")
        mock_exc.assert_not_called()


class MinionPoolTransferTest(
        base.MinionPoolReplicaTestBase, _ReplicaTransferTestsMixin):
    """Transfer execution that uses a pre-allocated destination minion pool."""

    def test_transfer(self):
        super().test_transfer()
        self.assertPoolAllocated(self._pool_id)
        self.assertMachinesAvailable(self._pool_id)
