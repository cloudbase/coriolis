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
import time
import uuid
import zlib
from unittest import mock

from oslo_config import cfg

from coriolis import constants, data_transfer
from coriolis.db import api as db_api
from coriolis.providers import backup_writers
from coriolis.providers import replicator as replicator_module
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
            self._transfer.id, {"notes": "updated by integration test"}
        )
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
                        backup_writers.DATA_TRANSFER_MECHANISM_HTTPS
                    ),
                },
            },
        )
        self.assertExecutionCompleted(execution.id)

        checksummed_disks = []
        original = backup_writers.HTTPBackupWriterImpl.get_disk_checksum

        def _recording_get_disk_checksum(writer, *args, **kwargs):
            checksummed_disks.append(writer._disk_id)
            return original(writer, *args, **kwargs)

        # Second run: incremental
        with mock.patch.object(
            backup_writers.HTTPBackupWriterImpl,
            "get_disk_checksum",
            _recording_get_disk_checksum,
        ):
            self._execute_and_wait(self._transfer.id)

        self.assertEqual(
            [os.path.basename(self._src_device)],
            checksummed_disks,
            "The checksum was not computed for the transferred disk",
        )

        if self._harness.uses_core_test_import_provider():
            self.assertTrue(
                test_utils.devices_match(self._src_device, self._dst_device),
                "Destination does not match source after incremental transfer",
            )


class ReplicaTransferIntegrationTest(
    base.ReplicaIntegrationTestBase, _ReplicaTransferTestsMixin
):
    """Full-pipeline replica transfer integration tests."""

    def test_transfer_with_ssh_backup_writer(self):
        # NOTE: for minion pools, updating the data_transfer_mechanism will not
        # set up the new transfer mechanism into existing minions.
        execution = self._client.transfers.update(
            self._transfer.id,
            {
                "destination_environment": {
                    "data_transfer_mechanism": (
                        backup_writers.DATA_TRANSFER_MECHANISM_SSH
                    ),
                },
            },
        )
        self.assertExecutionCompleted(execution.id)

        # Record the writers handed out during the run.
        writer_types = set()
        original = backup_writers.BackupWritersFactory.get_writer

        def _recording_get_writer(factory, *args, **kwargs):
            writer = original(factory, *args, **kwargs)
            writer_types.add(type(writer))
            return writer

        with mock.patch.object(
            backup_writers.BackupWritersFactory, "get_writer", _recording_get_writer
        ):
            self._execute_and_wait(self._transfer.id)

        self.assertEqual(
            {backup_writers.SSHBackupWriter},
            writer_types,
            "The transfer did not use the expected SSH backup writer",
        )

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

        server = socketserver.UnixStreamServer(socket_path, _CompressorHandler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        self.addCleanup(server_thread.join)
        self.addCleanup(server.shutdown)

        CONF.set_override("compressor_address", socket_path)
        self.addCleanup(CONF.clear_override, "compressor_address")

        with mock.patch.object(data_transfer.LOG, "exception") as mock_exc:
            self._execute_and_wait(self._transfer.id)

        self.assertGreater(
            call_count, 0, "External compressor service was never invoked"
        )
        mock_exc.assert_not_called()


class ClusteredTransferIntegrationTest(base.ReplicaIntegrationTestBase):
    """Clustered (multi-instance) replica transfer integration tests.

    A transfer with more than one instance is considered "clustered". The
    conductor runs a cross-instance sync barrier and assigns disk owners
    across instances. Each instance has its own disk + a disk shared between
    them (same disk id in both instances' export_info).
    """

    def setUp(self):
        super().setUp()

        self._own_device_a = self._src_device
        self._own_device_b = test_utils.add_scsi_debug_device()
        self.addCleanup(test_utils.remove_scsi_debug_device)
        test_utils.write_test_pattern(self._own_device_b, 8192)

        self._shared_device = test_utils.add_scsi_debug_device()
        self.addCleanup(test_utils.remove_scsi_debug_device)
        test_utils.write_test_pattern(self._shared_device, 4096)

        self._instance_a = "%s-%s-clustered" % (
            os.path.basename(self._own_device_a),
            uuid.uuid4().hex[:8],
        )
        self._instance_b = "%s-%s-clustered" % (
            os.path.basename(self._own_device_b),
            uuid.uuid4().hex[:8],
        )
        self._clustered_transfer = self._create_transfer(
            self._src_endpoint.id,
            self._dst_endpoint.id,
            instances=[self._instance_a, self._instance_b],
            source_environment={
                "instance_block_devices": {
                    self._instance_a: [self._own_device_a, self._shared_device],
                    self._instance_b: [self._own_device_b, self._shared_device],
                },
            },
        )

    def _volumes_info_for_instance(self, transfer_id, instance_name):
        ctxt = self._get_db_context()
        transfer = db_api.get_transfer(ctxt, transfer_id, include_task_info=True)
        info = transfer.get("info", {}).get(instance_name, {})
        return info.get("volumes_info", [])

    def test_clustered_transfer_with_shared_disk(self):
        # The conductor assigns the first instance as the shared disk's
        # owner: only its DEPLOY_TRANSFER_DISKS task creates a destination
        # volume and replicates data into it, while the other instance's
        # task records a placeholder volumes_info entry with
        # "replicate_disk_data" False and no "volume_dev".
        #
        # Assert that exactly one destination volume was created for the
        # shared disk, (transferred only once), while each instance's own
        # private disk is still transferred independently.
        if not self._harness.imp_provider.supports_shared_disks():
            self.skipTest(
                "Destination provider '%s' does not support shared disks"
                % type(self._harness.imp_provider).__name__
            )

        self._execute_and_wait(self._clustered_transfer.id)

        volumes_a = self._volumes_info_for_instance(
            self._clustered_transfer.id, self._instance_a
        )
        own_disk_id_a = os.path.basename(self._own_device_a)
        own_vol_a = next(v for v in volumes_a if v["disk_id"] == own_disk_id_a)

        volumes_b = self._volumes_info_for_instance(
            self._clustered_transfer.id, self._instance_b
        )
        own_disk_id_b = os.path.basename(self._own_device_b)
        own_vol_b = next(v for v in volumes_b if v["disk_id"] == own_disk_id_b)

        shared_disk_id = os.path.basename(self._shared_device)
        shared_vol_a = next(v for v in volumes_a if v["disk_id"] == shared_disk_id)
        shared_vol_b = next(v for v in volumes_b if v["disk_id"] == shared_disk_id)

        # The shared disk was only transferred once, by its owner.
        transferred = [v for v in (shared_vol_a, shared_vol_b) if v.get("volume_dev")]
        skipped = [v for v in (shared_vol_a, shared_vol_b) if not v.get("volume_dev")]
        self.assertEqual(
            1,
            len(transferred),
            "Expected exactly one destination volume for the shared disk "
            "'%s', got: %s" % (shared_disk_id, [shared_vol_a, shared_vol_b]),
        )
        self.assertEqual(1, len(skipped))
        self.assertFalse(
            skipped[0].get(constants.VOLUME_INFO_REPLICATE_DISK_DATA, True),
            "The non-owner instance's shared disk entry should have "
            "replicate_disk_data=False",
        )

        # The shared disk's single destination volume is distinct from
        # either instance's own disk destination.
        self.assertNotIn(
            transferred[0]["volume_dev"],
            (own_vol_a["volume_dev"], own_vol_b["volume_dev"]),
        )

        if not self._harness.uses_core_test_import_provider():
            # "volume_dev" is only a host-readable device path with the in-tree
            # test provider.
            return

        # Each instance's own disk was transferred independently.
        self.assertTrue(
            test_utils.devices_match(self._own_device_a, own_vol_a["volume_dev"]),
            "Instance '%s' own disk destination does not match its source"
            % self._instance_a,
        )
        self.assertTrue(
            test_utils.devices_match(self._own_device_b, own_vol_b["volume_dev"]),
            "Instance '%s' own disk destination does not match its source"
            % self._instance_b,
        )
        self.assertTrue(
            test_utils.devices_match(self._shared_device, transferred[0]["volume_dev"]),
            "Shared disk destination does not match its source",
        )

    def test_clustered_transfer_peer_sync_barrier_abort_on_error(self):
        # If one instance's task errors out while a peer instance's task of the
        # same type is stuck waiting at the cross-instance sync barrier, the
        # stuck peer must be aborted rather than left deadlocked forever.
        transfer = self._clustered_transfer

        injected_error = Exception("injected clustered peer failure")
        original = self._harness.exp_provider_class.get_replica_instance_info

        def _fail_for_instance_a(
            self_provider, ctxt, connection_info, source_environment, instance_name
        ):
            if instance_name == self._instance_a:
                # instance_b's task must complete and reach SYNCING first.
                time.sleep(5)
                raise injected_error

            return original(
                self_provider, ctxt, connection_info, source_environment, instance_name
            )

        with mock.patch.object(
            self._harness.exp_provider_class,
            "get_replica_instance_info",
            _fail_for_instance_a,
        ):
            execution = self._client.transfer_executions.create(
                transfer.id, shutdown_instances=False
            )
            self.addCleanup(self._cleanup_execution, transfer.id, execution.id)
            self.assertExecutionErrored(execution.id)

        final = db_api.get_tasks_execution(self._get_db_context(), execution.id)
        info_tasks = {
            t.instance: t
            for t in final.tasks
            if t.task_type == constants.TASK_TYPE_GET_INSTANCE_INFO
        }
        self.assertEqual(
            {self._instance_a, self._instance_b},
            set(info_tasks),
            "Expected a %s task for each clustered instance"
            % constants.TASK_TYPE_GET_INSTANCE_INFO,
        )
        for instance, task in info_tasks.items():
            self.assertEqual(
                constants.TASK_STATUS_ERROR,
                task.status,
                "%s task for instance '%s' ended with status %s instead "
                "of ERROR; exception_details: %s"
                % (
                    constants.TASK_TYPE_GET_INSTANCE_INFO,
                    instance,
                    task.status,
                    task.exception_details,
                ),
            )


class MinionPoolTransferTest(
    base.MinionPoolReplicaTestBase, _ReplicaTransferTestsMixin
):
    """Transfer execution that uses a pre-allocated destination minion pool."""

    def test_transfer(self):
        super().test_transfer()
        self.assertPoolAllocated(self._pool_id)
        self.assertMachinesAvailable(self._pool_id)


class ReplicaTransferViaSSHTunnelTest(base.ReplicaIntegrationTestBase):
    """Transfer tests using an SSH tunneled replicator client."""

    _EXTRA_SOURCE_ENVIRONMENT = {"use_tunnel": True}

    def test_transfer_via_ssh_tunnel(self):
        tunnel_starts = []
        original_get_ssh_tunnel = replicator_module.Client._get_ssh_tunnel

        def _spy_get_ssh_tunnel(client_self):
            tunnel = original_get_ssh_tunnel(client_self)
            tunnel.start = mock.Mock(wraps=tunnel.start)
            tunnel_starts.append(tunnel.start)
            return tunnel

        with mock.patch.object(
            replicator_module.Client, "_get_ssh_tunnel", _spy_get_ssh_tunnel
        ):
            self._execute_and_wait(self._transfer.id)

        self.assertTrue(tunnel_starts, "SSH tunnel was never constructed")
        self.assertTrue(
            any(t.called for t in tunnel_starts),
            "SSH tunnel was constructed but never started",
        )

        if self._harness.uses_core_test_import_provider():
            self.assertTrue(
                test_utils.devices_match(self._src_device, self._dst_device),
                "Devices do not match after transfer via SSH tunnel",
            )
