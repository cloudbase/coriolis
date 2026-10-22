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
import unittest
import uuid
import zlib
from unittest import mock

from oslo_config import cfg

from coriolis import constants, data_transfer
from coriolis.db import api as db_api
from coriolis.providers import backup_writers
from coriolis.providers import replicator as replicator_module
from coriolis.tests.integration import base
from coriolis.tests.integration import harness as integration_harness
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

        - Create source / destination endpoints and a Replica transfer via the
          Coriolis REST API (using coriolisclient).
        - Update the transfer to enable disk integrity verification.
        - Execute the transfer and wait for it to complete.
        - Mutate some data on the source instance's primary disk.
        - Execute a second transfer run (incremental=True).
        - Assert that the disk checksums were verified on both runs, and that
          the second run wrote less data than the first one.

        The execution fails if the source and destination checksums differ, so the
        content is verified for any provider pair. The devices are also compared
        directly if the test import provider is being used. Requires a source provider
        that implements ``can_exec_test_vm()``, and which supports disk checksumming.
        """
        if not self._exp_provider.can_exec_test_vm():
            self.skipTest(
                "%s does not support incremental transfer verification"
                % type(self._exp_provider).__name__
            )

        # Enable disk integrity verification for both runs.
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

        # First run: full transfer
        checksummed_disks, full_bytes = self._execute_and_spy_on_writer(
            self._transfer.id
        )

        self.assertEqual(
            self._transfer_disk_ids,
            checksummed_disks,
            "The checksum was not computed after the initial full transfer",
        )
        if self._harness.uses_core_test_import_provider():
            self.assertTrue(
                test_utils.devices_match(self._src_device, self._dst_device),
                "Devices do not match after initial full transfer",
            )

        # Mutate source.
        self._exp_provider.mutate_test_instance_data(self._instance_name)

        if self._harness.uses_core_test_import_provider():
            self.assertFalse(
                test_utils.devices_match(self._src_device, self._dst_device),
                "Devices should differ after mutating the source",
            )

        # Second run: incremental
        checksummed_disks, incremental_bytes = self._execute_and_spy_on_writer(
            self._transfer.id
        )

        self.assertEqual(
            self._transfer_disk_ids,
            checksummed_disks,
            "The checksum was not computed after the incremental transfer",
        )
        self.assertGreater(
            incremental_bytes, 0, "The incremental transfer wrote no data"
        )
        self.assertLess(
            incremental_bytes,
            full_bytes,
            "The incremental transfer wrote as much data as the full one",
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

    @classmethod
    def setUpClass(cls):
        h = integration_harness._IntegrationHarness.get()
        if not h.uses_core_test_export_provider():
            raise unittest.SkipTest(
                "Clustered transfers require the core test export provider"
            )
        super().setUpClass()

    def setUp(self):
        super().setUp()

        self._own_device_a = self._src_device
        self._own_device_b = test_utils.create_loop_device(
            self._SRC_DEVICE_SIZE_MB * 1024 * 1024
        )
        self.addCleanup(test_utils.remove_loop_device, self._own_device_b)
        test_utils.write_test_pattern(self._own_device_b, 8192)

        self._shared_device = test_utils.create_loop_device(
            self._SRC_DEVICE_SIZE_MB * 1024 * 1024
        )
        self.addCleanup(test_utils.remove_loop_device, self._shared_device)
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
        self.assertPoolAllocated(self._dst_pool_id)
        self.assertMachinesAvailable(self._dst_pool_id)


class ReplicaTransferViaSSHTunnelTest(base.ReplicaIntegrationTestBase):
    """Transfer tests using an SSH tunneled replicator client."""

    _EXTRA_SOURCE_ENVIRONMENT = {"use_tunnel": True}

    @classmethod
    def setUpClass(cls):
        h = integration_harness._IntegrationHarness.get()
        if not h.uses_core_test_export_provider():
            raise unittest.SkipTest(
                "'use_tunnel' is a core test export provider option"
            )
        super().setUpClass()

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


class SourceMinionPoolTransferTest(
    base.SourceMinionPoolReplicaTestBase, _ReplicaTransferTestsMixin
):
    """Transfer execution that uses a pre-allocated source minion pool."""

    def test_transfer(self):
        super().test_transfer()
        self.assertPoolAllocated(self._src_pool_id)
        self.assertMachinesAvailable(self._src_pool_id)


class MultiDiskTransferIntegrationTest(base.ReplicaIntegrationTestBase):
    """Replica transfer tests for an instance with more than one disk."""

    @classmethod
    def setUpClass(cls):
        h = integration_harness._IntegrationHarness.get()
        if not h.uses_core_test_export_provider():
            raise unittest.SkipTest(
                "Multi-disk instances require the core test export provider"
            )
        super().setUpClass()

    def setUp(self):
        super().setUp()

        self._root_device = self._src_device
        self._data_device = test_utils.create_loop_device(
            self._SRC_DEVICE_SIZE_MB * 1024 * 1024
        )
        self.addCleanup(test_utils.remove_loop_device, self._data_device)
        test_utils.write_test_pattern(self._data_device, 4096)

        self._multi_disk_instance = "%s-%s-multidisk" % (
            os.path.basename(self._root_device),
            uuid.uuid4().hex[:8],
        )
        # NOTE: the HTTP backup writer is the only one implementing
        # get_disk_checksum().
        self._multi_disk_transfer = self._create_transfer(
            self._src_endpoint.id,
            self._dst_endpoint.id,
            instances=[self._multi_disk_instance],
            source_environment={
                **self._exp_env_options,
                "instance_block_devices": {
                    self._multi_disk_instance: [
                        self._root_device,
                        self._data_device,
                    ],
                },
                "verify_disk_integrity": True,
            },
            destination_environment={
                **self._imp_env_options,
                "data_transfer_mechanism": (
                    backup_writers.DATA_TRANSFER_MECHANISM_HTTPS
                ),
            },
        )
        self.addCleanup(
            self._cleanup_provider_dst_devices, self._multi_disk_transfer.id
        )

    def _dst_devices_by_disk_id(self):
        ctxt = self._get_db_context()
        transfer = db_api.get_transfer(
            ctxt, self._multi_disk_transfer.id, include_task_info=True
        )
        info = transfer.get("info", {}).get(self._multi_disk_instance, {})
        return {
            vol["disk_id"]: vol["volume_dev"]
            for vol in info.get("volumes_info", [])
            if vol.get("volume_dev")
        }

    def test_incremental_transfer_with_unchanged_first_disk(self):
        """Incremental transfer in which only the second disk changed.

        Disks are replicated in order, so an unchanged first disk (e.g. a
        mostly idle root disk) must not prevent the following disks from
        being replicated.
        """
        root_disk_id = os.path.basename(self._root_device)
        data_disk_id = os.path.basename(self._data_device)

        # First run: full transfer
        checksummed_disks, _ = self._execute_and_spy_on_writer(
            self._multi_disk_transfer.id
        )

        self.assertEqual(
            [root_disk_id, data_disk_id],
            checksummed_disks,
            "Both disks should have been replicated in order by the initial "
            "full transfer",
        )

        # Mutate only the second disk.
        test_utils.write_bytes_at_offset(
            self._data_device, offset=4096, data=b"\xff\xfe\xfd\xfc" * 1024
        )

        # Second run: incremental
        checksummed_disks, incremental_bytes = self._execute_and_spy_on_writer(
            self._multi_disk_transfer.id
        )

        self.assertIn(
            data_disk_id,
            checksummed_disks,
            "The changed data disk was not replicated by the incremental transfer",
        )
        self.assertGreater(
            incremental_bytes, 0, "The incremental transfer wrote no data"
        )

        if not self._harness.uses_core_test_import_provider():
            # "volume_dev" is only a host-readable device path with the in-tree
            # test provider.
            return

        dst_devices = self._dst_devices_by_disk_id()
        self.assertTrue(
            test_utils.devices_match(self._root_device, dst_devices[root_disk_id]),
            "Root disk destination does not match its source",
        )
        self.assertTrue(
            test_utils.devices_match(self._data_device, dst_devices[data_disk_id]),
            "Data disk destination does not match its source after the "
            "incremental transfer",
        )
