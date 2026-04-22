# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Export-side (source) implementation of the test provider.

Uses Replicator (via SSH to a Docker data-minion container) to deploy and
manage the coriolis-replicator service and perform disk replication.
"""

import csv
import io
import os
import uuid

from oslo_config import cfg
from oslo_log import log as logging
import paramiko

from coriolis import constants
from coriolis import events
from coriolis.providers import backup_writers
from coriolis.providers.base import BaseEndpointInstancesProvider
from coriolis.providers.base import BaseEndpointInventoryExportProvider
from coriolis.providers.base import BaseEndpointSourceOptionsProvider
from coriolis.providers.base import BaseReplicaExportProvider
from coriolis.providers.base import BaseReplicaExportValidationProvider
from coriolis.providers.base import BaseUpdateSourceReplicaProvider
from coriolis.providers import replicator as replicator_module
from coriolis.tests.integration import utils as test_utils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

# Real VMs always have at least one NIC; destination providers may reject an
# instance with no NIC. "network_map" in the destination's environment options
# must map "test-network" to an existing network on the destination.
_TEST_NIC = {
    "id": "fa:16:3e:12:34:56",
    "network_name": "test-network",
    "mac_address": "fa:16:3e:12:34:56",
}


class TestExportProvider(
        BaseEndpointInstancesProvider,
        BaseEndpointInventoryExportProvider,
        BaseEndpointSourceOptionsProvider,
        BaseUpdateSourceReplicaProvider,
        BaseReplicaExportProvider,
        BaseReplicaExportValidationProvider):
    """Source-side provider backed by a local `scsi_debug` block device.

    ``connection_info`` (the source endpoint's connection info) has the form::

        {
            "pkey_path": "/root/.ssh/id_rsa",  # key for localhost SSH
        }

    ``source_environment`` (per-transfer source settings) has the form::

        {
            "instance_block_devices": {
                "instance-1": ["/dev/sdX"],  # source block device(s)
            },
        }
    """

    platform = "test-src"

    def __init__(self, event_handler):
        self._event_handler = event_handler

    def _event_manager(self):
        return events.EventManager(self._event_handler)

    def _make_replicator(self, conn_info, event_mgr, volumes_info, repl_state):
        """Build a Replicator that connects via SSH to *conn_info*.

        *conn_info* must contain ``ip``, ``port``, ``username``, and
        ``pkey_path`` keys. An optional ``use_tunnel`` key forces the
        replicator client to connect through an SSH tunnel instead of
        directly to the replicator's TCP port.
        """
        pkey = paramiko.RSAKey.from_private_key_file(conn_info["pkey_path"])
        repl_conn_info = {
            "ip": conn_info["ip"],
            "port": conn_info.get("port", 22),
            "username": conn_info.get("username", "root"),
            "pkey": pkey,
        }
        return replicator_module.Replicator(
            repl_conn_info, event_mgr, volumes_info, repl_state,
            use_tunnel=conn_info.get("use_tunnel", False))

    # BaseProvider / BaseEndpointProvider

    def get_connection_info_schema(self):
        return {
            "type": "object",
            "properties": {
                "pkey_path": {"type": "string"},
                "role": {"type": "string"},
            },
            "required": ["pkey_path"],
        }

    def validate_connection(self, ctxt, connection_info):
        pkey_path = connection_info["pkey_path"]
        if not os.path.exists(pkey_path):
            raise ValueError("SSH private key not found: %s" % pkey_path)

    # BaseExportInstanceProvider

    def get_source_environment_schema(self):
        return {
            "type": "object",
            "properties": {
                "instance_block_devices": {"type": "object"},
                "use_tunnel": {"type": "boolean"},
            },
        }

    # BaseEndpointInstancesProvider

    def get_instances(self, ctxt, connection_info, source_environment,
                      limit=None, last_seen_id=None,
                      instance_name_pattern=None, refresh=False):
        # "instance_block_devices" is keyed by instance name.
        instance_block_devices = source_environment.get(
            "instance_block_devices", {})
        names = list(instance_block_devices.keys()) or ["test-instance"]
        return [self._instance_info(name) for name in names]

    def get_instance(self, ctxt, connection_info, source_environment,
                     instance_name):
        return self._instance_info(instance_name)

    # BaseEndpointInventoryExportProvider

    def export_instance_inventory(
            self, ctxt, connection_info, source_environment):
        instance = self._instance_info("test-instance")
        output = io.StringIO()

        writer = csv.writer(output)
        writer.writerow([
            "VM ID", "VM Name", "Guest OS", "Num CPUs", "Memory (MB)",
            "NIC Count",
        ])
        writer.writerow([
            instance["id"],
            instance["name"],
            instance["os_type"],
            instance["num_cpu"],
            instance["memory_mb"],
            len(instance["devices"]["nics"]),
        ])

        return output.getvalue()

    def _instance_info(self, name):
        return {
            "id": name,
            "name": name,
            "instance_name": name,
            "num_cpu": 1,
            "memory_mb": 512,
            "os_type": "linux",
            "nested_virtualization": False,
            "devices": {
                "disks": [],
                "nics": [_TEST_NIC],
                "cdroms": [],
                "serial_ports": [],
                "floppies": [],
                "controllers": [],
            },
        }

    # BaseEndpointSourceOptionsProvider

    def get_source_environment_options(
            self, ctxt, connection_info, env=None, option_names=None):
        return [
            {
                "name": "source_opt",
                "values": ["foo", "lish"],
                "config_default": "foo",
            },
        ]

    # BaseUpdateSourceReplicaProvider

    def check_update_source_environment_params(
            self, ctxt, connection_info, instance_name, volumes_info,
            old_params, new_params):
        return volumes_info

    def get_os_morphing_tools(self, os_type, osmorphing_info):
        return []

    # BaseReplicaExportProvider

    def get_replica_instance_info(
            self, ctxt, connection_info, source_environment, instance_name):
        """Return minimal export info describing the source block device(s)."""
        block_devices = source_environment.get("instance_block_devices", {})
        block_device_paths = block_devices.get(instance_name, [])

        return {
            "id": instance_name,
            "name": instance_name,
            "instance_name": instance_name,
            "num_cpu": 1,
            "memory_mb": 512,
            "os_type": "linux",
            "nested_virtualization": False,
            "devices": {
                "disks": [
                    {
                        "id": os.path.basename(path),
                        "format": "raw",
                        "size_bytes": _get_block_device_size(path),
                    }
                    for path in block_device_paths
                ],
                "nics": [_TEST_NIC],
                "cdroms": [],
                "serial_ports": [],
                "floppies": [],
                "controllers": [],
            },
        }

    def deploy_replica_source_resources(
            self, ctxt, connection_info, export_info, source_environment):
        block_devices = source_environment.get("instance_block_devices", {})
        block_device_paths = block_devices.get(
            export_info["instance_name"], [])
        pkey_path = connection_info["pkey_path"]

        container_name = "coriolis-replicator-%s" % uuid.uuid4().hex[:8]
        container_id = test_utils.run_container(
            test_utils.DATA_MINION_IMAGE,
            container_name,
            is_systemd=True,
            ssh_key=f"{pkey_path}.pub",
            devices=block_device_paths,
        )

        try:
            container_ip = test_utils.get_container_ip(container_id)
            test_utils.wait_for_ssh(container_ip, 22, "root", pkey_path)

            src_conn_info = {
                "ip": container_ip,
                "port": 22,
                "username": "root",
                "pkey_path": pkey_path,
                "use_tunnel": source_environment.get("use_tunnel", False),
            }
            replicator = self._make_replicator(
                src_conn_info, self._event_manager(), [], None)
            replicator.init_replicator()

            disk_mappings = {
                os.path.basename(path): path for path in block_device_paths
            }
            return {
                "connection_info": src_conn_info,
                "migr_resources": {
                    "container_id": container_id,
                    "disk_mappings": disk_mappings,
                },
            }
        except Exception:
            test_utils.remove_container(container_id)
            raise

    def delete_replica_source_resources(
            self, ctxt, connection_info, source_environment,
            migr_resources_dict):
        container_id = (migr_resources_dict or {}).get("container_id")
        if container_id:
            test_utils.remove_container(container_id)

    def replicate_disks(
            self, ctxt, connection_info, source_environment, instance_name,
            source_resources, source_conn_info, target_conn_info,
            volumes_info, incremental):
        repl_state = _extract_repl_state(volumes_info) if incremental else None

        replicator = self._make_replicator(
            source_conn_info, self._event_manager(), volumes_info, repl_state)
        replicator.init_replicator()
        replicator.wait_for_chunks()

        disk_mappings = source_resources.get("disk_mappings", {})
        source_volumes_info = [
            {
                "disk_id": vol["disk_id"],
                "disk_path": disk_mappings.get(vol["disk_id"], vol["disk_id"]),
            }
            for vol in volumes_info
            if vol.get(constants.VOLUME_INFO_REPLICATE_DISK_DATA, True)
        ]
        for vol in volumes_info:
            if not vol.get(constants.VOLUME_INFO_REPLICATE_DISK_DATA, True):
                LOG.debug(
                    "Skipping replication for disk '%s' "
                    "(replicate_disk_data is False; the disk is replicated "
                    "by its owner instance's task).", vol.get("disk_id"))

        backup_writer = backup_writers.BackupWritersFactory(
            target_conn_info, volumes_info).get_writer()

        replicator.replicate_disks(
            source_volumes_info, backup_writer,
            verify_checksum=source_environment.get(
                "verify_disk_integrity", False))
        return volumes_info

    def delete_replica_source_snapshots(
            self, ctxt, connection_info, source_environment, volumes_info):
        # scsi_debug devices have no snapshots.
        return volumes_info

    def shutdown_instance(
            self, ctxt, connection_info, source_environment, instance_name):
        # Nothing to shut down for a block device.
        pass

    # BaseReplicaExportValidationProvider

    def validate_replica_export_input(
            self, ctxt, connection_info, instance_name, source_environment):
        return {}


# Helpers
def _get_block_device_size(device):
    """Return the size in bytes of *device* using its sysfs entry."""
    dev_name = os.path.basename(device)

    size_sectors_path = "/sys/block/%s/size" % dev_name
    with open(size_sectors_path) as fh:
        sectors = int(fh.read().strip())

    return sectors * 512


def _extract_repl_state(volumes_info):
    """Collect per-disk replicator state stored in volumes_info entries."""
    state = []
    for vol in volumes_info:
        rs = vol.get("replica_state")
        if rs:
            state.append(rs)

    return state
