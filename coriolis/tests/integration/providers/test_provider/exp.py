# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Export-side (source) implementation of the test provider.

Uses Replicator (via SSH to 127.0.0.1) to deploy and manage the
coriolis-replicator service and perform disk replication.
"""

import os

from oslo_config import cfg
from oslo_log import log as logging
import paramiko

from coriolis import events
from coriolis.providers import backup_writers
from coriolis.providers.base import BaseEndpointInstancesProvider
from coriolis.providers.base import BaseEndpointSourceOptionsProvider
from coriolis.providers.base import BaseReplicaExportProvider
from coriolis.providers.base import BaseReplicaExportValidationProvider
from coriolis.providers.base import BaseUpdateSourceReplicaProvider
from coriolis.providers import replicator as replicator_module

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class TestExportProvider(
        BaseEndpointInstancesProvider,
        BaseEndpointSourceOptionsProvider,
        BaseUpdateSourceReplicaProvider,
        BaseReplicaExportProvider,
        BaseReplicaExportValidationProvider):
    """Source-side provider backed by a local `scsi_debug` block device.

    ``connection_info`` (the source endpoint's connection info) has the form::

        {
            "block_device_path": "/dev/sdX",           # source block device
            "pkey_path":         "/root/.ssh/id_rsa",  # key for localhost SSH
        }
    """

    platform = "test-src"

    def __init__(self, event_handler):
        self._event_handler = event_handler

    def _event_manager(self):
        return events.EventManager(self._event_handler)

    def _make_replicator(self, pkey_path, event_mgr, volumes_info, repl_state):
        # TODO(claudiub): Use containers instead of using 127.0.0.1.
        pkey = paramiko.RSAKey.from_private_key_file(pkey_path)
        conn_info = {
            "ip": "127.0.0.1",
            "username": "root",
            "pkey": pkey,
        }
        return replicator_module.Replicator(
            conn_info, event_mgr, volumes_info, repl_state)

    # BaseProvider / BaseEndpointProvider

    def get_connection_info_schema(self):
        return {
            "type": "object",
            "properties": {
                "block_device_path": {"type": "string"},
                "pkey_path": {"type": "string"},
            },
            "required": ["block_device_path", "pkey_path"],
        }

    def validate_connection(self, ctxt, connection_info):
        block_device_path = connection_info["block_device_path"]
        if not os.path.exists(block_device_path):
            raise ValueError("Source device not found: %s" % block_device_path)
        pkey_path = connection_info["pkey_path"]
        if not os.path.exists(pkey_path):
            raise ValueError("SSH private key not found: %s" % pkey_path)

    # BaseExportInstanceProvider

    def get_source_environment_schema(self):
        return {"type": "object", "properties": {}}

    # BaseEndpointInstancesProvider

    def get_instances(self, ctxt, connection_info, source_environment,
                      limit=None, last_seen_id=None,
                      instance_name_pattern=None, refresh=False):
        return [self._instance_info(connection_info)]

    def get_instance(self, ctxt, connection_info, source_environment,
                     instance_name):
        return self._instance_info(connection_info)

    def _instance_info(self, connection_info):
        device = connection_info.get("device", "")
        name = os.path.basename(device) if device else "test-instance"
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
                "nics": [],
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
        """Return minimal export info describing the source block device."""
        block_device_path = connection_info["block_device_path"]
        size_bytes = _get_block_device_size(block_device_path)
        disk_id = os.path.basename(block_device_path)

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
                        "id": disk_id,
                        "format": "raw",
                        "size_bytes": size_bytes,
                    }
                ],
                "nics": [],
                "cdroms": [],
                "serial_ports": [],
                "floppies": [],
                "controllers": [],
            },
        }

    def deploy_replica_source_resources(
            self, ctxt, connection_info, export_info, source_environment):
        block_device_path = connection_info["block_device_path"]
        pkey_path = connection_info["pkey_path"]

        replicator = self._make_replicator(
            pkey_path, self._event_manager(), [], None)
        replicator.init_replicator()

        disk_id = os.path.basename(block_device_path)
        return {
            "connection_info": {
                "ip": "127.0.0.1",
                "port": 22,
                "username": "root",
                "pkey_path": pkey_path,
            },
            "migr_resources": {
                "disk_mappings": {disk_id: block_device_path},
            },
        }

    def delete_replica_source_resources(
            self, ctxt, connection_info, source_environment,
            migr_resources_dict):
        pkey_path = connection_info.get("pkey_path")
        if not pkey_path:
            return
        replicator = self._make_replicator(
            pkey_path, self._event_manager(), [], None)
        replicator.stop()

    def replicate_disks(
            self, ctxt, connection_info, source_environment, instance_name,
            source_resources, source_conn_info, target_conn_info,
            volumes_info, incremental):
        pkey_path = source_conn_info["pkey_path"]
        repl_state = _extract_repl_state(volumes_info) if incremental else None

        replicator = self._make_replicator(
            pkey_path, self._event_manager(), volumes_info, repl_state)
        replicator.init_replicator()
        replicator.wait_for_chunks()

        disk_mappings = source_resources.get("disk_mappings", {})
        source_volumes_info = [
            {
                "disk_id": vol["disk_id"],
                "disk_path": disk_mappings.get(vol["disk_id"], vol["disk_id"]),
            }
            for vol in volumes_info
        ]

        backup_writer = backup_writers.BackupWritersFactory(
            target_conn_info, volumes_info).get_writer()

        replicator.replicate_disks(source_volumes_info, backup_writer)
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
