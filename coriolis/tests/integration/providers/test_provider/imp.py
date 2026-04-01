# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Import-side (destination) implementation of the test provider.

Uses HTTPBackupWriterBootstrapper (via SSH to 127.0.0.1) to deploy and manage
the coriolis-writer service and provides the target_conn_info that
BackupWritersFactory expects.
"""

import os

from oslo_log import log as logging
import paramiko

from coriolis.providers import backup_writers
from coriolis.providers.base import BaseEndpointProvider
from coriolis.providers.base import BaseReplicaImportProvider
from coriolis.providers.base import BaseReplicaImportValidationProvider
from coriolis import utils

LOG = logging.getLogger(__name__)

# Port used by the test writer binary. Chosen to avoid collision with the
# production default (6677).
WRITER_TEST_PORT = 16677


class TestImportProvider(
        BaseEndpointProvider,
        BaseReplicaImportProvider,
        BaseReplicaImportValidationProvider):
    """Destination-side provider backed by a local `scsi_debug` block device.

    ``connection_info`` (the destination endpoint's connection info) has the
    form::

        {
            "devices":   ["/dev/sdY", ...],   # pre-allocated destination devs
            "pkey_path": "/root/.ssh/id_rsa", # key for localhost SSH
        }
    """

    platform = "test-dest"

    def __init__(self, event_handler):
        self._event_handler = event_handler

    # BaseProvider / BaseEndpointProvider

    def get_connection_info_schema(self):
        return {
            "type": "object",
            "properties": {
                "devices": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "pkey_path": {"type": "string"},
            },
            "required": ["devices", "pkey_path"],
        }

    def validate_connection(self, ctxt, connection_info):
        for dev in connection_info.get("devices", []):
            if not os.path.exists(dev):
                raise ValueError("Destination device not found: %s" % dev)
        pkey_path = connection_info["pkey_path"]
        if not os.path.exists(pkey_path):
            raise ValueError("SSH private key not found: %s" % pkey_path)

    # BaseImportInstanceProvider

    def get_target_environment_schema(self):
        return {"type": "object", "properties": {}}

    # BaseReplicaImportProvider

    def deploy_replica_disks(
            self, ctxt, connection_info, target_environment, instance_name,
            export_info, volumes_info):
        """Map each source disk in export_info to a destination device.

        Returns a volumes_info list where each entry has ``disk_id`` (from
        the source) and ``volume_dev`` (the destination block device path).
        """
        dest_devices = list(connection_info["devices"])
        src_disks = export_info.get("devices", {}).get("disks", [])

        if len(src_disks) > len(dest_devices):
            raise ValueError(
                "Not enough destination devices (%d) for %d source disks"
                % (len(dest_devices), len(src_disks))
            )

        result = []
        for i, disk in enumerate(src_disks):
            result.append({
                "disk_id": disk["id"],
                "volume_dev": dest_devices[i],
            })

        return result

    def deploy_replica_target_resources(
            self, ctxt, connection_info, target_environment, volumes_info):
        pkey_path = connection_info["pkey_path"]
        pkey = paramiko.RSAKey.from_private_key_file(pkey_path)
        ssh_conn_info = {
            "ip": "127.0.0.1",
            "port": 22,
            "username": "root",
            "pkey": pkey,
        }

        bootstrapper = backup_writers.HTTPBackupWriterBootstrapper(
            ssh_conn_info, WRITER_TEST_PORT)
        writer_conn_details = bootstrapper.setup_writer()

        return {
            "volumes_info": volumes_info,
            "connection_info": {
                "backend": "http_backup_writer",
                "connection_details": writer_conn_details,
            },
            "migr_resources": {},
        }

    def delete_replica_target_resources(
            self, ctxt, connection_info, target_environment,
            migr_resources_dict):
        pkey_path = connection_info.get("pkey_path")
        if not pkey_path:
            return
        ssh = _ssh_connect(pkey_path)
        try:
            utils.stop_service(
                ssh, backup_writers._CORIOLIS_HTTP_WRITER_CMD)
        finally:
            ssh.close()

    def delete_replica_disks(
            self, ctxt, connection_info, target_environment, volumes_info):
        # scsi_debug devices are managed externally; nothing to delete here.
        return volumes_info

    def create_replica_disk_snapshots(
            self, ctxt, connection_info, target_environment, volumes_info):
        # scsi_debug has no snapshot support.
        return volumes_info

    def delete_replica_target_disk_snapshots(
            self, ctxt, connection_info, target_environment, volumes_info):
        return volumes_info

    def restore_replica_disk_snapshots(
            self, ctxt, connection_info, target_environment, volumes_info):
        return volumes_info

    def deploy_replica_instance(
            self, ctxt, connection_info, target_environment, instance_name,
            export_info, volumes_info, clone_disks):
        return {"instance_deployment_info": {}}

    def finalize_replica_instance_deployment(
            self, ctxt, connection_info, target_environment,
            instance_deployment_info):
        return {
            "id": "test-instance",
            "name": "test-instance",
            "num_cpu": 1,
            "memory_mb": 512,
            "os_type": "linux",
            "nested_virtualization": False,
            "devices": {
                "disks": [],
                "cdroms": [],
                "nics": [],
                "serial_ports": [],
                "floppies": [],
                "controllers": [],
            },
        }

    def cleanup_failed_replica_instance_deployment(
            self, ctxt, connection_info, target_environment,
            instance_deployment_info):
        pass

    # BaseInstanceProvider

    def get_os_morphing_tools(self, os_type, osmorphing_info):
        return []

    # BaseImportInstanceProvider

    def deploy_os_morphing_resources(
            self, ctxt, connection_info, target_environment,
            instance_deployment_info):
        return {}

    def delete_os_morphing_resources(
            self, ctxt, connection_info, target_environment,
            os_morphing_resources):
        pass

    # BaseReplicaImportValidationProvider

    def validate_replica_import_input(
            self, ctxt, connection_info, target_environment, export_info,
            check_os_morphing_resources=False, check_final_vm_params=False):
        return {}

    def validate_replica_deployment_input(
            self, ctxt, connection_info, target_environment, export_info):
        return {}


# Helpers
def _ssh_connect(pkey_path):
    pkey = paramiko.RSAKey.from_private_key_file(pkey_path)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="127.0.0.1", username="root", pkey=pkey)
    return ssh


def _read_file(path):
    """Return the contents of *path* as a string."""
    with open(path) as fh:
        return fh.read()
