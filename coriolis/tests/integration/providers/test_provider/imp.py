# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Import-side (destination) implementation of the test provider.

Uses HTTPBackupWriterBootstrapper (via SSH to a Docker data-minion container)
to deploy and manage the coriolis-writer service and provides the
target_conn_info that BackupWritersFactory expects.
"""

import os
import uuid

from oslo_log import log as logging
import paramiko

from coriolis.providers import backup_writers
from coriolis.providers.base import BaseEndpointDestinationOptionsProvider
from coriolis.providers.base import BaseEndpointNetworksProvider
from coriolis.providers.base import BaseEndpointProvider
from coriolis.providers.base import BaseEndpointStorageProvider
from coriolis.providers.base import BaseReplicaImportProvider
from coriolis.providers.base import BaseReplicaImportValidationProvider
from coriolis.providers.base import BaseUpdateDestinationReplicaProvider
from coriolis.tests.integration import utils as test_utils

LOG = logging.getLogger(__name__)

# Port used by the test writer binary inside the container.
WRITER_TEST_PORT = 6677


class TestImportProvider(
        BaseEndpointProvider,
        BaseEndpointDestinationOptionsProvider,
        BaseEndpointNetworksProvider,
        BaseEndpointStorageProvider,
        BaseUpdateDestinationReplicaProvider,
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

    # BaseEndpointDestinationOptionsProvider

    def get_target_environment_options(
            self, ctxt, connection_info, env=None, option_names=None):
        return [
            {
                "name": "dest_opt",
                "values": ["foo", "lish"],
                "config_default": "foo",
            },
        ]

    # BaseEndpointNetworksProvider

    def get_networks(self, ctxt, connection_info, env):
        return [{"id": "test-net-1", "name": "test-net-1"}]

    # BaseEndpointStorageProvider

    def get_storage(self, ctxt, connection_info, target_environment):
        return {
            "storage_backends": [{"id": "test-store", "name": "test-store"}],
        }

    # BaseUpdateDestinationReplicaProvider

    def check_update_destination_environment_params(
            self, ctxt, connection_info, export_info, volumes_info,
            old_params, new_params):
        return volumes_info

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
        dest_devices = [vol["volume_dev"] for vol in volumes_info]
        container_name = "coriolis-writer-%s" % uuid.uuid4().hex[:8]

        container_id = test_utils.start_container(
            test_utils.DATA_MINION_IMAGE,
            container_name,
            is_systemd=True,
            ssh_key=f"{pkey_path}.pub",
            devices=dest_devices,
        )

        try:
            container_ip = test_utils.get_container_ip(container_id)
            test_utils.wait_for_ssh(container_ip, 22, "root", pkey_path)

            pkey = paramiko.RSAKey.from_private_key_file(pkey_path)
            ssh_conn_info = {
                "ip": container_ip,
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
                "migr_resources": {"container_id": container_id},
            }
        except Exception:
            test_utils.stop_container(container_id)
            raise

    def delete_replica_target_resources(
            self, ctxt, connection_info, target_environment,
            migr_resources_dict):
        container_id = (migr_resources_dict or {}).get("container_id")
        if container_id:
            test_utils.stop_container(container_id)

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
