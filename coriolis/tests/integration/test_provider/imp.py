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
from coriolis.providers.base import BaseDestinationMinionPoolProvider
from coriolis.providers.base import BaseEndpointDestinationOptionsProvider
from coriolis.providers.base import BaseEndpointNetworksProvider
from coriolis.providers.base import BaseEndpointProvider
from coriolis.providers.base import BaseEndpointStorageProvider
from coriolis.providers.base import BaseReplicaImportProvider
from coriolis.providers.base import BaseReplicaImportValidationProvider
from coriolis.providers.base import BaseUpdateDestinationReplicaProvider
from coriolis.tests.integration.test_provider import osmorphing
from coriolis.tests.integration import utils as test_utils
from coriolis import utils as coriolis_utils

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
        BaseReplicaImportValidationProvider,
        BaseDestinationMinionPoolProvider):
    """Destination-side provider backed by a local `scsi_debug` block device.

    ``connection_info`` (the destination endpoint's connection info) has the
    form::

        {
            "pkey_path": "/root/.ssh/id_rsa",  # key for localhost SSH
        }

    ``target_environment`` (per-transfer destination settings) has the form::

        {
            "devices": ["/dev/sdY", ...],  # pre-allocated destination devs
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
                "pkey_path": {"type": "string"},
                "role": {"type": "string"},
            },
            "required": ["pkey_path"],
        }

    def validate_connection(self, ctxt, connection_info):
        pkey_path = connection_info["pkey_path"]
        if not os.path.exists(pkey_path):
            raise ValueError("SSH private key not found: %s" % pkey_path)

    # BaseImportInstanceProvider

    def get_target_environment_schema(self):
        return {
            "type": "object",
            "properties": {
                "devices": {
                    "type": "array",
                    "items": {"type": "string"},
                },
            },
            "required": [],
        }

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
        dest_devices = list(target_environment["devices"])
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
        devices = [vol["volume_dev"] for vol in volumes_info]
        result = self._create_minion(
            "coriolis-writer", connection_info, devices)

        return {
            "volumes_info": volumes_info,
            "connection_info": result["backup_writer_connection_info"],
            "migr_resources": {"container_id": result["container_id"]},
        }

    def _create_minion(
            self, name_prefix, connection_info, devices=None, volumes=None,
            device_cgroup_rules=None, setup_writer=True):
        pkey_path = connection_info["pkey_path"]
        container_name = "%s-%s" % (name_prefix, uuid.uuid4().hex[:8])

        container_id = test_utils.run_container(
            test_utils.DATA_MINION_IMAGE,
            container_name,
            is_systemd=True,
            ssh_key=f"{pkey_path}.pub",
            devices=devices,
            volumes=volumes,
            device_cgroup_rules=device_cgroup_rules,
        )

        try:
            container_ip = test_utils.get_container_ip(container_id)
            test_utils.wait_for_ssh(container_ip, 22, "root", pkey_path)

            pkey = paramiko.RSAKey.from_private_key_file(pkey_path)
            ssh_conn_info = {
                "ip": container_ip,
                "port": 22,
                "username": "root",
                "pkey": coriolis_utils.serialize_key(pkey),
            }

            info = {
                "container_id": container_id,
                "ssh_connection_info": ssh_conn_info,
            }
            if setup_writer:
                bootstrapper = backup_writers.HTTPBackupWriterBootstrapper(
                    ssh_conn_info, WRITER_TEST_PORT)
                writer_conn_details = bootstrapper.setup_writer()
                info["backup_writer_connection_info"] = {
                    "backend": "http_backup_writer",
                    "connection_details": writer_conn_details,
                }

            return info
        except Exception:
            test_utils.remove_container(container_id)
            raise

    def delete_replica_target_resources(
            self, ctxt, connection_info, target_environment,
            migr_resources_dict):
        container_id = (migr_resources_dict or {}).get("container_id")
        if container_id:
            test_utils.remove_container(container_id)

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
        return osmorphing.OS_MORPHERS

    # BaseImportInstanceProvider

    def deploy_os_morphing_resources(
            self, ctxt, connection_info, target_environment,
            instance_deployment_info):
        devices = list(target_environment.get("devices", []))

        # lsblk inside the container sees all the host block devices because
        # Docker containers share the host kernel's sysfs (/sys/block/).
        # Populate ignore_devices with every host disk except the target
        # so osmorphing only considers the devices we actually attached.
        ignore_devices = list(
            test_utils.get_host_disk_devices() - set(devices)
        )

        # Mount the host's /lib/modules tree so that modprobe can
        # resolve built-in modules.
        volumes = ["/lib/modules:/lib/modules:ro"]
        result = self._create_minion(
            "coriolis-osmorphing", connection_info, devices,
            volumes, setup_writer=False,
        )

        return {
            "os_morphing_resources": {"container_id": result["container_id"]},
            "osmorphing_connection_info": result["ssh_connection_info"],
            "osmorphing_info": {
                "os_type": instance_deployment_info.get("os_type", "linux"),
                "ignore_devices": ignore_devices,
            },
        }

    def delete_os_morphing_resources(
            self, ctxt, connection_info, target_environment,
            os_morphing_resources):
        if os_morphing_resources:
            container_id = os_morphing_resources.get("container_id")
            if container_id:
                test_utils.remove_container(container_id)

    # BaseReplicaImportValidationProvider

    def validate_replica_import_input(
            self, ctxt, connection_info, target_environment, export_info,
            check_os_morphing_resources=False, check_final_vm_params=False):
        return {}

    def validate_replica_deployment_input(
            self, ctxt, connection_info, target_environment, export_info):
        return {}

    # BaseDestinationMinionPoolProvider

    def get_minion_pool_environment_schema(self):
        return self.get_target_environment_schema()

    def get_minion_pool_options(
            self, ctxt, connection_info, env=None, option_names=None):
        return self.get_target_environment_options(
            ctxt, connection_info, env, option_names)

    def validate_minion_compatibility_for_transfer(
            self, ctxt, connection_info, export_info, environment_options,
            minion_properties):
        pass

    def validate_minion_pool_environment_options(
            self, ctxt, connection_info, environment_options):
        pass

    def set_up_pool_shared_resources(
            self, ctxt, connection_info, environment_options, pool_identifier):
        return {}

    def tear_down_pool_shared_resources(
            self, ctxt, connection_info, environment_options,
            pool_shared_resources):
        pass

    def create_minion(
            self, ctxt, connection_info, environment_options, pool_identifier,
            pool_os_type, pool_shared_resources, new_minion_identifier):
        # Devices are hotplugged after container creation via mknod / nsenter.
        # We must pre-authorize all block devices through the
        # --device-cgroup-rule option, otherwise any device added will be
        # inaccessible ("operation not permitted" error on open).
        result = self._create_minion(
            "coriolis-pool-minion", connection_info, [],
            device_cgroup_rules=["b *:* rwm"])

        backup_writer_conn_info = result["backup_writer_connection_info"]
        return {
            "connection_info": result["ssh_connection_info"],
            "backup_writer_connection_info": backup_writer_conn_info,
            "minion_provider_properties": {
                "container_id": result["container_id"],
            },
        }

    def delete_minion(self, ctxt, connection_info, minion_properties):
        container_id = (minion_properties or {}).get("container_id")
        if container_id:
            test_utils.remove_container(container_id)

    def shutdown_minion(self, ctxt, connection_info, minion_properties):
        container_id = (minion_properties or {}).get("container_id")
        if container_id:
            test_utils.stop_container(container_id)

    def start_minion(self, ctxt, connection_info, minion_properties):
        container_id = (minion_properties or {}).get("container_id")
        if container_id:
            test_utils.start_container(container_id)

    def attach_volumes_to_minion(
            self, ctxt, connection_info, minion_properties,
            minion_connection_info, volumes_info):
        container_id = minion_properties["container_id"]
        for vol in volumes_info:
            device_path = vol["volume_dev"]
            test_utils.hotplug_device_to_container(container_id, device_path)

        return {
            "minion_properties": minion_properties,
            "volumes_info": volumes_info,
        }

    def detach_volumes_from_minion(
            self, ctxt, connection_info, minion_properties,
            minion_connection_info, volumes_info):
        container_id = (minion_properties or {}).get("container_id")
        if not container_id:
            return

        for vol in (volumes_info or []):
            dev_path = vol.get("volume_dev")
            if not dev_path:
                continue

            test_utils.unplug_device_from_container(container_id, dev_path)

        return {
            "minion_properties": minion_properties,
            "volumes_info": volumes_info,
        }

    def healthcheck_minion(
            self, ctxt, connection_info, minion_properties,
            minion_connection_info):
        ip = minion_connection_info.get("ip")
        port = minion_connection_info.get("port", 22)
        username = minion_connection_info.get("username", "root")
        pkey = minion_connection_info.get("pkey")

        client = coriolis_utils.connect_ssh(ip, port, username, pkey=pkey)
        client.close()

    def validate_osmorphing_minion_compatibility_for_transfer(
            self, ctxt, connection_info, export_info, environment_options,
            minion_properties):
        pass

    def get_additional_os_morphing_info(
            self, ctxt, connection_info, target_environment,
            instance_deployment_info):
        return {}
