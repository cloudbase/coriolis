# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Shared functionality between the import and export test providers.
"""

import os
import uuid

import paramiko

from coriolis import utils as coriolis_utils
from coriolis.tests.integration import utils as test_utils


class TestProviderMixin:
    """Shared provider methods between TestImportProvider and TestExportProvider."""

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

    def _create_minion(
        self,
        name_prefix,
        connection_info,
        devices=None,
        volumes=None,
        device_cgroup_rules=None,
    ):
        """Create a data-minion container and return its SSH connection info."""
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

            return {
                "container_id": container_id,
                "ssh_connection_info": ssh_conn_info,
            }
        except Exception:
            test_utils.remove_container(container_id)
            raise

    # BaseSourceMinionPoolProvider / BaseDestinationMinionPoolProvider

    def validate_minion_compatibility_for_transfer(
        self, ctxt, connection_info, export_info, environment_options, minion_properties
    ):
        pass

    def validate_minion_pool_environment_options(
        self, ctxt, connection_info, environment_options
    ):
        pass

    def set_up_pool_shared_resources(
        self, ctxt, connection_info, environment_options, pool_identifier
    ):
        return {}

    def tear_down_pool_shared_resources(
        self, ctxt, connection_info, environment_options, pool_shared_resources
    ):
        pass

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
        self,
        ctxt,
        connection_info,
        minion_properties,
        minion_connection_info,
        volumes_info,
    ):
        container_id = minion_properties["container_id"]

        for vol in volumes_info:
            if "volume_dev" in vol:
                # Destination side: the device was already resolved by
                # deploy_replica_disks, or left empty for a shared disk owned by another
                # instance of a clustered transfer, in which case there is nothing to
                # attach here.
                device_path = vol["volume_dev"]
                if not device_path:
                    continue
            else:
                # Source side: derive it from the disk_id.
                device_path = "/dev/%s" % vol["disk_id"]

            test_utils.hotplug_device_to_container(container_id, device_path)
            vol["volume_dev"] = device_path

        return {
            "minion_properties": minion_properties,
            "volumes_info": volumes_info,
        }

    def detach_volumes_from_minion(
        self,
        ctxt,
        connection_info,
        minion_properties,
        minion_connection_info,
        volumes_info,
    ):
        container_id = (minion_properties or {}).get("container_id")
        if not container_id:
            return

        for vol in volumes_info or []:
            dev_path = vol.get("volume_dev")
            if not dev_path:
                continue

            test_utils.unplug_device_from_container(container_id, dev_path)

        return {
            "minion_properties": minion_properties,
            "volumes_info": volumes_info,
        }

    def healthcheck_minion(
        self, ctxt, connection_info, minion_properties, minion_connection_info
    ):
        ip = minion_connection_info.get("ip")
        port = minion_connection_info.get("port", 22)
        username = minion_connection_info.get("username", "root")
        pkey = minion_connection_info.get("pkey")

        client = coriolis_utils.connect_ssh(ip, port, username, pkey=pkey)
        client.close()
