# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
SSH helpers for test providers which need to run commands inside test VMs.
"""

import collections

import paramiko
from oslo_log import log as logging

from coriolis import utils

LOG = logging.getLogger(__name__)

# Default guest login, matching the well-known Cirros test image. Can be overridden
# through ``source.guest_ssh`` in providers.yaml.
DEFAULT_GUEST_SSH = {"username": "cirros", "password": "gocubsgo"}

ExecResult = collections.namedtuple("ExecResult", ["returncode", "stdout", "stderr"])


def get_guest_ssh_config(source_config: dict) -> dict:
    """Return the guest SSH login: the defaults, updated by ``guest_ssh``.

    Recognized keys are ``username``, ``password``, ``pkey_path`` and ``port``.
    """
    return {**DEFAULT_GUEST_SSH, **source_config.get("guest_ssh", {})}


def exec_ssh_command(
    ip: str, command: str, guest_ssh: dict, connect_attempts: int = 10
) -> ExecResult:
    """Run *command* on *ip* over SSH, waiting for the guest's SSH port to open.

    *guest_ssh* is a dict as returned by ``get_guest_ssh_config``.
    """
    port = guest_ssh.get("port", 22)
    utils.wait_for_port_connectivity(ip, port)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        utils.retry_on_error(max_attempts=connect_attempts, sleep_seconds=5)(
            ssh.connect
        )(
            hostname=ip,
            port=port,
            username=guest_ssh["username"],
            password=guest_ssh.get("password"),
            key_filename=guest_ssh.get("pkey_path"),
            timeout=10,
        )
        _, stdout, stderr = ssh.exec_command(command)
        returncode = stdout.channel.recv_exit_status()
        return ExecResult(returncode, stdout.read().decode(), stderr.read().decode())
    finally:
        ssh.close()
