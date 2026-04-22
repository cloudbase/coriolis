# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Integration test utils.
"""

import json
import os
import socket
import subprocess
import tempfile
import time

from oslo_log import log as logging
import paramiko

from coriolis import utils as coriolis_utils

LOG = logging.getLogger(__name__)

_SETTLE_TIMEOUT = 15
_POLL_INTERVAL = 1

# Sysfs knob for adding / removing scsi_debug hosts. Writing "1" adds a new
# host with its own independent backing store (requires per_host_store=1);
# writing "-1" removes the most-recently added host (LIFO).
_SCSI_DEBUG_ADD_HOST = "/sys/bus/pseudo/drivers/scsi_debug/add_host"

DATA_MINION_IMAGE = "coriolis-data-minion:test"


def _lsblk_disk_names() -> set:
    """Return the set of disk-type block device names visible to lsblk."""
    result = _run(["lsblk", "-Jb", "-o", "NAME,TYPE"], check=False)
    if result.returncode != 0:
        return set()

    data = json.loads(result.stdout)
    return {
        d["name"] for d in data.get("blockdevices", [])
        if d["type"] == "disk"
    }


def _poll_for_new_disks(before, count, timeout=_SETTLE_TIMEOUT):
    """Block until *count* new disk names appear beyond *before*.

    :returns: sorted list of new names.
    :raises: ``AssertionError`` on timeout.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        subprocess.call(["udevadm", "settle"])
        new = sorted(_lsblk_disk_names() - before)
        if len(new) >= count:
            return new[:count]
        time.sleep(_POLL_INTERVAL)
    raise AssertionError(
        "Only %d new disk(s) appeared within %ds (expected %d)"
        % (len(sorted(_lsblk_disk_names() - before)), timeout, count)
    )


def init_scsi_debug(size_mb=64):
    """Load scsi_debug with per_host_store=1.

    Must be called once per process before any ``add_scsi_debug_device``
    calls. With ``per_host_store=1`` every host added via the sysfs knob
    gets its own independent backing store, so devices never share storage.
    """
    _run([
        "modprobe",
        "scsi_debug",
        "per_host_store=1",
        "num_tgts=1",
        f"dev_size_mb={size_mb}",
    ])


def destroy_scsi_debug():
    """Unload the scsi_debug module."""
    _run(["modprobe", "-r", "scsi_debug"])


def add_scsi_debug_device() -> str:
    """Add one scsi_debug host and return its /dev/sdX path.

    Each call creates an independent backing store (per_host_store=1), so
    writing to one device is never visible through another.
    """
    before = _lsblk_disk_names()
    with open(_SCSI_DEBUG_ADD_HOST, "w") as fh:
        fh.write("1\n")

    new = _poll_for_new_disks(before, count=1)
    path = os.path.join("/dev", new[0])
    LOG.info("scsi_debug device added: %s", path)

    return path


def remove_scsi_debug_device():
    """Remove the most-recently added scsi_debug host."""
    with open(_SCSI_DEBUG_ADD_HOST, "w") as fh:
        fh.write("-1\n")


def write_test_pattern(device_path, chunk_size=4096):
    """Fill *device_path* with a repeating 4-byte test pattern.

    Returns the pattern bytes so callers can verify the destination later.
    The write is done with ``dd`` so it works on raw block devices.
    """
    pattern = b"\xde\xad\xbe\xef"
    # Write the pattern to a temp file, then dd it onto the device.
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name
        tmp.write(pattern * (chunk_size // len(pattern)))

    try:
        _run(
            ["dd", "if=%s" % tmp_path, "of=%s" % device_path,
             "bs=%d" % chunk_size, "conv=notrunc"],
        )
        _run(["sync"])
    finally:
        os.unlink(tmp_path)

    return pattern


def write_bytes_at_offset(device_path, offset, data):
    """Write *data* at *offset* bytes into *device_path*."""
    with open(device_path, "r+b") as fh:
        fh.seek(offset)
        fh.write(data)


def devices_match(path_a, path_b):
    """Return True if the contents of two block devices are identical."""
    result = _run(["cmp", "--silent", path_a, path_b], check=False)
    return result.returncode == 0


def _run(cmd, check=True):
    LOG.debug("Running: %s", " ".join(str(c) for c in cmd))
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=check,
    )


def wait_for_ssh(host, port, username, pkey_path, timeout=30):
    """Block until SSH on *host*:*port* accepts connections.

    :param host: hostname or IP
    :param port: SSH port
    :param username: SSH username
    :param pkey_path: path to the private key file
    :param timeout: seconds before raising AssertionError
    """
    pkey = paramiko.RSAKey.from_private_key_file(pkey_path)
    deadline = time.monotonic() + timeout
    last_exc = None
    while time.monotonic() < deadline:
        try:
            client = coriolis_utils.connect_ssh(
                host, port, username, pkey=pkey, connect_timeout=5)
            client.close()
            return
        except (paramiko.SSHException, socket.error, OSError) as exc:
            last_exc = exc
            time.sleep(1)
    raise AssertionError(
        "SSH %s@%s:%d not ready after %ds: %s" % (
            username, host, port, timeout, last_exc))


# Docker utils


def _start_container(image, name, extra_args=None):
    cmd = ["docker", "run", "--detach", "--name", name]
    if extra_args:
        cmd.extend(extra_args)
    cmd.append(image)
    result = _run(cmd)
    return result.stdout.decode().strip()


def start_container(
    image, name, is_systemd=False, ssh_key=None, volumes=None, devices=None,
    extra_args=None,
):
    """Start a detached Docker container and return its container ID.

    :param image: Docker image name / tag to run.
    :param name: Name to assign to the container.
    :param is_systemd: If the container is running systemd. If true, the
      necessary volumes, security opts, and caps are added for it to run.
    :param ssh_key: SSH key to add as a volume to the authorized_keys.
    :param volumes: List of volumes to attach to the container.
    :param devices: List of devices to attach to the container.
    :param extra_args: Optional list of extra ``docker run`` arguments.
    :returns: container ID string (stripped).
    """
    volumes = volumes or []
    devices = devices or []
    extra_args = extra_args or []
    sec_opts = []
    caps = []

    if is_systemd:
        volumes += ["/sys/fs/cgroup:/sys/fs/cgroup:rw"]
        sec_opts = ["apparmor=unconfined"]
        caps = ["SYS_ADMIN"]
        extra_args += ["--cgroupns=host"]

    if ssh_key:
        volumes = [f"{ssh_key}:/root/.ssh/authorized_keys:ro"] + volumes

    for volume in volumes:
        extra_args += ["--volume", volume]

    for device in devices:
        extra_args += ["--device", f"{device}:{device}"]

    for cap in caps:
        extra_args += ["--cap-add", cap]

    for sec_opt in sec_opts:
        extra_args += ["--security-opt", sec_opt]

    return _start_container(image, name, extra_args)


def stop_container(container_id):
    """Stop and remove a Docker container, ignoring errors.

    :param container_id: container ID or name to stop / remove.
    """
    _run(["docker", "stop", "--time", "5", container_id], check=False)
    _run(["docker", "rm", "--force", container_id], check=False)


def get_container_ip(container_id):
    """Return the first bridge-network IP address of *container_id*.

    :param container_id: container ID or name
    :returns: IP address string
    """
    result = _run(
        ["docker", "inspect", "--format",
         "{{.NetworkSettings.IPAddress}}",
         container_id])
    return result.stdout.decode().strip()
