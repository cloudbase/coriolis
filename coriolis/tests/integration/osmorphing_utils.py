# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
OS morphing integration test utilities.

LUKS / OS morphing helpers (loopback, LUKS, bootable VM disk setup)
"""

import contextlib
import os
import subprocess
import tempfile
from typing import Iterator

from oslo_log import log as logging

from coriolis.tests.integration.utils import _run

LOG = logging.getLogger(__name__)


# LUKS / OS Morphing utils


@contextlib.contextmanager
def mounted(device_path, read_only=False) -> Iterator[str]:
    """Mount *device_path* in a temporary directory, yield the mount point."""
    opts = ["-o", "ro"] if read_only else []

    with tempfile.TemporaryDirectory() as mount_point:
        _run(["mount"] + opts + [device_path, mount_point])
        try:
            yield mount_point
        finally:
            _run(["umount", mount_point])


def write_os_image_to_disk(device_path, container_image):
    """Write a real Linux rootfs to *device_path*.

    Exports the filesystem of a container image via ``docker export`` and
    extracts it onto an ext4-formatted device, giving a chroot-able root with
    that container OS' standard filesystem and binaries present.
    """
    _run(["mkfs.ext4", "-F", device_path])

    result = _run(["docker", "create", container_image])
    container_id = result.stdout.decode().strip()

    try:
        with mounted(device_path) as mount_point:
            export = subprocess.Popen(
                ["docker", "export", container_id],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            subprocess.run(
                ["tar", "-x", "-C", mount_point],
                stdin=export.stdout,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
            export.stdout.close()
            export.wait()

    finally:
        _run(["docker", "rm", "-f", container_id], check=False)


def _fixup_luks_inner_os(mapper_path, luks_uuid):
    """Patch the OS image inside a LUKS mapper to work with OS morphing.

    Docker container images are not full OS installs, so a few things need
    fixing before Coriolis can morph them:

    1. /etc/crypttab is missing: the LUKS mixin needs a UUID= entry there to
       configure initramfs auto-unlock.
    2. /boot may be absent (e.g. Rocky Linux 9 Docker image): the osmount
       root-finder requires etc, bin, sbin, and boot to all be present.
    """
    mapper_name = "luks-%s" % luks_uuid
    crypttab_entry = "%s\tUUID=%s\tnone\tluks\n" % (mapper_name, luks_uuid)

    with mounted(mapper_path) as mount_point:
        etc_dir = os.path.join(mount_point, "etc")
        os.makedirs(etc_dir, exist_ok=True)
        crypttab_path = os.path.join(etc_dir, "crypttab")

        with open(crypttab_path, "w") as fh:
            fh.write(crypttab_entry)

        os.makedirs(os.path.join(mount_point, "boot"), exist_ok=True)


def make_luks_device(device_path, key_file, container_image):
    """Format *device_path* with LUKS and write a minimal Linux OS inside.

    The mapper device is opened only for the duration of the call. It is closed
    before returning, leaving the raw device encrypted.

    Exports the filesystem the container image onto the given device, then
    writes a /etc/crypttab entry so that the LUKS mixin can find the UUID
    when configuring initramfs auto-unlock during OS morphing.
    """
    _run([
        "cryptsetup", "luksFormat", "--batch-mode", "--key-file", key_file,
        device_path,
    ])

    luks_uuid = _run(
        ["cryptsetup", "luksUUID", device_path]).stdout.decode().strip()

    with luks_open(device_path, key_file) as mapper_path:
        write_os_image_to_disk(mapper_path, container_image)
        _fixup_luks_inner_os(mapper_path, luks_uuid)


@contextlib.contextmanager
def luks_open(device_path, key_file):
    mapper_name = "coriolis_luks_setup_%s" % os.path.basename(device_path)
    _run([
        "cryptsetup", "luksOpen", "--key-file", key_file, device_path,
        mapper_name,
    ])

    try:
        yield "/dev/mapper/%s" % mapper_name
    finally:
        _run(["cryptsetup", "luksClose", mapper_name])


def path_exists_on_device(device_path, rel_path):
    """Checks if *rel_path* exists on the filesystem of *device_path*.

    Uses lexists so dangling symlinks (e.g. absolute targets only valid inside
    the OS, not on the host) are still reported as present.
    """
    with mounted(device_path, read_only=True) as mount_point:
        return os.path.lexists(os.path.join(mount_point, rel_path))


def read_file_from_device(device_path, rel_path):
    """Retrieves the specified file from the filesystem of *device_path*.

    Mounts the device read-only into a temporary directory, reads the file,
    then unmounts.
    """
    with mounted(device_path, read_only=True) as mount_point:
        with open(os.path.join(mount_point, rel_path)) as f:
            return f.read()


def list_files_from_device(device_path, rel_path):
    """Enumerates files from the filesystem of *device_path*.

    Mounts the device read-only into a temporary directory, enumerates files,
    then unmounts.
    """
    with mounted(device_path, read_only=True) as mount_point:
        return os.listdir(os.path.join(mount_point, rel_path))
