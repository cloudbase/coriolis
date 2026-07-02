# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
OS morphing integration test utilities.

LUKS / OS morphing helpers (loopback, LUKS, bootable VM disk setup)
"""

import contextlib
import json
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
        "cryptsetup", "luksFormat", "--batch-mode", "--type", "luks2",
        "--key-file", key_file, device_path,
    ])

    luks_uuid = get_luks_uuid(device_path)

    with luks_open(device_path, key_file) as mapper_path:
        write_os_image_to_disk(mapper_path, container_image)
        _fixup_luks_inner_os(mapper_path, luks_uuid)


def get_luks_uuid(device_path):
    """Return the LUKS UUID of `device_path`."""
    result = _run(["cryptsetup", "luksUUID", device_path])
    return result.stdout.decode().strip()


@contextlib.contextmanager
def luks_open(device_path, key_file, disable_keyring=False):
    mapper_name = "coriolis_luks_setup_%s" % os.path.basename(device_path)
    cmd = ["cryptsetup", "luksOpen", "--key-file", key_file]
    if disable_keyring:
        cmd.append("--disable-keyring")
    cmd += [device_path, mapper_name]

    _run(cmd)

    try:
        yield "/dev/mapper/%s" % mapper_name
    finally:
        _run(["cryptsetup", "luksClose", mapper_name])


def luks_can_open(device_path, key_file):
    """Return True if `key_file` can unlock `device_path`, False otherwise."""
    result = _run([
        "cryptsetup", "luksOpen", "--test-passphrase",
        "--key-file", key_file, device_path,
    ], check=False)

    return result.returncode == 0


def luks_add_tpm2_token(device_path, keyslot_id):
    """Inject a systemd-tpm2 token into device_path pointing at keyslot_id.

    The token contains no real TPM material, it exists only so the LUKS2
    header reports a systemd-tpm2 token, letting tests verify that Coriolis
    removes it during OS morphing.
    """
    token = json.dumps(
        {"type": "systemd-tpm2", "keyslots": [str(keyslot_id)]})
    # --disable-external-tokens bypasses the libcryptsetup-token-systemd-tpm2
    # plugin that validates real systemd-tpm2 token fields (tpm2-pcrs, blob,
    # etc.). Our token is intentionally minimal, just enough for the test to
    # verify Coriolis removes it during OS morphing.
    subprocess.run(
        [
            "cryptsetup", "token", "import",
            "--disable-external-tokens", device_path,
        ],
        input=token.encode(),
        stdout=subprocess.DEVNULL,
        check=True,
    )


def luks_has_tpm2_token(device_path):
    """Return True if `device_path` has any systemd-tpm2 LUKS2 tokens."""
    result = _run(["cryptsetup", "luksDump", device_path], check=False)
    return result.returncode == 0 and b"systemd-tpm2" in result.stdout


def write_file_on_luks_device(device_path, key_file, rel_path, content):
    """Write content into the given rel_path on the given LUKS device.

    Open `device_path` with `key_file`, mount it, and write `content` to
    `rel_path` inside the filesystem, creating parent directories as needed.
    """
    with luks_open(device_path, key_file) as mapper_path:
        with mounted(mapper_path) as mount_point:
            full_path = os.path.join(mount_point, rel_path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "w") as fh:
                fh.write(content)


def read_file_from_luks_device(device_path, key_file, rel_path):
    """Read the rel_path file from the given LUKS device.

    Open `device_path` with `key_file`, mount read-only, and return the
    contents of `rel_path`, or None if the path does not exist.
    """
    with luks_open(device_path, key_file) as mapper_path:
        with mounted(mapper_path, read_only=True) as mount_point:
            full_path = os.path.join(mount_point, rel_path)
            if not os.path.exists(full_path):
                return None
            with open(full_path) as fh:
                return fh.read()


def luks_add_key(device_path, existing_key_file, new_key_file):
    """Add a new keyslot to `device_path` using `existing_key_file` to auth."""
    _run([
        "cryptsetup", "luksAddKey",
        "--pbkdf-memory", "65536",
        "--key-file", existing_key_file,
        device_path, new_key_file,
    ])


def luks_add_key_from_mapper(mapper_path, device_path, new_key_file):
    """Add a new keyslot to `device_path` to an open dm-crypt device.

    Does not require the original passphrase, the master key is extracted
    directly from the live, already mounted device via dmsetup.
    """
    mapper_name = os.path.basename(mapper_path)
    result = _run(["dmsetup", "table", "--showkeys", mapper_name])
    fields = result.stdout.decode().split()

    # dmsetup may prefix the line with "<name>: "; detect by trailing colon.
    offset = 1 if fields[0].endswith(':') else 0

    # dm-crypt table: <start> <end> crypt <cipher> <key> ...
    master_key_hex = fields[offset + 4]

    with tempfile.NamedTemporaryFile(delete=False, suffix=".key") as fh:
        master_key_path = fh.name
        fh.write(bytes.fromhex(master_key_hex))

    try:
        _run([
            "cryptsetup", "luksAddKey",
            "--pbkdf-memory", "65536",
            "--master-key-file", master_key_path,
            device_path, new_key_file,
        ])
    finally:
        os.unlink(master_key_path)


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
