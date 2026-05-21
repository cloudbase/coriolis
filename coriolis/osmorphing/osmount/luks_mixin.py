# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

import contextlib
import json
import os
import re

from oslo_log import log as logging

from coriolis import constants
from coriolis import exception
from coriolis import utils

LOG = logging.getLogger(__name__)

_LUKS_KEYFILE_DIR = "/etc/luks"
_DRACUT_LUKS_CONF_PATH = "/etc/dracut.conf.d/99-coriolis-luks.conf"

# cryptsetup loads TPM2 token plugins via dlopen, so dracut's ldd analysis
# misses them. List candidate paths in order of preference; the first one
# found in the guest OS will be added to install_items so dracut includes
# both the plugin and its libtss2 dependencies.
_CRYPTSETUP_TPM2_PLUGIN_PATHS = [
    "/usr/lib64/cryptsetup/libcryptsetup-token-systemd-tpm2.so",
    "/usr/lib/cryptsetup/libcryptsetup-token-systemd-tpm2.so",
    "/usr/lib/x86_64-linux-gnu/cryptsetup/libcryptsetup-token-systemd-tpm2.so",
]

_FIRSTBOOT_SCRIPT_PATH = "/usr/local/sbin/coriolis-luks-firstboot.sh"
_SYSTEMD_UNIT_PATH = "/etc/systemd/system/coriolis-luks-firstboot.service"

_RESOURCES_DIR = os.path.join(os.path.dirname(__file__), "resources")


def _load_script(filename):
    with open(os.path.join(_RESOURCES_DIR, filename)) as fh:
        return fh.read()


_LUKS_FIRSTBOOT_SCRIPTS = {
    "update-initramfs": _load_script("luks_firstboot_initramfs_tools.sh"),
    "dracut": _load_script("luks_firstboot_dracut.sh"),
}

_SYSTEMD_UNIT = """\
[Unit]
Description=Coriolis LUKS migration firstboot cleanup
After=local-fs.target
ConditionPathExists=/etc/luks

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/coriolis-luks-firstboot.sh
RemainAfterExit=yes
StandardOutput=journal+console
StandardError=journal+console

[Install]
WantedBy=multi-user.target
"""


class LinuxLUKSMixin:
    """Mixin providing LUKS-related methods for BaseLinuxOSMountTools.

    Expects the consuming class to provide ``self._ssh``, ``self._exec_cmd()``,
    and ``self._osmorphing_info``.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._luks_opened = []

    def _unlock_luks_devices(self, dev_paths):
        """Open any LUKS-encrypted devices listed in dev_paths in-place.

        Reads osmorphing_info["encrypted_disks_passphrase"], a single
        passphrase used for all LUKS-encrypted devices. Non-LUKS devices are
        silently skipped.

        `dev_paths` entries are replaced in-place with the resulting
        `/dev/mapper/<name>` path for any device that is unlocked.

        :returns: list of (mapper_name, original_dev_path) pairs opened,
          to be used for cleanup and further processing.
        """
        passphrase = self._osmorphing_info.get(constants.ENCRYPTED_DISKS_PASS)

        try:
            for i, dev_path in enumerate(dev_paths):
                mapper_path = self._unlock_luks_device(dev_path, passphrase)
                if mapper_path is None:
                    continue

                dev_paths[i] = mapper_path
                self._luks_opened.append(
                    (os.path.basename(mapper_path), dev_path))
        except Exception:
            self._close_luks_devices()
            raise

    def _unlock_luks_device(self, dev_path, passphrase):
        """Open a single LUKS-encrypted device.

        :raises CoriolisException: if the device is LUKS-encrypted but no
          passphrase was provided.
        :returns: the device mapper name, or None if the device is not LUKS
          and no passphrase was given.
        """
        is_luks = self._is_luks(dev_path)

        if is_luks and not passphrase:
            raise exception.CoriolisException(
                f"{dev_path} is LUKS-encrypted, but no passphrase is "
                "provided.")
        if not is_luks:
            LOG.debug(
                "Device '%s' is not a LUKS container; skipping.", dev_path)
            return None

        mapper_name = "coriolis_%s" % os.path.basename(dev_path)
        key_path = "/tmp/%s.key" % mapper_name
        with self._auth_luks(passphrase, key_path):
            self._exec_cmd(
                "sudo cryptsetup luksOpen --disable-keyring "
                "--key-file %s %s %s" % (key_path, dev_path, mapper_name))

        mapper_path = "/dev/mapper/%s" % mapper_name
        LOG.info("Unlocked LUKS device '%s' as '%s'", dev_path, mapper_path)

        return mapper_path

    def _is_luks(self, dev_path):
        try:
            self._exec_cmd("sudo cryptsetup isLuks %s" % dev_path)
            return True
        except exception.SSHCommandNotFoundException:
            LOG.warn("cryptsetup missing from OS morpher; cannot check if "
                     "device is LUKS-encrypted.")
        except Exception:
            # if it's not LUKS, we'll get exit code 1.
            # The exception is already logged in self._exec_cmd.
            pass

        return False

    def _close_luks_devices(self):
        """Close any LUKS mapper devices opened by _unlock_luks_devices."""
        for mapper_name, _ in self._luks_opened:
            self._exec_cmd(
                "sudo cryptsetup luksClose %s || true" % mapper_name)

        self._luks_opened = []

    def _write_remote_file(self, dest_path, content, mode=None):
        """Write content to dest_path on the remote host via a temp file."""
        tmp = self._exec_cmd("mktemp").strip()
        utils.write_ssh_file(self._ssh, tmp, content.encode("utf-8"))
        if mode is not None:
            self._exec_cmd(
                "sudo mv %s %s && sudo chmod %s %s" % (
                    tmp, dest_path, mode, dest_path))
        else:
            self._exec_cmd("sudo mv %s %s" % (tmp, dest_path))

    @contextlib.contextmanager
    def _auth_luks(self, passphrase, key_path):
        """Write passphrase to key_path on the remote host; yield; delete."""
        self._write_remote_file(key_path, passphrase)
        try:
            yield key_path
        finally:
            self._exec_cmd("sudo rm -f %s" % key_path)

    def _get_tpm2_token_info(self, dev_path):
        """Return list of (token_id, keyslot_id) pairs for systemd-tpm2 tokens.

        Returns an empty list for LUKS1 devices, which do not support tokens.
        """
        try:
            raw = self._exec_cmd(
                "sudo cryptsetup luksDump --dump-json-metadata %s" % dev_path)
            header = json.loads(raw)
        except Exception:
            LOG.warning(
                "Could not dump LUKS header for '%s': %s",
                dev_path, utils.get_exception_details())
            return []

        results = []
        for token_id, token in header.get("tokens", {}).items():
            if token.get("type") != "systemd-tpm2":
                continue

            for keyslot_id in token.get("keyslots", []):
                results.append((token_id, keyslot_id))

        return results

    def _remove_tpm2_tokens(self, dev_path, passphrase):
        """Remove systemd-tpm2 tokens and kill their keyslots from dev_path.

        Token removal modifies only the LUKS2 header metadata. Keyslot
        removal is authenticated via the migration passphrase.
        """
        token_info = self._get_tpm2_token_info(dev_path)
        if not token_info:
            return

        for token_id, keyslot_id in token_info:
            try:
                self._exec_cmd(
                    "sudo cryptsetup token remove --token-id %s %s" % (
                        token_id, dev_path))
                LOG.info(
                    "Removed systemd-tpm2 token %s from '%s'",
                    token_id, dev_path)
            except Exception:
                LOG.warning(
                    "Failed to remove TPM2 token %s from '%s': %s",
                    token_id, dev_path, utils.get_exception_details())
                continue

            key_path = "/tmp/coriolis_%s.key" % os.path.basename(dev_path)
            try:
                with self._auth_luks(passphrase, key_path):
                    self._exec_cmd(
                        "sudo cryptsetup luksKillSlot --key-file %s %s %s" % (
                            key_path, dev_path, keyslot_id))
                LOG.info(
                    "Killed TPM2 keyslot %s from '%s'", keyslot_id, dev_path)
            except Exception:
                LOG.warning(
                    "Failed to kill TPM2 keyslot %s from '%s': %s",
                    keyslot_id, dev_path, utils.get_exception_details())

    def _transform_crypttab(self, os_root_dir, transform):
        """Apply transform to each non-comment entry in /etc/crypttab.

        transform(parts) receives the split fields [name, device, keyfile,
        options] and returns a modified parts list to replace the line, or
        None to leave it unchanged.

        Returns True if the file was written back, False if nothing changed or
        the file does not exist.
        """
        crypttab_path = os.path.join(os_root_dir, "etc/crypttab")
        if not utils.test_ssh_path(self._ssh, crypttab_path):
            return False

        content = utils.read_ssh_file(self._ssh, crypttab_path).decode("utf-8")
        new_lines = []
        changed = False

        for line in content.splitlines(keepends=True):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                new_lines.append(line)
                continue

            parts = stripped.split(None, 3)
            new_parts = transform(parts)
            if new_parts is None:
                new_lines.append(line)
            else:
                new_lines.append("\t".join(new_parts) + "\n")
                changed = True

        if not changed:
            return False

        self._write_remote_file(crypttab_path, "".join(new_lines))
        return True

    def _remove_tpm2_crypttab_options(self, os_root_dir):
        """Strip tpm2-* options from /etc/crypttab in the mounted OS.

        Prevents the initramfs from attempting TPM2 unsealing on the target,
        which would fail because the source TPM is not present there.
        """
        def _strip_tpm2(parts):
            if len(parts) < 4:
                return None

            opts = [
                o for o in parts[3].split(",") if not o.startswith("tpm2-")
            ]
            new_opts = ",".join(opts)
            if new_opts == parts[3]:
                return None

            return [parts[0], parts[1], parts[2], new_opts]

        if self._transform_crypttab(os_root_dir, _strip_tpm2):
            LOG.info(
                "Removed TPM2 options from /etc/crypttab in '%s'", os_root_dir)

    def remove_encryption_artifacts(self, os_root_dir):
        """Remove stale TPM2 tokens, kill their keyslots, and strip tpm2-*
        options from /etc/crypttab.

        Called after OS morphing, before closing LUKS devices. The source
        TPM does not exist on the destination, leaving its token and crypttab
        options in place would cause the initramfs to hang or fail on first
        boot when it tries and fails to unseal the key.
        """
        if not self._luks_opened:
            return

        self._event_manager.progress_update(
            "Removing stale TPM2 LUKS artifacts")

        passphrase = self._osmorphing_info.get(constants.ENCRYPTED_DISKS_PASS)
        for _, dev_path in self._luks_opened:
            self._remove_tpm2_tokens(dev_path, passphrase)

        self._remove_tpm2_crypttab_options(os_root_dir)

    def _get_migration_keyfile_path(self, dev_path):
        return "%s/coriolis_%s.key" % (
            _LUKS_KEYFILE_DIR, os.path.basename(dev_path))

    def _get_luks_uuid(self, dev_path):
        return self._exec_cmd(
            "sudo cryptsetup luksUUID %s" % dev_path).strip()

    def _update_crypttab_keyfile(self, os_root_dir, uuid_to_keyfile):
        """Update the keyfile column in crypttab for matching LUKS UUIDs."""
        def _set_keyfile(parts):
            if len(parts) < 2:
                return None

            m = (re.match(r"UUID=([0-9a-f-]+)", parts[1], re.IGNORECASE) or
                 re.match(r".*/by-uuid/([0-9a-f-]+)", parts[1], re.IGNORECASE))
            if not m:
                return None

            keyfile = uuid_to_keyfile.get(m.group(1))
            if keyfile is None:
                return None

            while len(parts) < 4:
                parts.append("")

            parts[2] = keyfile
            # cryptsetup-initramfs (Ubuntu 22.04+) only embeds crypttab
            # entries in the initramfs when the device is verifiable at build
            # time OR when the 'initramfs' option is present. Inside a chroot
            # (no udev, no /dev/disk/by-uuid/), verification always fails, so
            # we force-add 'initramfs' here.
            opts_list = [o for o in parts[3].split(",") if o]
            if "initramfs" not in opts_list:
                opts_list.append("initramfs")

            parts[3] = ",".join(opts_list)

            return parts

        if not self._transform_crypttab(os_root_dir, _set_keyfile):
            raise exception.CoriolisException(
                "No /etc/crypttab entries matched LUKS UUIDs in '%s'; "
                "cannot configure initramfs auto-unlock." % os_root_dir)

        LOG.info("Updated crypttab keyfile entries in '%s'", os_root_dir)

    def _write_migration_keyfiles(self, os_root_dir):
        """Write migration keyfiles into the OS and update crypttab."""
        passphrase = self._osmorphing_info.get(constants.ENCRYPTED_DISKS_PASS)
        if not passphrase or not self._luks_opened:
            return

        keyfile_dir = os.path.join(os_root_dir, _LUKS_KEYFILE_DIR.lstrip("/"))
        self._exec_cmd(
            "sudo mkdir -p %s && sudo chmod 700 %s" % (
                keyfile_dir, keyfile_dir))

        uuid_to_keyfile = {}
        for _, dev_path in self._luks_opened:
            luks_uuid = self._get_luks_uuid(dev_path)

            keyfile_path = self._get_migration_keyfile_path(dev_path)
            abs_path = os.path.join(os_root_dir, keyfile_path.lstrip("/"))

            self._write_remote_file(abs_path, passphrase, mode="400")
            uuid_to_keyfile[luks_uuid] = keyfile_path
            LOG.info(
                "Written migration keyfile for LUKS device '%s' (UUID %s)",
                dev_path, luks_uuid)

        self._update_crypttab_keyfile(os_root_dir, uuid_to_keyfile)

        initramfs_tool = self._detect_initramfs_tool(os_root_dir)
        if initramfs_tool == "dracut":
            self._configure_dracut_keyfiles(os_root_dir, uuid_to_keyfile)
        elif initramfs_tool == "update-initramfs":
            self._configure_initramfs_tools_keyfiles(os_root_dir)
        else:
            raise exception.CoriolisException(
                "No initramfs tool found in OS at '%s'; cannot configure "
                "keyfile-based LUKS auto-unlock." % os_root_dir)

    def _configure_dracut_keyfiles(self, os_root_dir, uuid_to_keyfile):
        """Write a dracut.conf.d snippet to embed keyfiles in the initramfs."""
        install_items = list(uuid_to_keyfile.values())

        # cryptsetup loads TPM2 token plugins via dlopen; add it
        # explicitly so dracut includes it (and its libtss2 deps via
        # ldd analysis of the .so) in the initramfs.
        for plugin_path in _CRYPTSETUP_TPM2_PLUGIN_PATHS:
            if utils.test_ssh_path(
                    self._ssh,
                    os.path.join(os_root_dir, plugin_path.lstrip("/"))):
                install_items.append(plugin_path)
                LOG.debug(
                    "Including cryptsetup TPM2 token plugin in "
                    "dracut install_items: %s", plugin_path)
                break

        conf_abs = os.path.join(
            os_root_dir, _DRACUT_LUKS_CONF_PATH.lstrip("/"))
        conf_content = 'install_items+=" %s "\n' % " ".join(install_items)
        self._write_remote_file(conf_abs, conf_content)
        self._exec_cmd(
            "sudo chown root:root %s && sudo chmod 644 %s" % (
                conf_abs, conf_abs))
        LOG.info(
            "Written dracut LUKS keyfile config at '%s'",
            _DRACUT_LUKS_CONF_PATH)

    def _configure_initramfs_tools_keyfiles(self, os_root_dir):
        """Set KEYFILE_PATTERN in cryptsetup-initramfs conf-hook.

        cryptsetup-initramfs (Debian / Ubuntu) only embeds keyfiles whose paths
        match KEYFILE_PATTERN. Set it so migration keyfiles are included in the
        rebuilt initramfs.
        """
        hook_dir = os.path.join(os_root_dir, "etc/cryptsetup-initramfs")
        self._exec_cmd("sudo mkdir -p %s" % hook_dir)
        hook_abs = os.path.join(hook_dir, "conf-hook")
        pattern = "%s/coriolis_*.key" % _LUKS_KEYFILE_DIR

        existing = ""
        if utils.test_ssh_path(self._ssh, hook_abs):
            existing = utils.read_ssh_file(self._ssh, hook_abs).decode("utf-8")

        # Always append: the default conf-hook has #KEYFILE_PATTERN=
        # (commented out). Appending sets the active value. The last assignment
        # wins, overriding any earlier line.
        new_content = existing + '\nKEYFILE_PATTERN="%s"\n' % pattern
        self._write_remote_file(hook_abs, new_content)
        self._exec_cmd(
            "sudo chown root:root %s && sudo chmod 644 %s" % (
                hook_abs, hook_abs))
        LOG.info(
            "Set KEYFILE_PATTERN in cryptsetup-initramfs conf-hook at '%s'",
            hook_abs)

    def _detect_initramfs_tool(self, os_root_dir):
        initramfs_bins = ["usr/sbin/update-initramfs", "sbin/update-initramfs"]
        for initramfs_bin in initramfs_bins:
            path = os.path.join(os_root_dir, initramfs_bin)
            if utils.test_ssh_path(self._ssh, path):
                return "update-initramfs"

        for dracut_bin in ["usr/bin/dracut", "usr/sbin/dracut", "sbin/dracut"]:
            path = os.path.join(os_root_dir, dracut_bin)
            if utils.test_ssh_path(self._ssh, path):
                return "dracut"

        return None

    def _build_dracut_include_args(self, os_root_dir):
        """Return --include args that force-embed crypttab and LUKS keyfiles.

        dracut's install_items embeds the keyfile but does not guarantee that
        /etc/crypttab lands in the initramfs image. Without crypttab,
        systemd-cryptsetup-generator names the mapper luks-<UUID> instead of
        the crypttab mapper name and cannot find the keyfile.
        """
        args = []
        crypttab = os.path.join(os_root_dir, "etc/crypttab")
        if utils.test_ssh_path(self._ssh, crypttab):
            args += ["--include", "/etc/crypttab", "/etc/crypttab"]

        luks_dir = os.path.join(os_root_dir, _LUKS_KEYFILE_DIR.lstrip("/"))
        try:
            keyfiles = self._exec_cmd(
                "sudo find %s -name 'coriolis_*.key' -type f 2>/dev/null"
                % luks_dir).strip().splitlines()
        except Exception:
            keyfiles = []

        for kf in keyfiles:
            # strip os_root_dir prefix.
            rel = kf[len(os_root_dir):]
            args += ["--include", rel, rel]

        return args

    def _rebuild_initramfs(self, os_root_dir):
        """Rebuild the initramfs inside the mounted OS chroot.

        /dev, /proc, /sys, /run are already bind-mounted by mount_os().
        """
        tool = self._detect_initramfs_tool(os_root_dir)
        if tool == "update-initramfs":
            self._exec_cmd(
                "sudo chroot %s update-initramfs -u -k all" % os_root_dir)
        elif tool == "dracut":
            # --regenerate-all scans the chroot's own /lib/modules/ for
            # installed kernels instead of relying on uname -r
            #
            # Explicitly --include the crypttab and any LUKS migration keyfiles
            # so that systemd-cryptsetup-generator finds them in the initramfs
            # and uses the crypttab mapper name (luks-root) and keyfile for
            # auto-unlock. install_items in dracut.conf.d embeds the keyfile
            # but does NOT guarantee that crypttab ends up in the image.
            include_args = self._build_dracut_include_args(os_root_dir)
            self._exec_cmd(
                "sudo chroot %s dracut --regenerate-all --force %s"
                % (os_root_dir, " ".join(include_args)))
        else:
            raise exception.CoriolisException(
                "No initramfs tool found in OS at '%s'; cannot rebuild "
                "initramfs for LUKS auto-unlock." % os_root_dir)

    def _detect_init_system(self, os_root_dir):
        path = os.path.join(os_root_dir, "lib/systemd/systemd")
        if utils.test_ssh_path(self._ssh, path):
            return "systemd"

        path = os.path.join(os_root_dir, "sbin/openrc")
        if utils.test_ssh_path(self._ssh, path):
            return "openrc"

        path = os.path.join(os_root_dir, "sbin/initctl")
        if utils.test_ssh_path(self._ssh, path):
            return "upstart"

        return "sysvinit"

    def _register_firstboot_script_systemd(self, os_root_dir):
        unit_abs = os.path.join(os_root_dir, _SYSTEMD_UNIT_PATH.lstrip("/"))
        self._write_remote_file(unit_abs, _SYSTEMD_UNIT)
        self._exec_cmd(
            "sudo chown root:root %s && sudo chmod 644 %s" % (
                unit_abs, unit_abs))

        wants_dir = os.path.join(
            os_root_dir, "etc/systemd/system/multi-user.target.wants")
        self._exec_cmd("sudo mkdir -p %s" % wants_dir)
        self._exec_cmd(
            "sudo ln -sf %s %s/coriolis-luks-firstboot.service" % (
                _SYSTEMD_UNIT_PATH, wants_dir))

    def _install_luks_firstboot_script(self, os_root_dir):
        """Write firstboot cleanup script and register with the init system."""
        initramfs_tool = self._detect_initramfs_tool(os_root_dir)
        script_content = _LUKS_FIRSTBOOT_SCRIPTS.get(initramfs_tool)
        if script_content is None:
            raise exception.CoriolisException(
                "No initramfs tool found in OS at '%s'; cannot install "
                "LUKS firstboot cleanup script." % os_root_dir)

        script_abs = os.path.join(
            os_root_dir, _FIRSTBOOT_SCRIPT_PATH.lstrip("/"))
        self._exec_cmd("sudo mkdir -p %s" % os.path.dirname(script_abs))
        self._write_remote_file(script_abs, script_content)
        self._exec_cmd(
            "sudo chown root:root %s && sudo chmod 500 %s" % (
                script_abs, script_abs))

        init_system = self._detect_init_system(os_root_dir)
        LOG.info(
            "Detected init system '%s'; installing LUKS firstboot script",
            init_system)

        if init_system == "systemd":
            self._register_firstboot_script_systemd(os_root_dir)
        else:
            raise exception.CoriolisException(
                "For VMs with LUKS-encrypted devices, only systemd-based VMs "
                "are supported.")

    def install_encryption_firstboot_setup(self, os_root_dir):
        """Install a firstboot script to re-enroll TPM2."""
        if not self._luks_opened:
            return

        self._event_manager.progress_update(
            "Injecting migration keyfile and installing firstboot LUKS "
            "cleanup")

        self._write_migration_keyfiles(os_root_dir)
        self._fix_grub_luks_root(os_root_dir)
        self._rebuild_initramfs(os_root_dir)
        self._install_luks_firstboot_script(os_root_dir)

    def _fix_grub_luks_root(self, os_root_dir):
        """Patch grub.cfg to use crypttab mapper names for LUKS root devices.

        update-grub names the root device after the mapper opened by osmount
        (e.g.: ``/dev/mapper/coriolis_foo``). The initramfs opens LUKS device
        using the name from crypttab (e.g.: ``/dev/mapper/luks-root``). They
        must match, so replace the osmount names in every grub.cfg we find.
        """
        if not self._luks_opened:
            return

        crypttab_path = os.path.join(os_root_dir, "etc/crypttab")
        if not utils.test_ssh_path(self._ssh, crypttab_path):
            return

        crypttab = utils.read_ssh_file(
            self._ssh, crypttab_path).decode("utf-8")

        # Build UUID -> crypttab-mapper-name mapping.
        uuid_to_crypttab_name = {}
        for line in crypttab.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith('#'):
                continue

            parts = stripped.split(None, 3)
            if len(parts) < 2:
                continue

            mapper_name = parts[0]
            m = (re.match(r'UUID=([0-9a-f-]+)', parts[1], re.IGNORECASE) or
                 re.match(r'.*/by-uuid/([0-9a-f-]+)', parts[1], re.IGNORECASE))
            if m:
                uuid_to_crypttab_name[m.group(1).lower()] = mapper_name

        if not uuid_to_crypttab_name:
            return

        # Map osmount mapper name -> crypttab mapper name for every opened
        # LUKS device that appears in crypttab.
        replacements = {}
        for _, dev_path in self._luks_opened:
            luks_uuid = self._get_luks_uuid(dev_path)
            if not luks_uuid:
                continue

            crypttab_name = uuid_to_crypttab_name.get(luks_uuid.lower())
            if not crypttab_name:
                continue

            osmount_name = "coriolis_%s" % os.path.basename(dev_path)
            if osmount_name != crypttab_name:
                replacements["/dev/mapper/%s" % osmount_name] = (
                    "/dev/mapper/%s" % crypttab_name
                )

        if not replacements:
            return

        for rel_cfg in ["boot/grub/grub.cfg", "boot/grub2/grub.cfg"]:
            cfg_path = os.path.join(os_root_dir, rel_cfg)
            if not utils.test_ssh_path(self._ssh, cfg_path):
                continue

            content = self._exec_cmd("sudo cat %s" % cfg_path)
            modified = False
            for old, new in replacements.items():
                if old not in content:
                    continue

                content = content.replace(old, new)
                modified = True
                LOG.info("grub.cfg: replaced '%s' -> '%s'", old, new)

            if modified:
                self._write_remote_file(cfg_path, content)
