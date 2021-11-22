# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.
# pylint: disable=anomalous-backslash-in-string

import abc
import base64
import collections
import itertools
import os
import re

from oslo_log import log as logging
import paramiko
from six import with_metaclass

from coriolis import exception
from coriolis import utils

LOG = logging.getLogger(__name__)

MAJOR_COLUMN_INDEX = 4


class BaseOSMountTools(object, with_metaclass(abc.ABCMeta)):

    def __init__(self, connection_info, event_manager, ignore_devices,
                 operation_timeout):
        self._event_manager = event_manager
        self._ignore_devices = ignore_devices
        self._environment = {}
        self._connection_info = connection_info
        self._osmount_operation_timeout = operation_timeout
        self._connect()

    @abc.abstractmethod
    def _connect(self):
        pass

    @abc.abstractmethod
    def get_connection(self):
        pass

    @abc.abstractmethod
    def check_os(self):
        pass

    def setup(self):
        pass

    @abc.abstractmethod
    def mount_os(self):
        pass

    @abc.abstractmethod
    def dismount_os(self, root_dir):
        pass

    def set_proxy(self, proxy_settings):
        pass

    def get_environment(self):
        return self._environment


class BaseSSHOSMountTools(BaseOSMountTools):
    @utils.retry_on_error(max_attempts=5, sleep_seconds=3)
    def _connect(self):
        connection_info = self._connection_info

        ip = connection_info["ip"]
        port = connection_info.get("port", 22)
        username = connection_info["username"]
        pkey = connection_info.get("pkey")
        password = connection_info.get("password")

        LOG.info(
            "Waiting for SSH connectivity on OSMorphing host: %(ip)s:%(port)s",
            {"ip": ip, "port": port})
        utils.wait_for_port_connectivity(ip, port)

        self._event_manager.progress_update(
            "Connecting through SSH to OSMorphing host on: %(ip)s:%(port)s" % (
                {"ip": ip, "port": port}))
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=ip, port=port, username=username, pkey=pkey,
                    password=password)
        ssh.set_log_channel("paramiko.morpher.%s.%s" % (ip, port))
        self._ssh = ssh

    def setup(self):
        if self._allow_ssh_env_vars():
            self._ssh.close()
            self._connect()

    def _allow_ssh_env_vars(self):
        pass

    def _exec_cmd(self, cmd, timeout=None):
        if not timeout:
            timeout = self._osmount_operation_timeout
        try:
            return utils.exec_ssh_cmd(self._ssh, cmd, self._environment,
                                      get_pty=True, timeout=timeout)
        except exception.MinionMachineCommandTimeout as ex:
            raise exception.OSMorphingSSHOperationTimeout(
                cmd=cmd, timeout=timeout) from ex

    def get_connection(self):
        return self._ssh


class BaseLinuxOSMountTools(BaseSSHOSMountTools):
    def _get_pvs(self):
        out = self._exec_cmd("sudo pvdisplay -c").decode().splitlines()
        LOG.debug("Output of 'pvdisplay -c' command: %s", out)
        pvs = {}
        for line in out:
            if line == "":
                continue
            line = line.strip().split(":")
            if len(line) >= 2:
                if pvs.get(line[1]) is None:
                    pvs[line[1]] = [line[0], ]
                else:
                    pvs[line[1]].append(line[0])
            else:
                LOG.warn(
                    "Ignoring improper `pvdisplay` output entry: %s" % line)
        LOG.debug("Physical Volume attributes: %s", pvs)
        return pvs

    def _check_vgs(self):
        try:
            self._exec_cmd("sudo vgck")
        except Exception as ex:
            raise exception.CoriolisException(
                "An LVM-related problem has been encountered which prevents "
                "the OSMorphing from proceeding further. Please ensure that "
                "the source VM's LVM configuration is correct and the VM is "
                "able to use all LVM volumes on the source. Error occured "
                "while checking the consistency of all LVM VGs: %s" % str(ex))

    def _get_vgnames(self):
        vg_names = []
        vgscan_out_lines = self._exec_cmd(
            "sudo vgscan").decode().splitlines()
        LOG.debug("Output of vgscan commnad: %s", vgscan_out_lines)
        for vgscan_out_line in vgscan_out_lines:
            m = re.match(
                r'\s*Found volume group "(.*)" using metadata type lvm2',
                vgscan_out_line)
            if m:
                vg_names.append(m.groups()[0])
        LOG.debug("Volume group names: %s", vg_names)
        return vg_names

    def _get_lv_paths(self):
        """ Returns list with paths of available LVM volumes. """
        lvm_paths = []
        out = self._exec_cmd("sudo lvdisplay -c").decode().strip()
        if out:
            LOG.debug("Decoded `lvdisplay` output data: %s", out)
            out_lines = out.splitlines()
            for line in out_lines:
                lvm_vol = line.strip().split(':')
                if lvm_vol:
                    lvm_paths.append(lvm_vol[0])

        LOG.debug("Found LVM device paths: %s", lvm_paths)
        return lvm_paths

    def _check_mount_fstab_partitions(
            self, os_root_dir, skip_mounts=["/", "/boot"],
            skip_filesystems=["swap"], mountable_lvm_devs=None):
        """ Reads the contents of /etc/fstab from the VM's root directory and
        tries to mount all clearly identified (by UUID or path) filesystems.
        Returns the list of the new directories which were mounted.
        param: skip_mounts: list(str()): list of directories (inside the
        chroot) to not try to mount.
        param: skip_filesystems: list(str()): list of filesystem types to skip
        mounting entirely
        param: mountable_lvm_devs: list(str()): list of LVM device paths which
        exist and are mountable should they appear in /etc/fstab
        """
        new_mountpoints = []
        etc_fstab_path = os.path.join(os_root_dir, "etc/fstab")
        if not utils.test_ssh_path(self._ssh, etc_fstab_path):
            LOG.warn(
                "etc/fstab file not found in '%s'. Cannot mount non-root dirs",
                os_root_dir)
            return []

        etc_fstab_raw = utils.read_ssh_file(self._ssh, etc_fstab_path)
        etc_fstab = etc_fstab_raw.decode('utf-8')

        LOG.debug(
            "Mounting non-root partitions from fstab file: %s" % (
                base64.b64encode(etc_fstab_raw)))

        # dictionary of the form {"mountpoint":
        #   {"device": "<dev>", "filesystem": "<fs>", "options": "<opts>"}}
        mounts = {}
        # fstab entry format:
        # <device> <mountpoint> <filesystem> <options> <dump> <fsck>
        fstab_entry_regex = (
            "^(\s*([^#\s]+)\s+(\S+)\s+(\S+)\s+(\S+)(\s+(\d)(\s+(\d))?)?\s*)$")
        for line in etc_fstab.splitlines():
            match = re.match(fstab_entry_regex, line)

            if not match:
                LOG.warn(
                    "Skipping unparseable /etc/fstab line: '%s'", line)
                continue

            device = match.group(2)
            mountpoint = match.group(3)
            if mountpoint in mounts:
                raise exception.CoriolisException(
                    "Mountpoint '%s' appears to be mounted twice in "
                    "'/etc/fstab'" % mountpoint)

            mounts[mountpoint] = {
                "device": device,
                "filesystem": match.group(4),
                "options": match.group(5)}
        # NOTE: we sort the mountpoints based on length to ensure
        # they get mounted in the correct order:
        mounts = collections.OrderedDict(
            (mountpoint, mounts[mountpoint])
            for mountpoint in sorted(mounts, key=len))

        # regex for supported fstab device references:
        fs_uuid_entry_regex = "^((UUID=|/dev/disk/by-uuid/)(.+))$"
        if not mountable_lvm_devs:
            mountable_lvm_devs = []
        device_paths = self._get_device_file_paths(mountable_lvm_devs)
        for (mountpoint, details) in mounts.items():
            device = details['device']
            fs_uuid_match = re.match(fs_uuid_entry_regex, device)
            if fs_uuid_match:
                device = "/dev/disk/by-uuid/%s" % fs_uuid_match.group(3)
            else:
                device_file_path = self._get_symlink_target(device)
                if device not in mountable_lvm_devs and (
                        device_file_path not in device_paths):
                    LOG.warn(
                        "Found fstab entry for dir %s which references device "
                        "%s. Only LVM volumes or devices referenced by UUID=* "
                        "or /dev/disk/by-uuid/* notation are supported. "
                        "Devicemapper paths for LVM volumes are also "
                        "supported. Skipping mounting directory." % (
                            mountpoint, device))
                    continue
            if mountpoint in skip_mounts:
                LOG.debug(
                    "Skipping undesired mount: %s: %s", mountpoint, details)
                continue
            if details["filesystem"] in skip_filesystems:
                LOG.debug(
                    "Skipping mounting undesired FS for device %s: %s",
                    device, details)
                continue

            LOG.debug("Attempting to mount fstab device: %s: %s",
                      device, details)
            # NOTE: `mountpoint` should always be an absolute path:
            chroot_mountpoint = "%s%s" % (os_root_dir, mountpoint)
            if not utils.test_ssh_path(self._ssh, device):
                LOG.warn(
                    "Device path %s not found, skipping mount.", device)
                continue
            mountcmd = "sudo mount -t %s -o %s %s '%s'" % (
                details["filesystem"], details["options"],
                device, chroot_mountpoint)
            try:
                self._exec_cmd(mountcmd)
                new_mountpoints.append(chroot_mountpoint)
            except Exception:
                LOG.warn(
                    "Failed to run fstab filesystem mount command: '%s'. "
                    "Skipping mount. Error details: %s",
                    mountcmd, utils.get_exception_details())

        if new_mountpoints:
            LOG.info(
                "The following new /etc/fstab entries were successfully "
                "mounted: %s", new_mountpoints)

        return new_mountpoints

    def _get_symlink_target(self, symlink):
        target = symlink
        try:
            target = self._exec_cmd('readlink -en %s' % symlink).decode()
            LOG.debug("readlink %s returned: %s" % (symlink, target))
        except Exception:
            LOG.warn('Target not found for symlink: %s. Original link path '
                     'will be returned' % symlink)

        return target

    def _get_device_file_paths(self, symlink_list):
        """ Reads a list of symlink paths, such as `/dev/GROUP/VOLUME0` or
        `/dev/mapper/GROUP-VOLUME0` and returns a list of respective links'
        target device file paths, such as `/dev/dm0`
        """
        dev_file_paths = []
        for link in symlink_list:
            dev_file = self._get_symlink_target(link)
            if not dev_file:
                dev_file = link
            dev_file_paths.append(dev_file)
        return dev_file_paths

    def _get_mounted_devices(self):
        mounts = self._exec_cmd(
            "cat /proc/mounts").decode().splitlines()
        ret = []
        mounted_device_numbers = []
        dev_nmb_cmd = "mountpoint -x %s"
        for line in mounts:
            colls = line.split()
            if colls[0].startswith("/dev"):
                dev_name = self._get_symlink_target(colls[0])
                ret.append(dev_name)
                mounted_device_numbers.append(
                    self._exec_cmd(dev_nmb_cmd % dev_name).decode().rstrip())

        block_devs = self._exec_cmd(
            "ls -al /dev | grep ^b").decode().splitlines()
        for dev_line in block_devs:
            dev = dev_line.split()
            major_minor = "%s:%s" % (
                dev[MAJOR_COLUMN_INDEX].rstrip(','),
                dev[MAJOR_COLUMN_INDEX + 1])

            if major_minor in mounted_device_numbers:
                dev_path = "/dev/%s" % dev[-1]
                if dev_path not in ret:
                    ret.append(dev_path)

        LOG.debug("Currently mounted devices: %s", ret)
        return ret

    def _get_mount_destinations(self):
        mounts = self._exec_cmd(
            "cat /proc/mounts").decode().splitlines()
        ret = set()
        for line in mounts:
            colls = line.split()
            ret.add(colls[1])
        return ret

    def _get_volume_block_devices(self):
        # NOTE: depending on the version of the worker OS, scanning for just
        # the device NAME may lead to LVM volumes getting displayed as:
        # "<VG-name>-<LV-name> (dm-N)",
        #   where 'ln -s /dev/dm-N /dev/<VG-name>/<LV-name>'
        # Querying for the kernel device name (KNAME) should ensure we get the
        # device names we desire both for physical and logical volumes.
        volume_devs = self._exec_cmd(
            "lsblk -lnao KNAME").decode().splitlines()
        LOG.debug("All block devices: %s", str(volume_devs))

        volume_devs = ["/dev/%s" % d for d in volume_devs if
                       not re.match(r"^.*\d+$", d)]

        LOG.debug("Ignoring block devices: %s", self._ignore_devices)
        volume_devs = [d for d in volume_devs if d
                       not in self._ignore_devices]

        LOG.info("Volume block devices: %s", volume_devs)
        return volume_devs

    def _find_dev_with_contents(self, devices, all_files=None,
                                one_of_files=None):
        if all_files and one_of_files:
            raise exception.CoriolisException(
                "all_files and one_of_files are mutually exclusive")
        dev_name = None
        for dev_path in devices:
            dirs = None
            tmp_dir = self._exec_cmd('mktemp -d').decode().splitlines()[0]
            try:
                self._exec_cmd('sudo mount %s %s' % (dev_path, tmp_dir))
                # NOTE: it's possible that the device was mounted successfully
                # but an I/O error occurs later along the line:
                dirs = utils.list_ssh_dir(self._ssh, tmp_dir)
                LOG.debug("Contents of device %s:\n%s", dev_path, dirs)

                if all_files and dirs:
                    common = [i if i in dirs else None for i in all_files]
                    if not all(common):
                        continue

                    dev_name = dev_path
                    break
                elif one_of_files and dirs:
                    common = [i for i in one_of_files if i in dirs]
                    if len(common) > 0:
                        dev_name = dev_path
                        break

            except Exception:
                self._event_manager.progress_update(
                    "Failed to mount and scan device '%s'" % dev_path)
                LOG.warn(
                    "Failed to mount and scan device '%s':\n%s",
                    dev_path, utils.get_exception_details())
                utils.ignore_exceptions(self._exec_cmd)(
                    "sudo umount %s" % tmp_dir
                )
                utils.ignore_exceptions(self._exec_cmd)(
                    "sudo rmdir %s" % tmp_dir
                )
                tmp_dir = None
                continue
            finally:
                if tmp_dir:
                    self._exec_cmd('sudo umount %s' % tmp_dir)

        return dev_name

    def _find_and_mount_root(self, devices):
        files = ["etc", "bin", "sbin", "boot"]
        os_root_dir = None
        os_root_device = self._find_dev_with_contents(
            devices, all_files=files)

        if os_root_device is None:
            raise exception.OperatingSystemNotFound(
                "Coriolis was unable to identify the root partition of the OS "
                "being migrated for mounting during OSMorphing. Please ensure "
                "that the source VM's root partition(s) are not encrypted, "
                "and that they are using a filesystem type and version which "
                "is supported by the OS images used for the OSMorphing minion "
                "machine. Also ensure that the source VM's mountpoint "
                "declarations in '/etc/fstab' are all correct, and are "
                "declared using '/dev/disk/by-uuid/' or 'UUID=' notation. "
                "If all else fails, please retry while using an OSMorphing "
                "minion machine image which is the same OS release as the VM "
                "being migrated.")

        try:
            tmp_dir = self._exec_cmd('mktemp -d').decode().splitlines()[0]
            self._exec_cmd('sudo mount %s %s' % (os_root_device, tmp_dir))
            os_root_dir = tmp_dir
        except Exception as ex:
            self._event_manager.progress_update(
                "Exception occurred while Coriolis was attempting to mount the"
                " root device (%s) of the OS being migrated for OSMorphing. "
                "Please ensure that the source VM's root partition(s) are "
                "using a filesystem type and version which is supported by the"
                " OS images used for the OSMorphing minion machine. Also "
                "ensure that the source VM's mountpoint declarations in "
                "'/etc/fstab' are all correct, and are declared using "
                "'/dev/disk/by-uuid/' or 'UUID=' notation. If all else fails, "
                "please retry while using an OSMorphing minion machine image "
                "which is the same OS release as the VM being migrated. Error "
                "was: %s" % (os_root_device, str(ex)))
            LOG.error(ex)
            LOG.warn(
                "Failed to mount root device '%s':\n%s",
                os_root_device, utils.get_exception_details())
            utils.ignore_exceptions(self._exec_cmd)(
                "sudo umount %s" % tmp_dir
            )
            utils.ignore_exceptions(self._exec_cmd)(
                "sudo rmdir %s" % tmp_dir
            )
            raise

        for directory in ['proc', 'sys', 'dev', 'run']:
            mount_dir = os.path.join(os_root_dir, directory)
            if not utils.test_ssh_path(self._ssh, mount_dir):
                LOG.info(
                    "No '%s' directory in mounted OS. Skipping mount.",
                    directory)
                continue
            self._exec_cmd(
                'sudo mount -o bind /%(dir)s/ %(mount_dir)s' %
                {'dir': directory, 'mount_dir': mount_dir})

        if os_root_device in devices:
            devices.remove(os_root_device)

        return os_root_dir, os_root_device

    def mount_os(self):
        dev_paths = []
        mounted_devs = self._get_mounted_devices()

        volume_devs = self._get_volume_block_devices()
        for volume_dev in volume_devs:
            self._exec_cmd("sudo partx -v -a %s || true" % volume_dev)
            dev_paths += self._exec_cmd(
                "sudo ls -1 %s*" % volume_dev).decode().splitlines()
        LOG.debug("All simple devices to scan: %s", dev_paths)

        lvm_dev_paths = []
        self._check_vgs()
        pvs = self._get_pvs()
        for vg_name in self._get_vgnames():
            found = False
            for pv in pvs[vg_name]:
                if pv in dev_paths:
                    found = True
                    break
            if not found:
                continue
            self._exec_cmd("sudo vgchange -ay %s" % vg_name)
            self._exec_cmd("sudo vgchange --refresh")
            dev_paths_for_group = self._exec_cmd(
                "sudo ls -1 /dev/%s/*" % vg_name).decode().splitlines()
            lvm_dev_paths.extend(dev_paths_for_group)
        LOG.debug("All LVM vols to scan: %s", lvm_dev_paths)

        valid_filesystems = ['ext2', 'ext3', 'ext4', 'xfs', 'btrfs']

        dev_paths_to_mount = []
        for dev_path in itertools.chain(dev_paths, lvm_dev_paths):
            LOG.debug("Checking device: '%s'", dev_path)
            dev_target = self._get_symlink_target(dev_path)
            if dev_target in mounted_devs:
                # this device is already mounted. Skip it, as it most likely
                # means this device belongs to the worker VM.
                LOG.debug(
                    "Device '%s' (target '%s') is already mounted, assuming it"
                    "belongs to the worker VM so it will be skipped",
                    dev_path, dev_target)
                continue
            fs_type = self._exec_cmd(
                "sudo blkid -o value -s TYPE %s || true" %
                dev_path).decode().splitlines()
            LOG.debug('Device %s filesystem types: %s', dev_path, fs_type)
            if fs_type and fs_type[0] in valid_filesystems:
                if fs_type[0] == "xfs":
                    utils.run_xfs_repair(self._ssh, dev_path)
                else:
                    try:
                        utils.check_fs(self._ssh, fs_type[0], dev_path)
                    except Exception as err:
                        self._event_manager.progress_update(
                            "Coriolis failed to check one of the migrated VM's"
                            " filesystems. This could have been caused by "
                            "FS corruption during the last disk sync. If "
                            "the migration fails, please ensure that any "
                            "source-side FS integrity mechanisms (e.g. "
                            "filesystem quiescing, crash-consistent backups, "
                            "etc.) are enabled and available for the source "
                            "machine. If none are available, please try "
                            "migrating/replicating the source machine while it"
                            " is powered off. Error was: %s" % str(err))
                        LOG.error(err)
                dev_paths_to_mount.append(dev_path)

        os_root_dir, os_root_device = self._find_and_mount_root(
            dev_paths_to_mount)

        grub_dirs = ["grub", "grub2"]
        os_boot_device = self._find_dev_with_contents(
            dev_paths_to_mount, one_of_files=grub_dirs)

        if os_boot_device:
            LOG.debug("Mounting boot device '%s'", os_boot_device)
            self._exec_cmd(
                'sudo mount %s "%s/boot"' % (
                    os_boot_device, os_root_dir))

        lvm_devs = list(set(self._get_lv_paths()) - set(mounted_devs))
        self._check_mount_fstab_partitions(
            os_root_dir, mountable_lvm_devs=lvm_devs)

        return os_root_dir, os_root_device

    def dismount_os(self, root_dir):
        self._exec_cmd('sudo fuser --kill --mount %s || true' % root_dir)
        mounted_fs = self._get_mount_destinations()
        # Sort all mounted filesystems by length. This will ensure that
        # the first in the list is a subfolder of the next in the list,
        # and we unmount them in the proper order
        mounted_fs = list(reversed(sorted(mounted_fs, key=len)))
        for d in mounted_fs:
            # umount these two at the very end
            if d.endswith('/dev') or d.rstrip('/') == root_dir.rstrip('/'):
                continue
            if d.startswith(root_dir):
                # mounted filesystem is a subfolder of our root_dir
                self._exec_cmd('sudo umount %s' % d)

        dev_fs = "%s/%s" % (root_dir.rstrip('/'), "dev")
        self._exec_cmd('mountpoint -q %s && sudo umount %s' % (dev_fs, dev_fs))
        self._exec_cmd(
            'mountpoint -q %s && sudo umount %s' % (root_dir, root_dir))

    def set_proxy(self, proxy_settings):
        url = proxy_settings.get('url')
        if not url:
            return

        username = proxy_settings.get('username')
        if username:
            password = proxy_settings.get('password', '')
            url = utils.get_url_with_credentials(url, username, password)

        LOG.debug("Proxy URL: %s", url)
        for var in ['http_proxy', 'https_proxy', 'ftp_proxy']:
            self._environment[var] = url
            # Some commands look for the uppercase var name
            self._environment[var.upper()] = url

        no_proxy = proxy_settings.get('no_proxy')
        if no_proxy:
            LOG.debug("Proxy exclusions: %s", no_proxy)
            self._environment["no_proxy"] = '.'.join(no_proxy)
