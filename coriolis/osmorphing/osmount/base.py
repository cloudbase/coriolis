# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import abc
import os
import re
import base64

from oslo_log import log as logging
import paramiko
from six import with_metaclass

from coriolis import exception
from coriolis import utils

LOG = logging.getLogger(__name__)


class BaseOSMountTools(object, with_metaclass(abc.ABCMeta)):

    def __init__(self, connection_info, event_manager, ignore_devices):
        self._event_manager = event_manager
        self._ignore_devices = ignore_devices
        self._environment = {}
        self._connection_info = connection_info
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
    def dismount_os(self, dirs):
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

        LOG.info("Waiting for connectivity on host: %(ip)s:%(port)s",
                 {"ip": ip, "port": port})
        utils.wait_for_port_connectivity(ip, port)

        self._event_manager.progress_update(
            "Connecting to SSH host: %(ip)s:%(port)s" %
            {"ip": ip, "port": port})
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=ip, port=port, username=username, pkey=pkey,
                    password=password)
        self._ssh = ssh

    def setup(self):
        if self._allow_ssh_env_vars():
            self._ssh.close()
            self._connect()

    def _allow_ssh_env_vars(self):
        pass

    def _exec_cmd(self, cmd):
        return utils.exec_ssh_cmd(self._ssh, cmd, self._environment)

    def get_connection(self):
        return self._ssh


class BaseLinuxOSMountTools(BaseSSHOSMountTools):
    def _get_pvs(self):
        out = self._exec_cmd("sudo pvdisplay -c").decode().split("\n")
        pvs = {}
        for l in out:
            if l == "":
                continue
            line = l.strip().split(":")
            if pvs.get(line[1]) is None:
                pvs[line[1]] = [line[0], ]
            else:
                pvs[line[1]].append(line[0])
        return pvs

    def _get_vgnames(self):
        vg_names = []
        vgscan_out_lines = self._exec_cmd(
            "sudo vgscan").decode().split('\n')[1:-1]
        for vgscan_out_line in vgscan_out_lines:
            m = re.match(
                r'\s*Found volume group "(.*)" using metadata type lvm2',
                vgscan_out_line)
            if m:
                vg_names.append(m.groups()[0])
        return vg_names

    def _get_volume_block_devices(self):
        # NOTE: depending on the version of the worker OS, scanning for just
        # the device NAME may lead to LVM volumes getting displayed as:
        # "<VG-name>-<LV-name> (dm-N)",
        #   where 'ln -s /dev/dm-N /dev/<VG-name>/<LV-name>'
        # Querying for the kernel device name (KNAME) should ensure we get the
        # device names we desire both for physical and logical volumes.
        volume_devs = self._exec_cmd(
            "lsblk -lnao KNAME").decode().split('\n')[:-1]
        LOG.debug("All block devices: %s", str(volume_devs))

        # Skip this instance's current root device (first in the list)
        volume_devs = ["/dev/%s" % d for d in volume_devs if
                       not re.match(r"^.*\d+$", d)][1:]

        LOG.debug("Ignoring block devices: %s", self._ignore_devices)
        volume_devs = [d for d in volume_devs if d
                       not in self._ignore_devices]

        LOG.info("Volume block devices: %s", volume_devs)
        return volume_devs

    def _check_var_partition(self, os_root_dir, other_mounted_dirs):

        reg_expr = (
            '^(([^#\s]\S+)\s+/var\s+\S+\s+\S+\s+[0-9]+\s+[0-9]+)$')
        etc_fstab_path = os.path.join(os_root_dir, "etc/fstab")

        if not utils.test_ssh_path(self._ssh, etc_fstab_path):
            LOG.debug("fstab file not found.")
            return

        etc_fstab_raw = utils.read_ssh_file(self._ssh, etc_fstab_path)
        etc_fstab = etc_fstab_raw.decode('utf-8')

        LOG.debug("Searching for separate /var partition in %s" %
                  base64.b64encode(etc_fstab_raw))

        var_found = None
        for i in etc_fstab.splitlines():
            var_found = re.search(reg_expr, i)
            if var_found:
                if i.startswith('UUID='):
                    var_dev = var_found.group(2)
                    var_mnt_dir = os.path.join(os_root_dir, 'var')
                    try:
                        self._exec_cmd(
                            'sudo mount %s %s' % (var_dev, var_mnt_dir))
                        other_mounted_dirs.append(var_mnt_dir)
                    except Exception as ex:
                        LOG.warn(
                            "Could not mount /var partition: %s" %
                            utils.get_exception_details())
                        self._event_manager.progress_update(
                            "Unable to mount /var partition")
                        raise
                    break
                else:
                    raise ValueError("At %s , fstab partitions must "
                                     "be mounted by UUID!" % i)
        if not var_found:
            LOG.debug("External /var partition not detected.")

    def mount_os(self):
        dev_paths = []
        other_mounted_dirs = []

        volume_devs = self._get_volume_block_devices()
        for volume_dev in volume_devs:
            self._exec_cmd("sudo partx -v -a %s || true" % volume_dev)
            dev_paths += self._exec_cmd(
                "sudo ls %s*" % volume_dev).decode().split('\n')[:-1]

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
            lvm_dev_paths = self._exec_cmd(
                "sudo ls /dev/%s/*" % vg_name).decode().split('\n')[:-1]
            dev_paths += lvm_dev_paths

        valid_filesystems = ['ext2', 'ext3', 'ext4', 'xfs', 'btrfs']

        dev_paths_to_mount = []
        for dev_path in dev_paths:
            fs_type = self._exec_cmd(
                "sudo blkid -o value -s TYPE %s || true" %
                dev_path).decode().split('\n')[0]
            if fs_type in valid_filesystems:
                utils.check_fs(self._ssh, fs_type, dev_path)
                dev_paths_to_mount.append(dev_path)

        os_root_device = None
        os_root_dir = None
        boot_dev_path = None
        for dev_path in dev_paths_to_mount:
            tmp_dir = self._exec_cmd('mktemp -d').decode().split('\n')[0]
            self._exec_cmd('sudo mount %s %s' % (dev_path, tmp_dir))
            dirs = self._exec_cmd('ls %s' % tmp_dir).decode().split('\n')

            # TODO(alexpilotti): better ways to check for a linux root?
            if (not os_root_dir and 'etc' in dirs and 'bin' in dirs and
                    'sbin' in dirs):
                os_root_dir = tmp_dir
                os_root_device = dev_path
                LOG.info("OS root device: %s", dev_path)
            # TODO(alexpilotti): better ways to check for a linux boot dir?
            else:
                # TODO(alexpilotti): better ways to check for a linux boot dir?
                if not boot_dev_path and ('grub' in dirs or 'grub2' in dirs):
                    # Needs to be remounted under os_root_dir
                    boot_dev_path = dev_path
                    LOG.info("OS boot device: %s", dev_path)

                self._exec_cmd('sudo umount %s' % tmp_dir)

            if os_root_dir and boot_dev_path:
                break

        if not os_root_dir:
            raise exception.OperatingSystemNotFound("root partition not found")

        self._check_var_partition(os_root_dir, other_mounted_dirs)

        for dir in set(dirs).intersection(['proc', 'sys', 'dev', 'run']):
            mount_dir = os.path.join(os_root_dir, dir)
            if not utils.test_ssh_path(self._ssh, mount_dir):
                LOG.info(
                    "No '%s' directory in mounted OS. Skipping mount.", dir)
                continue
            self._exec_cmd(
                'sudo mount -o bind /%(dir)s/ %(mount_dir)s' %
                {'dir': dir, 'mount_dir': mount_dir})
            other_mounted_dirs.append(mount_dir)

        if boot_dev_path:
            boot_dir = os.path.join(os_root_dir, 'boot')
            self._exec_cmd('sudo mount %s %s' % (boot_dev_path, boot_dir))
            other_mounted_dirs.append(boot_dir)

        return os_root_dir, other_mounted_dirs, os_root_device

    def dismount_os(self, dirs):
        for dir in dirs:
            self._exec_cmd('sudo umount %s' % dir)

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
