import os
import re

from oslo_log import log as logging

from coriolis import exception
from coriolis.osmorphing.osmount import base
from coriolis import utils

LOG = logging.getLogger(__name__)


class UbuntuOSMountTools(base.BaseSSHOSMountTools):
    def check_os(self):
        os_info = utils.get_linux_os_info(self._ssh)
        if os_info and os_info[0] == 'Ubuntu':
            return True

    def _exec_cmd(self, cmd):
        return utils.exec_ssh_cmd(self._ssh, cmd)

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
        volume_devs = self._exec_cmd(
            "lsblk -lnao NAME").decode().split('\n')[:-1]
        LOG.debug("All block devices: %s", str(volume_devs))

        # Skip this instance's current root device (first in the list)
        volume_devs = ["/dev/%s" % d for d in volume_devs if
                       not re.match(r"^.*\d+$", d)][1:]
        LOG.info("Volume block devices: %s", str(volume_devs))
        return volume_devs

    def mount_os(self):
        dev_paths = []
        other_mounted_dirs = []

        self._exec_cmd("sudo apt-get update -y")
        self._exec_cmd("sudo apt-get install lvm2 -y")
        self._exec_cmd("sudo modprobe dm-mod")

        volume_devs = self._get_volume_block_devices()
        for volume_dev in volume_devs:
            self._exec_cmd("sudo partx -v -a %s || true" % volume_dev)
            dev_paths += self._exec_cmd(
                "sudo ls %s*" % volume_dev).decode().split('\n')[:-1]

        for vg_name in self._get_vgnames():
            self._exec_cmd("sudo vgchange -ay %s" % vg_name)
            lvm_dev_paths = self._exec_cmd(
                "sudo ls /dev/%s/*" % vg_name).decode().split('\n')[:-1]
            dev_paths += lvm_dev_paths

        valid_filesystems = ['ext2', 'ext3', 'ext4', 'xfs']

        dev_paths_to_mount = []
        for dev_path in dev_paths:
            fs_type = self._exec_cmd(
                "sudo blkid -o value -s TYPE %s || true" %
                dev_path).decode().split('\n')[0]
            if fs_type in valid_filesystems:
                utils.check_fs(self._ssh, fs_type, dev_path)
                dev_paths_to_mount.append(dev_path)

        os_root_dir = None
        boot_dev_path = None
        for dev_path in dev_paths_to_mount:
            tmp_dir = self._exec_cmd('mktemp -d').decode().split('\n')[0]
            self._exec_cmd('sudo mount %s %s' % (dev_path, tmp_dir))
            dirs = self._exec_cmd('ls %s' % tmp_dir).decode().split('\n')

            # TODO: better ways to check for a linux root?
            if (not os_root_dir and 'etc' in dirs and 'bin' in dirs and
                    'sbin' in dirs):
                os_root_dir = tmp_dir
                LOG.info("OS root device: %s", dev_path)
            # TODO: better ways to check for a linux boot dir?
            else:
                # TODO: better ways to check for a linux boot dir?
                if not boot_dev_path and ('grub' in dirs or 'grub2' in dirs):
                    # Needs to be remounted under os_root_dir
                    boot_dev_path = dev_path
                    LOG.info("OS boot device: %s", dev_path)

                self._exec_cmd('sudo umount %s' % tmp_dir)

            if os_root_dir and boot_dev_path:
                break

        if not os_root_dir:
            raise exception.OperatingSystemNotFound("root partition not found")

        for dir in set(dirs).intersection(['proc', 'sys', 'dev', 'run']):
            mount_dir = os.path.join(os_root_dir, dir)
            self._exec_cmd(
                'sudo mount -o bind /%(dir)s/ %(mount_dir)s' %
                {'dir': dir, 'mount_dir': mount_dir})
            other_mounted_dirs.append(mount_dir)

        if boot_dev_path:
            boot_dir = os.path.join(os_root_dir, 'boot')
            self._exec_cmd('sudo mount %s %s' % (boot_dev_path, boot_dir))
            other_mounted_dirs.append(boot_dir)

        return os_root_dir, other_mounted_dirs

    def dismount_os(self, dirs):
        for dir in dirs:
            self._exec_cmd('sudo umount %s' % dir)
