import os
import re

from coriolis import utils


class UbuntuOSMountTools(object):
    @staticmethod
    def check_os(ssh):
        os_info = utils.get_linux_os_info(ssh)
        if os_info and os_info[0] == 'Ubuntu':
            return True

    def _get_vgnames(self, ssh):
        vg_names = []
        vgscan_out_lines = utils.exec_ssh_cmd(
            ssh, "sudo vgscan").decode().split('\n')[1:-1]
        for vgscan_out_line in vgscan_out_lines:
            m = re.match(
                r'\s*Found volume group "(.*)" using metadata type lvm2',
                vgscan_out_line)
            if m:
                vg_names.append(m.groups()[0])
        return vg_names

    def mount_os(self, ssh, volume_devs):
        dev_paths = []
        other_mounted_dirs = []

        for volume_dev in volume_devs:
            utils.exec_ssh_cmd(
                ssh,  "sudo partx -v -a %s || true" % volume_dev)
            dev_paths += utils.exec_ssh_cmd(
                ssh, "sudo ls %s*" % volume_dev).decode().split('\n')[:-1]

        utils.exec_ssh_cmd(ssh, "sudo apt-get update -y")
        utils.exec_ssh_cmd(ssh, "sudo apt-get install lvm2 -y")
        utils.exec_ssh_cmd(ssh, "sudo modprobe dm-mod")

        for vg_name in self._get_vgnames(ssh):
            utils.exec_ssh_cmd(ssh, "sudo vgchange -ay %s" % vg_name)
            lvm_dev_paths = utils.exec_ssh_cmd(
                ssh, "sudo ls /dev/%s/*" % vg_name).decode().split('\n')[:-1]
            dev_paths += lvm_dev_paths

        valid_filesystems = ['ext2', 'ext3', 'ext4', 'xfs']

        dev_paths_to_mount = []
        for dev_path in dev_paths:
            fs_type = utils.exec_ssh_cmd(
                ssh, "sudo blkid -o value -s TYPE %s || true" %
                dev_path).decode().split('\n')[0]
            if fs_type in valid_filesystems:
                dev_paths_to_mount.append(dev_path)

        os_root_dir = None
        boot_dev_path = None
        for dev_path in dev_paths_to_mount:
            tmp_dir = utils.exec_ssh_cmd(
                ssh,  'mktemp -d').decode().split('\n')[0]
            utils.exec_ssh_cmd(ssh, 'sudo mount %s %s' % (dev_path, tmp_dir))
            dirs = utils.exec_ssh_cmd(
                ssh, 'ls %s' % tmp_dir).decode().split('\n')

            # TODO: better ways to check for a linux root?
            if (not os_root_dir and 'etc' in dirs and 'bin' in dirs and
                    'sbin' in dirs):
                os_root_dir = tmp_dir
            # TODO: better ways to check for a linux boot dir?
            else:
                # TODO: better ways to check for a linux boot dir?
                if not boot_dev_path and ('grub' in dirs or 'grub2' in dirs):
                    # Needs to be remounted under os_root_dir
                    boot_dev_path = dev_path

                utils.exec_ssh_cmd(ssh, 'sudo umount %s' % tmp_dir)

            if os_root_dir and boot_dev_path:
                break

        if not os_root_dir:
            raise Exception("root partition not found")

        for dir in set(dirs).intersection(['proc', 'sys', 'dev', 'run']):
            mount_dir = os.path.join(os_root_dir, dir)
            utils.exec_ssh_cmd(
                ssh, 'sudo mount -o bind /%(dir)s/ %(mount_dir)s' %
                {'dir': dir, 'mount_dir': mount_dir})
            other_mounted_dirs.append(mount_dir)

        if boot_dev_path:
            boot_dir = os.path.join(os_root_dir, 'boot')
            utils.exec_ssh_cmd(ssh, 'sudo mount %s %s' %
                               (boot_dev_path, boot_dir))
            other_mounted_dirs.append(boot_dir)

        return os_root_dir, other_mounted_dirs

    def dismount_os(self, ssh, dirs):
        for dir in dirs:
            utils.exec_ssh_cmd(ssh, 'sudo umount %s' % dir)
