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

        dev_paths_to_mount = []
        for dev_path in dev_paths:
            fs_type = utils.exec_ssh_cmd(
                ssh, "sudo blkid -o value -s TYPE %s || true" %
                dev_path).decode().split('\n')[0]
            if fs_type in ['ext2', 'ext3', 'ext4']:
                dev_paths_to_mount.append(dev_path)

        os_root_dir = None
        for dev_path in dev_paths_to_mount:
            tmp_dir = utils.exec_ssh_cmd(
                ssh,  'mktemp -d').decode().split('\n')[0]
            utils.exec_ssh_cmd(ssh, 'sudo mount %s %s' % (dev_path, tmp_dir))
            dirs = utils.exec_ssh_cmd(
                ssh, 'ls %s' % tmp_dir).decode().split('\n')
            # TODO: better ways to check for a linux root?
            if 'etc' in dirs and 'bin' in dirs and 'sbin' in dirs:
                for dir in set(dirs).intersection(['proc', 'sys', 'dev',
                                                   'run']):
                    utils.exec_ssh_cmd(
                        ssh,
                        'sudo mount -o bind /%(dir)s/ %(mount_dir)s/%(dir)s' %
                        {'dir': dir, 'mount_dir': tmp_dir})
                os_root_dir = tmp_dir
                break

        if not os_root_dir:
            raise Exception("root partition not found")

        return os_root_dir
