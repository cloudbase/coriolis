import abc
import itertools
import os
import re
import uuid

from coriolis import utils


class BaseOSMorphingTools(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, conn, os_root_dir, hypervisor, platform, event_manager):
        self._conn = conn
        self._os_root_dir = os_root_dir
        self._hypervisor = hypervisor
        self._platform = platform
        self._distro = None
        self._version = None
        self._event_manager = event_manager

    def check_os(self):
        if not self._distro:
            os_info = self._check_os()
            if os_info:
                self._distro, self._version = os_info
        if self._distro:
            return self._distro, self._version

    @abc.abstractmethod
    def _check_os(self):
        pass

    @abc.abstractmethod
    def set_net_config(self, nics_info, dhcp):
        pass

    def get_packages(self):
        return [], []

    def pre_packages_install(self):
        pass

    def install_packages(self, package_names):
        pass

    def uninstall_packages(self, package_names):
        pass

    def post_packages_install(self):
        pass


class BaseLinuxOSMorphingTools(BaseOSMorphingTools):
    __metaclass__ = abc.ABCMeta

    _packages = {}

    def __init__(self, conn, os_root_dir, hypervisor, platform, event_manager):
        super(BaseLinuxOSMorphingTools, self).__init__(
            conn, os_root_dir, hypervisor, platform, event_manager)
        self._ssh = conn

    def get_packages(self):
        k_add = [(h, p) for (h, p) in self._packages.keys() if
                 (h is None or h == self._hypervisor) and
                 (p is None or p == self._platform)]

        add = [p[0] for p in itertools.chain.from_iterable(
               [l for k, l in self._packages.items() if k in k_add])]

        k_remove = set(self._packages.keys()) - set(k_add)
        remove = [p[0] for p in itertools.chain.from_iterable(
                  [l for k, l in self._packages.items() if k in k_remove])
                  if p[1]]

        return add, remove

    def _test_path(self, chroot_path):
        path = os.path.join(self._os_root_dir, chroot_path)
        return utils.test_ssh_path(self._ssh, path)

    def _read_file(self, chroot_path):
        path = os.path.join(self._os_root_dir, chroot_path)
        return utils.read_ssh_file(self._ssh, path)

    def _write_file(self, chroot_path, content):
        path = os.path.join(self._os_root_dir, chroot_path)
        utils.write_ssh_file(self._ssh, path, content)

    def _list_dir(self, chroot_path):
        path = os.path.join(self._os_root_dir, chroot_path)
        return utils.list_ssh_dir(self._ssh, path)

    def _exec_cmd(self, cmd):
        return utils.exec_ssh_cmd(self._ssh, cmd)

    def _exec_cmd_chroot(self, cmd):
        return utils.exec_ssh_cmd_chroot(self._ssh, self._os_root_dir, cmd)

    def _check_user_exists(self, username):
        try:
            self._exec_cmd_chroot("id -u %s" % username)
            return True
        except:
            return False

    def _write_file_sudo(self, chroot_path, content):
        # NOTE: writes the file to a temp location due to permission issues
        tmp_file = 'tmp/%s' % str(uuid.uuid4())
        self._write_file(tmp_file, content)
        self._exec_cmd_chroot("cp /%s /%s" % (tmp_file, chroot_path))
        self._exec_cmd_chroot("rm /%s" % tmp_file)

    def _enable_systemd_service(self, service_name):
        self._exec_cmd_chroot("systemctl enable %s.service" % service_name)

    def _get_os_release(self):
        return self._read_config_file("etc/os-release", check_exists=True)

    def _read_config_file(self, chroot_path, check_exists=False):
        if not check_exists or self._test_path(chroot_path):
            content = self._read_file(chroot_path).decode()
            return self._get_config(content)
        else:
            return {}

    def _get_config(self, config_content):
        config = {}
        for config_line in config_content.split('\n'):
            m = re.match('(.*)="?([^"]*)"?', config_line)
            if m:
                name, value = m.groups()
                config[name] = value
        return config
