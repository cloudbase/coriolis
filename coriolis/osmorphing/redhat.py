import re

from oslo_log import log as logging

from coriolis import constants
from coriolis.osmorphing import base

LOG = logging.getLogger(__name__)


class RedHatMorphingTools(base.BaseOSMorphingTools):
    _packages = {
        (None, None): [("dracut-config-generic", False)],
        (constants.HYPERVISOR_VMWARE, None): [("open-vm-tools", True)],
        (constants.HYPERVISOR_HYPERV, None): [("hyperv-daemons", True)],
        (None, constants.PLATFORM_OPENSTACK): [
            ("cloud-init", True),
            ("cloud-utils-growpart", False)],
    }
    _CLOUD_INIT_USER = 'centos'

    def check_os(self):
        redhat_release_path = "etc/redhat-release"
        if self._test_path(redhat_release_path):
            release_info = self._read_file(
                redhat_release_path).decode().split('\n')[0].strip()
            m = re.match(r"^(.*) release ([0-9].*) \((.*)\).*$", release_info)
            if m:
                distro, version, codename = m.groups()
                return (distro, version)

    def set_dhcp(self):
        # TODO: set BOOTPROTO="dhcp" in all relevant ifcfg-* configurations
        pass

    def install_packages(self, package_names):
        apt_get_cmd = 'yum install %s -y' % " ".join(package_names)
        self._exec_cmd_chroot(apt_get_cmd)

    def uninstall_packages(self, package_names):
        for package_name in package_names:
            apt_get_cmd = 'yum remove %s -y' % package_name
            self._exec_cmd_chroot(apt_get_cmd)

    def _run_dracut(self):
        package_names = self._exec_cmd_chroot(
            'rpm -q kernel').decode().split('\n')[:-1]
        for package_name in package_names:
            m = re.match('^kernel-(.*)$', package_name)
            if m:
                kernel_version = m.groups()[0]
                LOG.info("Generating initrd for kernel: %s", kernel_version)
                self._exec_cmd_chroot("dracut -f --kver %s" % kernel_version)

    def _add_cloud_init_user(self):
        if ("cloud-init" in self.get_packages()[0] and not
                self._check_user_exists(self._CLOUD_INIT_USER)):
            self._exec_cmd_chroot("useradd %s" % self._CLOUD_INIT_USER)

    def _add_hyperv_ballooning_rule(self):
        udev_file = "etc/udev/rules.d/100-balloon.rules"
        content = 'SUBSYSTEM=="memory", ACTION=="add", ATTR{state}="online"\n'

        if (self._hypervisor == constants.HYPERVISOR_HYPERV and
                not self._test_path(udev_file)):
            # NOTE: writes the file to a temp location due to permission issues
            tmp_file = 'tmp/100-balloon.rules'
            self._write_file(tmp_file, content)
            self._exec_cmd_chroot("cp /%s /%s" % (tmp_file, udev_file))
            self._exec_cmd_chroot("rm /%s" % tmp_file)

    def post_packages_install(self):
        self._run_dracut()
        self._add_cloud_init_user()
        self._add_hyperv_ballooning_rule()
