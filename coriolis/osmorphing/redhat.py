import os
import re
import uuid

from oslo_log import log as logging

from coriolis import constants
from coriolis.osmorphing import base
from coriolis import utils

LOG = logging.getLogger(__name__)

RELEASE_RHEL = "Red Hat Enterprise Linux Server"
RELEASE_CENTOS = "CentOS Linux"
RELEASE_FEDORA = "Fedora"


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
    _NETWORK_SCRIPTS_PATH = "etc/sysconfig/network-scripts"

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
        for ifcfg_file in self._get_net_config_files():
            ifcfg_content = self._read_file(ifcfg_file).decode()
            ifcfg = self._get_config(ifcfg_content)
            if (ifcfg.get("TYPE") == "Ethernet" and
                    ifcfg.get("BOOTPROTO") == "none"):
                ifcfg["BOOTPROTO"] = "dhcp"
                ifcfg["UUID"] = str(uuid.uuid4())

                if 'IPADDR' in ifcfg:
                    del ifcfg['IPADDR']
                if 'GATEWAY' in ifcfg:
                    del ifcfg['GATEWAY']
                if 'NETMASK' in ifcfg:
                    del ifcfg['NETMASK']
                if 'NETWORK' in ifcfg:
                    del ifcfg['NETWORK']

                ifcfg_updated_content = self._get_config_file_content(ifcfg)
                self._write_file_sudo(ifcfg_file, ifcfg_updated_content)

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

    def _add_hyperv_ballooning_udev_rules(self):
        udev_file = "etc/udev/rules.d/100-balloon.rules"
        content = 'SUBSYSTEM=="memory", ACTION=="add", ATTR{state}="online"\n'

        if (self._hypervisor == constants.HYPERVISOR_HYPERV and
                not self._test_path(udev_file)):
            self._write_file_sudo(udev_file, content)

    def _add_net_udev_rules(self):
        udev_file = "etc/udev/rules.d/70-persistent-net.rules"
        if not self._test_path(udev_file):
            net_ifaces_info = self._get_net_ifaces_info()
            content = utils.get_udev_net_rules(net_ifaces_info)
            self._write_file_sudo(udev_file, content)

    def _get_config_file_content(self, config):
        return "%s\n" % "\n".join(
            ['%s="%s"' % (k, v) for k, v in config.items()])

    def _get_config(self, config_content):
        config = {}
        for config_line in config_content.split('\n'):
            m = re.match('(.*)="?([^"]*)"?', config_line)
            if m:
                name, value = m.groups()
                config[name] = value
        return config

    def _get_net_config_files(self):
        dir_content = self._list_dir(self._NETWORK_SCRIPTS_PATH)
        return [os.path.join(self._NETWORK_SCRIPTS_PATH, f) for f in
                dir_content if re.match("^ifcfg-(.*)", f)]

    def _get_net_ifaces_info(self):
        net_ifaces_info = []
        for ifcfg_file in self._get_net_config_files():
            ifcfg_content = self._read_file(ifcfg_file).decode()
            ifcfg = self._get_config(ifcfg_content)
            if ifcfg.get("TYPE") == "Ethernet":
                mac_address = ifcfg.get("HWADDR")
                if not mac_address:
                    LOG.warn("HWADDR not defined in: %s", ifcfg_file)
                    continue
                name = ifcfg.get("NAME")
                if not name:
                    # Get the name from the config file
                    name = re.match("^.*/ifcfg-(.*)", ifcfg_file).groups()[0]
                net_ifaces_info.append((name, mac_address))
        return net_ifaces_info

    def _set_selinux_autorelabel(self):
        self._exec_cmd_chroot("touch /.autorelabel")

    def post_packages_install(self):
        self._add_net_udev_rules()
        self._add_hyperv_ballooning_udev_rules()
        self._run_dracut()
        self._add_cloud_init_user()
        self._set_selinux_autorelabel()
