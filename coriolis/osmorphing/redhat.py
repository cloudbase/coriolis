import os
import re
import uuid

from oslo_log import log as logging
import yaml

from coriolis import constants
from coriolis.osmorphing import base
from coriolis import utils

LOG = logging.getLogger(__name__)

RELEASE_RHEL = "Red Hat Enterprise Linux Server"
RELEASE_CENTOS = "CentOS Linux"
RELEASE_FEDORA = "Fedora"

DEFAULT_CLOUD_USER = "cloud-user"


class RedHatMorphingTools(base.BaseOSMorphingTools):
    _packages = {
        (None, None): [("dracut-config-generic", False)],
        (constants.HYPERVISOR_VMWARE, None): [("open-vm-tools", True)],
        (constants.HYPERVISOR_HYPERV, None): [("hyperv-daemons", True)],
        (None, constants.PLATFORM_OPENSTACK): [
            ("cloud-init", True),
            ("cloud-utils", False),
            ("parted", False),
            ("git", False),
            ("cloud-utils-growpart", False)],
    }
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

    def _get_net_ifaces_info(self, ifcfgs_ethernet, mac_addresses):
        net_ifaces_info = []

        for ifcfg_file, ifcfg in ifcfgs_ethernet:
            mac_address = ifcfg.get("HWADDR")
            if not mac_address:
                if len(ifcfgs_ethernet) == 1 and len(mac_addresses) == 1:
                    mac_address = mac_addresses[0]
                    LOG.info("HWADDR not defined in: %s, using migration "
                             "configuration mac_address: %s",
                             ifcfg_file, mac_address)
            if not mac_address:
                LOG.warn("HWADDR not defined, skipping: %s", ifcfg_file)
                continue
            name = ifcfg.get("NAME")
            if not name:
                # Get the name from the config file
                name = re.match("^.*/ifcfg-(.*)", ifcfg_file).groups()[0]
            net_ifaces_info.append((name, mac_address))
        return net_ifaces_info

    def _add_net_udev_rules(self, net_ifaces_info):
        udev_file = "etc/udev/rules.d/70-persistent-net.rules"
        if not self._test_path(udev_file):
            if net_ifaces_info:
                content = utils.get_udev_net_rules(net_ifaces_info)
                self._write_file_sudo(udev_file, content)

    def _has_systemd(self):
        try:
            self._exec_cmd_chroot("rpm -q systemd")
            return True
        except:
            return False

    def _enable_systemd_service(self, service_name):
        self._exec_cmd_chroot("systemctl enable %s.service" % service_name)

    def _set_dhcp_net_config(self, ifcfgs_ethernet):
        for ifcfg_file, ifcfg in ifcfgs_ethernet:
            if ifcfg.get("BOOTPROTO") == "none":
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

                self._write_config_file(ifcfg_file, ifcfg)

    def _get_ifcfgs_by_type(self, ifcfg_type):
        ifcfgs = []
        for ifcfg_file in self._get_net_config_files():
            ifcfg = self._read_config_file(ifcfg_file)
            if ifcfg.get("TYPE") == ifcfg_type:
                ifcfgs.append((ifcfg_file, ifcfg))
        return ifcfgs

    def set_net_config(self, nics_info, dhcp):
        ifcfgs_ethernet = self._get_ifcfgs_by_type("Ethernet")

        if dhcp:
            self._set_dhcp_net_config(ifcfgs_ethernet)

        mac_addresses = [ni.get("mac_address") for ni in nics_info]
        net_ifaces_info = self._get_net_ifaces_info(ifcfgs_ethernet,
                                                    mac_addresses)
        self._add_net_udev_rules(net_ifaces_info)

    def install_packages(self, package_names):
        yum_cmd = 'yum install %s -y' % " ".join(package_names)
        self._exec_cmd_chroot(yum_cmd)

    def uninstall_packages(self, package_names):
        for package_name in package_names:
            yum_cmd = 'yum remove %s -y' % package_name
            self._exec_cmd_chroot(yum_cmd)

    def _run_dracut(self):
        package_names = self._exec_cmd_chroot(
            'rpm -q kernel').decode().split('\n')[:-1]
        for package_name in package_names:
            m = re.match('^kernel-(.*)$', package_name)
            if m:
                kernel_version = m.groups()[0]
                LOG.info("Generating initrd for kernel: %s", kernel_version)
                self._exec_cmd_chroot("dracut -f --kver %s" % kernel_version)

    def _get_default_cloud_user(self):
        cloud_cfg_path = os.path.join(self._os_root_dir, 'etc/cloud/cloud.cfg')
        if not self._test_path(cloud_cfg_path):
            raise Exception("cloud-init config file not found: %s" %
                            cloud_cfg_path)
        cloud_cfg_content = self._read_file(cloud_cfg_path)
        cloud_cfg = yaml.load(cloud_cfg_content)
        return cloud_cfg.get('system_info', {}).get('default_user', {}).get(
            'name', DEFAULT_CLOUD_USER)

    def _set_network_nozeroconf_config(self):
        network_cfg_file = "etc/sysconfig/network"
        network_cfg = self._read_config_file(network_cfg_file,
                                             check_exists=True)
        network_cfg["NOZEROCONF"] = "yes"
        self._write_config_file(network_cfg_file, network_cfg)

    def _configure_cloud_init(self):
        if "cloud-init" in self.get_packages()[0]:
            cloud_user = self._get_default_cloud_user()
            if not self._check_user_exists(cloud_user):
                self._exec_cmd_chroot("useradd %s" % cloud_user)
            self._set_network_nozeroconf_config()
            if self._has_systemd():
                self._enable_systemd_service("cloud-init")

    def _add_hyperv_ballooning_udev_rules(self):
        udev_file = "etc/udev/rules.d/100-balloon.rules"
        content = 'SUBSYSTEM=="memory", ACTION=="add", ATTR{state}="online"\n'

        if (self._hypervisor == constants.HYPERVISOR_HYPERV and
                not self._test_path(udev_file)):
            self._write_file_sudo(udev_file, content)

    def _read_config_file(self, chroot_path, check_exists=False):
        if not check_exists or self._test_path(chroot_path):
            content = self._read_file(chroot_path).decode()
            return self._get_config(content)
        else:
            return {}

    def _write_config_file(self, chroot_path, config_data):
        content = self._get_config_file_content(config_data)
        self._write_file_sudo(chroot_path, content)

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

    def _set_selinux_autorelabel(self):
        self._exec_cmd_chroot("touch /.autorelabel")

    def pre_packages_install(self):
        distro, version = self.check_os()
        if distro == RELEASE_RHEL and "cloud-init" in self.get_packages()[0]:
            major_version = version.split(".")[0]
            repo_name = "rhel-%s-server-rh-common-rpms" % major_version
            # This is necessary for cloud-init
            LOG.info("Enabling repository: %s" % repo_name)
            self._exec_cmd_chroot(
                "subscription-manager repos --enable %s" % repo_name)

    def post_packages_install(self):
        self._add_hyperv_ballooning_udev_rules()
        self._run_dracut()
        self._configure_cloud_init()
        self._set_selinux_autorelabel()
