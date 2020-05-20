# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import os
import re
import uuid

from oslo_log import log as logging

from coriolis import utils
from coriolis.osmorphing import base
from coriolis.osmorphing.osdetect import centos as centos_detect
from coriolis.osmorphing.osdetect import redhat as redhat_detect


RED_HAT_DISTRO_IDENTIFIER = redhat_detect.RED_HAT_DISTRO_IDENTIFIER

LOG = logging.getLogger(__name__)

# NOTE: some constants duplicated for backwards-compatibility:
RELEASE_RHEL = RED_HAT_DISTRO_IDENTIFIER
RELEASE_CENTOS = centos_detect.CENTOS_DISTRO_IDENTIFIER
RELEASE_FEDORA = "Fedora"


IFCFG_TEMPLATE = """
TYPE=Ethernet
BOOTPROTO=dhcp
DEFROUTE=yes
IPV4_FAILURE_FATAL=no
IPV6INIT=yes
IPV6_AUTOCONF=yes
IPV6_DEFROUTE=yes
IPV6_FAILURE_FATAL=no
NAME=%(device_name)s
DEVICE=%(device_name)s
ONBOOT=yes
NM_CONTROLLED=no
"""


class BaseRedHatMorphingTools(base.BaseLinuxOSMorphingTools):
    _NETWORK_SCRIPTS_PATH = "etc/sysconfig/network-scripts"

    @classmethod
    def check_os_supported(cls, detected_os_info):
        if detected_os_info['distribution_name'] != (
                RED_HAT_DISTRO_IDENTIFIER):
            return False
        return cls._version_supported_util(
            detected_os_info['release_version'], minimum=6)

    def __init__(self, conn, os_root_dir, os_root_dev,
                 hypervisor, event_manager, detected_os_info):
        super(BaseRedHatMorphingTools, self).__init__(
            conn, os_root_dir, os_root_dev,
            hypervisor, event_manager, detected_os_info)
        self._enable_repos = []

    def disable_predictable_nic_names(self):
        cmd = 'grubby --update-kernel=ALL --args="%s"'
        self._exec_cmd_chroot(cmd % "net.ifnames=0 biosdevname=0")

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
                self._event_manager.warn(
                    "HWADDR not defined, skipping: %s" % ifcfg_file)
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
        except Exception:
            return False

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

        network_cfg_file = "etc/sysconfig/network"
        network_cfg = self._read_config_file(network_cfg_file,
                                             check_exists=True)
        if "GATEWAY" in network_cfg:
            del network_cfg["GATEWAY"]
            self._write_config_file(network_cfg_file, network_cfg)

    def _get_ifcfgs_by_type(self, ifcfg_type):
        ifcfgs = []
        for ifcfg_file in self._get_net_config_files():
            ifcfg = self._read_config_file(ifcfg_file)
            if ifcfg.get("TYPE") == ifcfg_type:
                ifcfgs.append((ifcfg_file, ifcfg))
        return ifcfgs

    def _write_nic_configs(self, nics_info):
        for idx, _ in enumerate(nics_info):
            dev_name = "eth%d" % idx
            cfg_path = "etc/sysconfig/network-scripts/ifcfg-%s" % dev_name
            if self._test_path(cfg_path):
                self._exec_cmd_chroot(
                    "cp %s %s.bak" % (cfg_path, cfg_path)
                )
            self._write_file_sudo(
                cfg_path,
                IFCFG_TEMPLATE % {
                    "device_name": dev_name,
                })

    def set_net_config(self, nics_info, dhcp):
        if dhcp:
            self.disable_predictable_nic_names()
            self._write_nic_configs(nics_info)
            return

        ifcfgs_ethernet = self._get_ifcfgs_by_type("Ethernet")
        mac_addresses = [ni.get("mac_address") for ni in nics_info]
        net_ifaces_info = self._get_net_ifaces_info(ifcfgs_ethernet,
                                                    mac_addresses)
        self._add_net_udev_rules(net_ifaces_info)

    def _yum_install(self, package_names, enable_repos=[]):
        yum_cmd = 'yum install %s -y%s' % (
            " ".join(package_names),
            "".join([" --enablerepo=%s" % r for r in enable_repos]))
        self._exec_cmd_chroot(yum_cmd)

    def _yum_uninstall(self, package_names):
        for package_name in package_names:
            yum_cmd = 'yum remove %s -y' % package_name
            self._exec_cmd_chroot(yum_cmd)

    def _yum_clean_all(self):
        self._exec_cmd_chroot("yum clean all")
        if self._test_path('var/cache/yum'):
            self._exec_cmd_chroot("rm -rf /var/cache/yum")

    def pre_packages_install(self, package_names):
        super(BaseRedHatMorphingTools, self).pre_packages_install(
            package_names)
        self._yum_clean_all()
        self._yum_install(['grubby'])

    def install_packages(self, package_names):
        self._yum_install(package_names, self._enable_repos)

    def uninstall_packages(self, package_names):
        self._yum_uninstall(package_names)

    def _run_dracut(self):
        self._run_dracut_base('kernel')

    def _run_dracut_base(self, rpm_base_name):
        package_names = []
        try:
            package_names = self._exec_cmd_chroot(
                'rpm -q %s' % rpm_base_name).decode().splitlines()
        except Exception as ex:
            self._event_manager.progress_update(
                "Failed to query kernel package name: '%s'. Unable to rebuild"
                " initrd for the new platform")
            LOG.exception(ex)

        for package_name in package_names:
            m = re.match('^%s-(.*)$' % rpm_base_name, package_name)
            if m:
                kernel_version = m.groups()[0]
                self._event_manager.progress_update(
                    "Generating initrd for kernel: %s" % kernel_version)
                self._exec_cmd_chroot(
                    "dracut -f /boot/initramfs-%(version)s.img %(version)s" %
                    {"version": kernel_version})

    def _set_network_nozeroconf_config(self):
        network_cfg_file = "etc/sysconfig/network"
        network_cfg = self._read_config_file(network_cfg_file,
                                             check_exists=True)
        network_cfg["NOZEROCONF"] = "yes"
        self._write_config_file(network_cfg_file, network_cfg)

    def _write_config_file(self, chroot_path, config_data):
        content = self._get_config_file_content(config_data)
        self._write_file_sudo(chroot_path, content)

    def _get_config_file_content(self, config):
        return "%s\n" % "\n".join(
            ['%s="%s"' % (k, v) for k, v in config.items()])

    def _get_net_config_files(self):
        dir_content = self._list_dir(self._NETWORK_SCRIPTS_PATH)
        return [os.path.join(self._NETWORK_SCRIPTS_PATH, f) for f in
                dir_content if re.match("^ifcfg-(.*)", f)]
