# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import os
from io import StringIO

import yaml

from coriolis import utils
from coriolis.osmorphing import base

LO_NIC_TPL = """
auto lo
iface lo inet loopback
"""

INTERFACES_NIC_TPL = """
auto %(device_name)s
iface %(device_name)s inet dhcp
"""


class BaseDebianMorphingTools(base.BaseLinuxOSMorphingTools):
    def _check_os(self):
        lsb_release_path = "etc/lsb-release"
        debian_version_path = "etc/debian_version"
        if self._test_path(lsb_release_path):
            config = self._read_config_file("etc/lsb-release")
            dist_id = config.get('DISTRIB_ID')
            if dist_id == 'Debian':
                release = config.get('DISTRIB_RELEASE')
                return (dist_id, release)
        elif self._test_path(debian_version_path):
            release = self._read_file(
                debian_version_path).decode().splitlines()
            if release:
                return ('Debian', release[0])

    def disable_predictable_nic_names(self):
        grub_cfg = os.path.join(
            self._os_root_dir,
            "etc/default/grub")
        if self._test_path(grub_cfg) is False:
            return
        contents = self._read_file(grub_cfg).decode()
        cfg = utils.Grub2ConfigEditor(contents)
        cfg.append_to_option(
            "GRUB_CMDLINE_LINUX_DEFAULT",
            {"opt_type": "key_val", "opt_key": "net.ifnames", "opt_val": 0})
        cfg.append_to_option(
            "GRUB_CMDLINE_LINUX_DEFAULT",
            {"opt_type": "key_val", "opt_key": "biosdevname", "opt_val": 0})
        cfg.append_to_option(
            "GRUB_CMDLINE_LINUX",
            {"opt_type": "key_val", "opt_key": "net.ifnames", "opt_val": 0})
        cfg.append_to_option(
            "GRUB_CMDLINE_LINUX",
            {"opt_type": "key_val", "opt_key": "biosdevname", "opt_val": 0})
        self._write_file_sudo("etc/default/grub", cfg.dump())
        self._exec_cmd_chroot("/usr/sbin/update-grub")

    def _compose_interfaces_config(self, nics_info):
        fp = StringIO()
        fp.write(LO_NIC_TPL)
        fp.write("\n\n")
        for idx, _ in enumerate(nics_info):
            dev_name = "eth%d" % idx
            cfg = INTERFACES_NIC_TPL % {
                "device_name": dev_name,
            }
            fp.write(cfg)
            fp.write("\n\n")
        fp.seek(0)
        return fp.read()

    def _compose_netplan_cfg(self, nics_info):
        cfg = {
            "network": {
                "version": 2,
                "ethernets": {
                    "lo": {
                        "match": {
                            "name": "lo"
                        },
                        "addresses": ["127.0.0.1/8"]
                    }
                }
            }
        }
        for idx, _ in enumerate(nics_info):
            cfg["network"]["ethernets"]["eth%d" % idx] = {
                "dhcp4": True,
                "dhcp6": True,
            }
        return yaml.dump(cfg, default_flow_style=False)

    def set_net_config(self, nics_info, dhcp):
        if not dhcp:
            return

        self.disable_predictable_nic_names()
        if self._test_path("etc/network"):
            ifaces_file = "etc/network/interfaces"
            contents = self._compose_interfaces_config(nics_info)
            if self._test_path(ifaces_file):
                self._exec_cmd_chroot(
                    "cp %s %s.bak" % (ifaces_file, ifaces_file))
            self._write_file_sudo(ifaces_file, contents)

        netplan_base = "etc/netplan"
        if self._test_path(netplan_base):
            curr_files = self._list_dir(netplan_base)
            for cnf in curr_files:
                if cnf.endswith(".yaml") or cnf.endswith(".yml"):
                    pth = "%s/%s" % (netplan_base, cnf)
                    self._exec_cmd_chroot(
                        "mv %s %s.bak" % (pth, pth)
                    )
            new_cfg = self._compose_netplan_cfg(nics_info)
            cfg_name = "%s/coriolis_netplan.yaml" % netplan_base
            self._write_file_sudo(cfg_name, new_cfg)

    def pre_packages_install(self, package_names):
        super(BaseDebianMorphingTools, self).pre_packages_install(
            package_names)

        if package_names:
            self._event_manager.progress_update("Updating packages list")
            self._exec_cmd_chroot('apt-get clean')
            self._exec_cmd_chroot('apt-get update -y')

    def install_packages(self, package_names):
        apt_get_cmd = 'apt-get install %s -y' % " ".join(package_names)
        self._exec_cmd_chroot(apt_get_cmd)

    def uninstall_packages(self, package_names):
        for package_name in package_names:
            apt_get_cmd = 'apt-get remove %s -y || true' % package_name
            self._exec_cmd_chroot(apt_get_cmd)
