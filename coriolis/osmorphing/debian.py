# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from io import StringIO
import os

from oslo_log import log as logging
import yaml

from coriolis import exception
from coriolis.osmorphing import base
from coriolis.osmorphing.osdetect import debian as debian_osdetect
from coriolis import utils

DEBIAN_DISTRO_IDENTIFIER = debian_osdetect.DEBIAN_DISTRO_IDENTIFIER

LO_NIC_TPL = """
auto lo
iface lo inet loopback
"""

INTERFACES_NIC_TPL = """
auto %(device_name)s
iface %(device_name)s inet dhcp
"""

LOG = logging.getLogger(__name__)


class BaseDebianMorphingTools(base.BaseLinuxOSMorphingTools):

    netplan_base = "etc/netplan"

    @classmethod
    def check_os_supported(cls, detected_os_info):
        if detected_os_info['distribution_name'] != (
                DEBIAN_DISTRO_IDENTIFIER):
            return False
        return cls._version_supported_util(
            detected_os_info['release_version'], minimum=8)

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

    def get_update_grub2_command(self):
        return "update-grub"

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
            LOG.info("Setting static IP configuration")
            self._setup_network_preservation(nics_info)
            return

        self.disable_predictable_nic_names()
        if self._test_path("etc/network"):
            ifaces_file = "etc/network/interfaces"
            contents = self._compose_interfaces_config(nics_info)
            if self._test_path(ifaces_file):
                self._exec_cmd_chroot(
                    "cp %s %s.bak" % (ifaces_file, ifaces_file))
            self._write_file_sudo(ifaces_file, contents)

        if self._test_path(self.netplan_base):
            curr_files = self._list_dir(self.netplan_base)
            for cnf in curr_files:
                if cnf.endswith(".yaml") or cnf.endswith(".yml"):
                    pth = "%s/%s" % (self.netplan_base, cnf)
                    self._exec_cmd_chroot(
                        "mv %s %s.bak" % (pth, pth)
                    )
            new_cfg = self._compose_netplan_cfg(nics_info)
            cfg_name = "%s/coriolis_netplan.yaml" % self.netplan_base
            self._write_file_sudo(cfg_name, new_cfg)

    def get_installed_packages(self):
        cmd = "dpkg-query -f '${binary:Package}\\n' -W"
        try:
            self.installed_packages = self._exec_cmd_chroot(
                cmd).decode('utf-8').splitlines()
        except exception.CoriolisException:
            self.installed_packages = []

    def pre_packages_install(self, package_names):
        super(BaseDebianMorphingTools, self).pre_packages_install(
            package_names)
        try:
            if package_names:
                self._event_manager.progress_update("Updating packages list")
                self._exec_cmd_chroot('apt-get clean')
                self._exec_cmd_chroot('apt-get update -y')
        except Exception as err:
            raise exception.PackageManagerOperationException(
                "Failed to refresh apt repositories. Please ensure that *all* "
                "of the apt repositories configured within the source machine "
                "exist, are properly configured, and are reachable from the "
                "virtual network on the target platform which the OSMorphing "
                "minion machine is running on. If there are repositories "
                "configured within the source machine which are local, "
                "private, or otherwise unreachable from the target platform, "
                "please either try disabling the repositories within the "
                "source machine, or try to set up a mirror of said "
                "repositories which will be reachable from the temporary "
                "OSMorphing minion machine on the target platform. Original "
                "error was: %s" % str(err)) from err

    def install_packages(self, package_names):
        try:
            # NOTE(aznashwan): in the rare event that a replica sync occurs
            # while the source machine was midway through installing or
            # configuring a package, forcing it to retain old conf files
            # which were previously modified on the source machine.
            deb_reconfigure_cmd = (
                "dpkg --configure --force-confold -a")
            self._exec_cmd_chroot(deb_reconfigure_cmd)

            apt_get_cmd = (
                '/bin/bash -c "DEBIAN_FRONTEND=noninteractive '
                'apt-get install %s -y '
                '-o Dpkg::Options::=\'--force-confdef\'"' % (
                    " ".join(package_names)))
            self._exec_cmd_chroot(apt_get_cmd)
        except Exception as err:
            raise exception.FailedPackageInstallationException(
                package_names=package_names, package_manager='apt',
                error=str(err)) from err

    def uninstall_packages(self, package_names):
        try:
            for package_name in package_names:
                apt_get_cmd = 'apt-get remove %s -y || true' % package_name
                self._exec_cmd_chroot(apt_get_cmd)
        except exception.CoriolisException as err:
            raise exception.FailedPackageUninstallationException(
                package_names=package_names, package_manager='apt',
                error=str(err)) from err
