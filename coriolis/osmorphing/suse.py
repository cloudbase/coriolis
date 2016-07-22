# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import os
import re

from coriolis import constants
from coriolis.osmorphing import base


class SUSEMorphingTools(base.BaseLinuxOSMorphingTools):
    _packages = {
        (constants.HYPERVISOR_VMWARE, None): [("open-vm-tools", True)],
        (None, constants.PLATFORM_OPENSTACK): [("cloud-init", True)],
    }

    def _check_os(self):
        os_release = self._get_os_release()
        name = os_release.get("NAME")
        if name == "SLES" or name.startswith("openSUSE"):
            pretty_name = os_release.get("PRETTY_NAME")
            if name == "openSUSE Tumbleweed":
                self._version_id = None
            else:
                self._version_id = os_release.get("VERSION_ID")
            return (name, pretty_name)

        suse_release_path = "etc/SuSE-release"
        if self._test_path(suse_release_path):
            out = self._read_file(suse_release_path).decode()
            release = out.split('\n')[0]
            version_id = re.findall('^VERSION = (.*)$', out, re.MULTILINE)[0]
            patch_level = re.findall('^PATCHLEVEL = (.*)$', out, re.MULTILINE)
            if patch_level:
                version_id = "%s.%s" % (version_id, patch_level[1])
            self._version_id = version_id
            return ('SUSE', release)

    def set_net_config(self, nics_info, dhcp):
        # TODO: add networking support
        pass

    def pre_packages_install(self):
        super(SUSEMorphingTools, self).pre_packages_install()

        if self._platform == constants.PLATFORM_OPENSTACK:
            # TODO: use OS version to choose the right repo
            repo_version_map = {("SLES", "11.4"): "SLE_11_SP4",
                                ("SLES", "11.3"): "SLE_11_SP3",
                                ("SLES", "11.2"): "SLE_11_SP2",
                                ("SLES", "12.1"): "SLE_12_SP1",
                                ("SLES", "12"): "SLE_12",
                                ("openSUSE", "13.2"): "openSUSE_13.2",
                                ("openSUSE Leap", "42.1"):
                                "openSUSE_Leap_42.1",
                                ("openSUSE Tumbleweed", None):
                                "openSUSE_Tumbleweed"}

            repo_version = repo_version_map.get(
                (self._distro, self._version_id), "SLE_12_SP1")

            repo = "obs://Cloud:Tools/%s" % repo_version
            self._event_manager.progress_update("Adding repository: %s" % repo)
            self._exec_cmd_chroot("zypper --non-interactive addrepo -f %s "
                                  "Cloud-Tools" % repo)
        self._exec_cmd_chroot(
            "zypper --non-interactive --no-gpg-checks refresh")

    def _run_dracut(self):
        package_names = self._exec_cmd_chroot(
            'rpm -q kernel-default').decode().split('\n')[:-1]
        for package_name in package_names:
            m = re.match(r'^kernel-default-(.*)\.\d\..*$', package_name)
            if m:
                kernel_version = "%s-default" % m.groups()[0]
                self._event_manager.progress_update(
                    "Generating initrd for kernel: %s" % kernel_version)
                self._exec_cmd_chroot(
                    "dracut -f /boot/initrd-%(version)s %(version)s" %
                    {"version": kernel_version})

    def _has_systemd(self):
        try:
            self._exec_cmd_chroot("rpm -q systemd")
            return True
        except:
            return False

    def _configure_cloud_init(self):
        if "cloud-init" in self.get_packages()[0]:
            if self._has_systemd():
                self._enable_systemd_service("cloud-init")

    def install_packages(self, package_names):
        self._exec_cmd_chroot(
            'zypper --non-interactive install %s' % " ".join(package_names))

    def uninstall_packages(self, package_names):
        self._exec_cmd_chroot(
            'zypper --non-interactive remove %s' % " ".join(package_names))

    def post_packages_install(self):
        self._run_dracut()
        self._configure_cloud_init()
        super(SUSEMorphingTools, self).post_packages_install()
