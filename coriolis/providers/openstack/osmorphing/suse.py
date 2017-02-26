# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis.osmorphing import suse as base_suse


class SUSEMorphingTools(base_suse.BaseSUSEMorphingTools):
    _packages = {
        constants.HYPERVISOR_VMWARE: [("open-vm-tools", True)],
        None: [("cloud-init", True)],
    }

    def pre_packages_install(self, package_names):
        super(SUSEMorphingTools, self).pre_packages_install(package_names)

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

    def _configure_cloud_init(self):
        if "cloud-init" in self.get_packages()[0]:
            if self._has_systemd():
                self._enable_systemd_service("cloud-init")

    def post_packages_install(self, package_names):
        self._run_dracut()
        self._configure_cloud_init()
        super(SUSEMorphingTools, self).post_packages_install(package_names)
