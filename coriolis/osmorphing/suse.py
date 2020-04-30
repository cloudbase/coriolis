# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import re

from oslo_log import log as logging

from coriolis import exception
from coriolis import utils
from coriolis.osmorphing import base


LOG = logging.getLogger(__name__)


class BaseSUSEMorphingTools(base.BaseLinuxOSMorphingTools):
    def _check_os(self):
        os_release = self._get_os_release()
        name = os_release.get("NAME")
        if name and (name == "SLES" or name.startswith("openSUSE")):
            pretty_name = os_release.get("PRETTY_NAME")
            if name == "openSUSE Tumbleweed":
                self._version_id = None
            else:
                self._version_id = os_release.get("VERSION_ID")
            return (name, pretty_name)

        suse_release_path = "etc/SuSE-release"
        if self._test_path(suse_release_path):
            out = self._read_file(suse_release_path).decode()
            release = out.splitlines()
            release_info = self._get_config(out)
            version_id = release_info['VERSION']
            patch_level = release_info.get('PATCHLEVEL', None)
            if patch_level:
                version_id = "%s.%s" % (version_id, patch_level)
            self._version_id = version_id
            if release:
                return ('SUSE', release[0])

    def disable_predictable_nic_names(self):
        # TODO(gsamfira): implement once we have networking support
        pass

    def set_net_config(self, nics_info, dhcp):
        # TODO(alexpilotti): add networking support
        pass

    def _run_dracut(self):
        package_names = self._exec_cmd_chroot(
            'rpm -q kernel-default').decode().splitlines()
        for package_name in package_names:
            m = re.match(r'^kernel-default-(.*)\.\d\..*$', package_name)
            if m:
                kernel_version = "%s-default" % m.groups()[0]
                self._event_manager.progress_update(
                    "Generating initrd for kernel: %s" % kernel_version)
                self._exec_cmd_chroot(
                    "dracut -f /boot/initrd-%(version)s %(version)s" %
                    {"version": kernel_version})

    def _run_mkinitrd(self):
        self._event_manager.progress_update("Rebuilding initrds")
        try:
            # NOTE: on SLES<12, the `mkinitrd` executable
            # must be run with no arguments:
            self._exec_cmd_chroot("mkinitrd")
            self._event_manager.progress_update(
                "Successfully rebuilt initrds")
        except Exception:
            # NOTE: old version of `mkinitrd` can error out due to
            # incompatibilities with sysfs/procfs:
            self._event_manager.progress_update(
                "Error occurred while rebuilding initrds, skipping")
            LOG.warn(
                "Exception occured while rebuilding SLES initrds:\n%s" % (
                    utils.get_exception_details()))

    def _rebuild_initrds(self):
        if float(self._version_id) < 12:
            self._run_mkinitrd()
        else:
            self._run_dracut()

    def _has_systemd(self):
        try:
            self._exec_cmd_chroot("rpm -q systemd")
            return True
        except Exception:
            return False

    def _enable_sles_module(self, module):
        available_modules = self._exec_cmd_chroot(
            "SUSEConnect --list-extensions").decode()
        module_match = re.search("%s.*" % module, available_modules)
        try:
            module_path = module_match.group(0)
            self._event_manager.progress_update(
                "Enabling module: %s" % module_path)
            conf = "/etc/zypp/zypp.conf"
            self._exec_cmd_chroot("cp %s %s.tmp" % (conf, conf))
            self._exec_cmd_chroot(
                "sed -i -e 's/^gpgcheck.*//g' -e '$ a\gpgcheck = off' %s" % (
                    conf))
            self._exec_cmd_chroot("SUSEConnect -p %s" % module_path)
            self._exec_cmd_chroot("mv -f %s.tmp %s" % (conf, conf))
            self._exec_cmd_chroot(
                "zypper --non-interactive --no-gpg-checks refresh")
        except Exception as err:
            raise exception.CoriolisException(
                "Failed to activate SLES module: %s. Please review logs"
                " for more details." % module) from err

    def _add_cloud_tools_repo(self):
        repo_suffix = ""
        if self._version_id:
            repo_suffix = "_%s" % self._version_id
        repo = "obs://Cloud:Tools/%s%s" % (
            self._distro.replace(" ", "_"), repo_suffix)
        self._event_manager.progress_update(
            "Adding repository: %s" % repo)
        try:
            self._exec_cmd_chroot(
                "zypper --non-interactive addrepo -f %s Cloud-Tools" % repo)
            self._exec_cmd_chroot(
                "zypper --non-interactive --no-gpg-checks refresh")
        except Exception as err:
            raise exception.CoriolisException(
                "Failed to add Cloud-Tools repo: %s. Please review logs"
                " for more details." % repo) from err

    def install_packages(self, package_names):
        self._exec_cmd_chroot(
            'zypper --non-interactive install %s' % " ".join(package_names))

    def uninstall_packages(self, package_names):
        try:
            self._exec_cmd_chroot(
                'zypper --non-interactive remove %s' % " ".join(package_names))
        except Exception:
            self._event_manager.progress_update(
                "Error occured while uninstalling packages. Ignoring")
            LOG.warn(
                "Error occured while uninstalling packages. Ignoring. "
                "Exception:\n%s", utils.get_exception_details())
