# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import re

from oslo_log import log as logging

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
            release = out.split('\n')[0]
            release_info = self._get_config(out)
            version_id = release_info['VERSION']
            patch_level = release_info.get('PATCHLEVEL', None)
            if patch_level:
                version_id = "%s.%s" % (version_id, patch_level)
            self._version_id = version_id
            return ('SUSE', release)

    def set_net_config(self, nics_info, dhcp):
        # TODO(alexpilotti): add networking support
        pass

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
        except Exception:
            return False

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
