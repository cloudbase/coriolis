# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import os

from oslo_log import log as logging
import yaml

from coriolis import constants
from coriolis import exception
from coriolis.osmorphing import redhat as base_redhat

LOG = logging.getLogger(__name__)

RELEASE_RHEL = "Red Hat Enterprise Linux Server"
RELEASE_CENTOS = "CentOS Linux"
RELEASE_FEDORA = "Fedora"

DEFAULT_CLOUD_USER = "cloud-user"


class RedHatMorphingTools(base_redhat.BaseRedHatMorphingTools):
    _packages = {
        constants.HYPERVISOR_VMWARE: [("open-vm-tools", True)],
        constants.HYPERVISOR_HYPERV: [("hyperv-daemons", True)],
        None: [
            ("dracut-config-generic", False),
            ("cloud-init", True),
            ("cloud-utils", False),
            ("parted", False),
            ("git", False),
            ("cloud-utils-growpart", False)],
    }

    def _get_default_cloud_user(self):
        cloud_cfg_path = os.path.join(self._os_root_dir, 'etc/cloud/cloud.cfg')
        if not self._test_path(cloud_cfg_path):
            raise exception.CoriolisException(
                "cloud-init config file not found: %s" % cloud_cfg_path)
        cloud_cfg_content = self._read_file(cloud_cfg_path)
        cloud_cfg = yaml.load(cloud_cfg_content)
        return cloud_cfg.get('system_info', {}).get('default_user', {}).get(
            'name', DEFAULT_CLOUD_USER)

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

    def pre_packages_install(self, package_names):
        super(RedHatMorphingTools, self).pre_packages_install(package_names)

        distro, version = self.check_os()
        if distro == RELEASE_RHEL and "cloud-init" in self.get_packages()[0]:
            major_version = version.split(".")[0]
            repo_name = "rhel-%s-server-rh-common-rpms" % major_version
            # This is necessary for cloud-init
            self._event_manager.progress_update(
                "Enabling repository: %s" % repo_name)
            self._exec_cmd_chroot(
                "subscription-manager repos --enable %s" % repo_name)

    def post_packages_install(self, package_names):
        self._add_hyperv_ballooning_udev_rules()
        self._run_dracut()
        self._configure_cloud_init()
        self._set_selinux_autorelabel()
        super(RedHatMorphingTools, self).post_packages_install(package_names)
