# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

import copy
import io
import yaml

from oslo_log import log as logging

from coriolis.osmorphing import debian
from coriolis.osmorphing.osdetect import ubuntu as ubuntu_osdetect


LOG = logging.getLogger(__name__)

UBUNTU_DISTRO_IDENTIFIER = ubuntu_osdetect.UBUNTU_DISTRO_IDENTIFIER


class BaseUbuntuMorphingTools(debian.BaseDebianMorphingTools):

    @classmethod
    def check_os_supported(cls, detected_os_info):
        if detected_os_info['distribution_name'] != (
                UBUNTU_DISTRO_IDENTIFIER):
            return False

        version, subversion = detected_os_info['release_version'].split('.', 1)
        if int(version) % 2 != 0 or not subversion.startswith("04"):
            LOG.warning(
                "Detected Ubuntu release version "
                f"'{detected_os_info['release_version']}' is not an LTS one. "
                "Coriolis only supports morphing Ubuntu LTS versions.")
            return False

        return cls._version_supported_util(
            detected_os_info['release_version'], minimum=12.04)

    def _set_netplan_ethernet_configs(
            self, nics_info, dhcp=False, iface_name_prefix=None):
        """ Updates any Ethernet interface configurations in /etc/netplan/*

        If an 'iface_name_prefix' is given, replaces all interface names
        consitently throughout all netplan files.
        """
        interface_index = 0
        # mapping between original interface name and mapped name.
        processed_interfaces = {}
        for conffile in self._list_dir("etc/netplan"):
            if not conffile.endswith(".yaml"):
                LOG.debug(
                    "Skipping file '%s' because of missing extension",
                    conffile)
                continue

            # back existing file up:
            config_path_chroot = "etc/netplan/%s" % conffile
            config_path = "%s/%s" % (self._os_root_dir, config_path_chroot)
            config_backup_path = "%s.bak" % config_path
            self._exec_cmd("sudo cp '%s' '%s'" % (
                config_path, config_backup_path))

            config_contents = self._read_file(config_path_chroot)
            config_data = yaml.load(
                io.StringIO(config_contents.decode("utf-8")),
                Loader=yaml.SafeLoader)

            config_network_data = config_data.get("network")
            if config_network_data is None:
                LOG.debug(
                    "Missing network config in file '%s'. Skipping" % (
                        config_path_chroot))
                continue

            config_version = config_network_data.get("version")
            if not config_version or int(config_version) != 2:
                LOG.debug(
                    "Skipping incompatible config version '%s' in file %s" % (
                        config_version, config_path_chroot))
                continue

            ethernet_configurations = config_network_data.get("ethernets")
            if not ethernet_configurations:
                LOG.debug(
                    "No Ethernet configurations in file %s",
                    config_path_chroot)
            new_ethernet_configurations = {}
            set_dhcp = bool(dhcp)
            for iface_name in ethernet_configurations.keys():
                new_iface_name = iface_name
                if iface_name_prefix and not iface_name.startswith(
                        iface_name_prefix):
                    if iface_name in processed_interfaces.keys():
                        new_iface_name = processed_interfaces[iface_name]
                        LOG.info(
                            "Already processed config for interface '%s'. "
                            "Using previously mapped name (%s)",
                            iface_name, new_iface_name)
                    else:
                        new_iface_name = "%s%d" % (
                            iface_name_prefix, interface_index)
                        interface_index = interface_index + 1
                    LOG.debug(
                        "Renamed interface '%s' to '%s' in '%s'",
                        iface_name, new_iface_name, config_path_chroot)
                new_config = copy.deepcopy(
                    ethernet_configurations[iface_name])
                if set_dhcp:
                    new_config["dhcp4"] = True
                    new_config["dhcp6"] = True

                LOG.info(
                    "Updating netplan interface configuration from '%s' to "
                    "'%s' in file '%s'.",
                    {iface_name: ethernet_configurations[iface_name]},
                    {new_iface_name: new_config},
                    config_path_chroot)
                new_ethernet_configurations[new_iface_name] = new_config
                processed_interfaces[iface_name] = new_iface_name

            config_data["network"]["ethernets"] = new_ethernet_configurations
            LOG.info(
                "Writing following configuration to '%s': %s" % (
                    config_path_chroot, config_data))
            self._write_file_sudo(config_path_chroot, yaml.dump(config_data))

    def _run_update_initramfs(self):
        # env LC_ALL=C suppresses Perl/shell locale warnings that would
        # otherwise appear in stdout and get mixed into the version list.
        # Using 'env' rather than a shell assignment prefix because
        # _exec_cmd_chroot runs the command directly via chroot (no shell),
        # so "LC_ALL=C cmd" would be treated as the binary name.
        raw = self._exec_cmd_chroot("env LC_ALL=C linux-version list")

        kernel_versions = [v for v in raw.splitlines() if v and v[0].isdigit()]
        if not kernel_versions:
            LOG.warning(
                "No kernel versions found via 'linux-version list'; "
                "skipping update-initramfs. Raw output was: %r",
                raw,
            )
            return
        for version in kernel_versions:
            self._exec_cmd_chroot(f"update-initramfs -k {version} -u")
