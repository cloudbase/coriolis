# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import abc
import itertools
import os
import re
import uuid

from oslo_log import log as logging
from six import with_metaclass

from coriolis import exception
from coriolis import utils
from coriolis.osmorphing.osdetect import base as base_os_detect


LOG = logging.getLogger(__name__)


# Required OS release fields which are expected from the OSDetect tools.
# 'schemas.CORIOLIS_DETECTED_OS_MORPHING_INFO_SCHEMA' schema:
REQUIRED_DETECTED_OS_FIELDS = [
    "os_type", "distribution_name", "release_version", "friendly_release_name"]


class BaseOSMorphingTools(object, with_metaclass(abc.ABCMeta)):

    def __init__(
            self, conn, os_root_dir, os_root_device, hypervisor,
            event_manager, detected_os_info):

        self.check_detected_os_info_parameters(detected_os_info)

        self._conn = conn
        self._os_root_dir = os_root_dir
        self._os_root_device = os_root_device
        self._distro = detected_os_info['distribution_name']
        self._version = detected_os_info['release_version']
        self._hypervisor = hypervisor
        self._event_manager = event_manager
        self._detected_os_info = detected_os_info
        self._environment = {}

    @abc.abstractclassmethod
    def get_required_detected_os_info_fields(cls):
        raise NotImplementedError("Required OS params not defined.")

    @classmethod
    def check_detected_os_info_parameters(cls, detected_os_info):
        required_fields = cls.get_required_detected_os_info_fields()
        missing_os_info_fields = [
            field for field in required_fields
            if field not in detected_os_info]
        if missing_os_info_fields:
            raise exception.InvalidDetectedOSParams(
                "There are parameters (%s) which are required by %s but "
                "are missing from the detected OS info: %s" % (
                    missing_os_info_fields, cls.__name__, detected_os_info))

        extra_os_info_fields = [
            field for field in detected_os_info
            if field not in required_fields]
        if extra_os_info_fields:
            raise exception.InvalidDetectedOSParams(
                "There were detected OS info parameters (%s) which were not "
                "expected by %s: %s" % (
                    extra_os_info_fields, cls.__name__, detected_os_info))
        return True

    @abc.abstractclassmethod
    def check_os_supported(cls, detected_os_info):
        raise NotImplementedError(
            "OS compatibility check not implemented for tools class %s" % (
                cls.__name__))

    @abc.abstractmethod
    def set_net_config(self, nics_info, dhcp):
        pass

    @abc.abstractmethod
    def get_packages(self):
        return [], []

    @abc.abstractmethod
    def run_user_script(self, user_script):
        pass

    @abc.abstractmethod
    def pre_packages_install(self, package_names):
        pass

    @abc.abstractmethod
    def install_packages(self, package_names):
        pass

    @abc.abstractmethod
    def post_packages_install(self, package_names):
        pass

    @abc.abstractmethod
    def pre_packages_uninstall(self, package_names):
        pass

    @abc.abstractmethod
    def uninstall_packages(self, package_names):
        pass

    @abc.abstractmethod
    def post_packages_uninstall(self, package_names):
        pass

    def set_environment(self, environment):
        self._environment = environment


class BaseLinuxOSMorphingTools(BaseOSMorphingTools):

    _packages = {}

    def __init__(self, conn, os_root_dir, os_root_dev, hypervisor,
                 event_manager, detected_os_info):
        super(BaseLinuxOSMorphingTools, self).__init__(
            conn, os_root_dir, os_root_dev, hypervisor, event_manager,
            detected_os_info)
        self._ssh = conn

    @classmethod
    def get_required_detected_os_info_fields(cls):
        return REQUIRED_DETECTED_OS_FIELDS

    @classmethod
    def _version_supported_util(cls, version, minimum, maximum=None):
        """ Parses version strings which are prefixed with a floating point and
        checks whether the value is between the provided minimum and maximum
        (excluding the maximum).
        If a check for specific version is desired, the provided minimum and
        maximum values should be set to equal.
        Ex: "18.04LTS" => 18.04
        """
        if not version:
            return False

        if type(version) is not str:
            raise ValueError(
                "Non-string version provided: %s (type %s)" % (
                    version, type(version)))

        float_regex = "([0-9]+(\\.[0-9]+)?)"
        match = re.match(float_regex, version)
        if not match:
            LOG.warn(
                "Version string '%s' does not contain a float", version)
            return False

        version_float = None
        try:
            version_float = float(match.groups()[0])
        except ValueError:
            LOG.warn(
                "Failed to parse float OS release '%s'", match.groups()[0])
            return False
        LOG.debug(
            "Extracted float version from string '%s' is: %s",
            version, version_float)

        if version_float < minimum:
            LOG.debug(
                "Version '%s' smaller than the minimum of '%s' for "
                "release: %s", version_float, minimum, version)
            return False

        if maximum:
            if maximum == minimum and version_float == minimum:
                LOG.debug(
                    "Version '%s' coincides exactly with the required "
                    "minimum/maximum version: %s", version_float, minimum)
                return True
            if version_float >= maximum:
                LOG.debug(
                    "Version '%s' is larger or equal to the maximum of '%s' "
                    "for release: %s", version_float, maximum, version)
                return False

        return True

    def get_packages(self):
        k_add = [h for h in self._packages.keys() if
                 h is None or h == self._hypervisor]

        add = [p[0] for p in itertools.chain.from_iterable(
               [l for k, l in self._packages.items() if k in k_add])]

        # If the second value in the tuple is True, the package must
        # be uninstalled on export
        remove = [p[0] for p in itertools.chain.from_iterable(
                  [l for k, l in self._packages.items()])
                  if p[1]]

        return add, remove

    def run_user_script(self, user_script):
        if len(user_script) == 0:
            return

        script_path = "/tmp/coriolis_user_script"
        try:
            utils.write_ssh_file(
                self._conn,
                script_path,
                user_script)
        except Exception as err:
            raise exception.CoriolisException(
                "Failed to copy user script to target system.") from err

        try:
            utils.exec_ssh_cmd(
                self._conn,
                "sudo chmod +x %s" % script_path,
                get_pty=True)

            utils.exec_ssh_cmd(
                self._conn,
                'sudo "%s" "%s"' % (script_path, self._os_root_dir),
                get_pty=True)
        except Exception as err:
            raise exception.CoriolisException(
                "Failed to run user script.") from err

    def pre_packages_install(self, package_names):
        self._copy_resolv_conf()

    def post_packages_install(self, package_names):
        self._restore_resolv_conf()

    def pre_packages_uninstall(self, package_names):
        self._copy_resolv_conf()

    def post_packages_uninstall(self, package_names):
        self._restore_resolv_conf()

    def _test_path(self, chroot_path):
        path = os.path.join(self._os_root_dir, chroot_path)
        return utils.test_ssh_path(self._ssh, path)

    def _read_file(self, chroot_path):
        path = os.path.join(self._os_root_dir, chroot_path)
        return utils.read_ssh_file(self._ssh, path)

    def _write_file(self, chroot_path, content):
        path = os.path.join(self._os_root_dir, chroot_path)
        utils.write_ssh_file(self._ssh, path, content)

    def _list_dir(self, chroot_path):
        path = os.path.join(self._os_root_dir, chroot_path)
        return utils.list_ssh_dir(self._ssh, path)

    def _exec_cmd(self, cmd):
        return utils.exec_ssh_cmd(
            self._ssh, cmd, environment=self._environment, get_pty=True)

    def _exec_cmd_chroot(self, cmd):
        return utils.exec_ssh_cmd_chroot(
            self._ssh, self._os_root_dir, cmd,
            environment=self._environment, get_pty=True)

    def _check_user_exists(self, username):
        try:
            self._exec_cmd_chroot("id -u %s" % username)
            return True
        except Exception:
            return False

    def _write_file_sudo(self, chroot_path, content):
        # NOTE: writes the file to a temp location due to permission issues
        tmp_file = 'tmp/%s' % str(uuid.uuid4())
        self._write_file(tmp_file, content)
        self._exec_cmd_chroot("cp /%s /%s" % (tmp_file, chroot_path))
        self._exec_cmd_chroot("rm /%s" % tmp_file)
        utils.exec_ssh_cmd(
            self._ssh, "sudo sync", self._environment, get_pty=True)

    def _enable_systemd_service(self, service_name):
        self._exec_cmd_chroot("systemctl enable %s.service" % service_name)

    def _get_os_release(self):
        return self._read_config_file("etc/os-release", check_exists=True)

    def _read_config_file(self, chroot_path, check_exists=False):
        full_path = os.path.join(self._os_root_dir, chroot_path)
        return utils.read_ssh_ini_config_file(
            self._ssh, full_path, check_exists=check_exists)

    def _copy_resolv_conf(self):
        resolv_conf_path = os.path.join(self._os_root_dir, "etc/resolv.conf")
        resolv_conf_path_old = "%s.old" % resolv_conf_path

        if self._test_path(resolv_conf_path):
            self._exec_cmd(
                "sudo mv -f %s %s" % (resolv_conf_path, resolv_conf_path_old))
        self._exec_cmd('sudo cp -L --remove-destination /etc/resolv.conf %s' %
                       resolv_conf_path)

    def _restore_resolv_conf(self):
        resolv_conf_path = os.path.join(self._os_root_dir, "etc/resolv.conf")
        resolv_conf_path_old = "%s.old" % resolv_conf_path

        if self._test_path(resolv_conf_path_old):
            self._exec_cmd(
                "sudo mv -f %s %s" % (resolv_conf_path_old, resolv_conf_path))

    def _replace_fstab_entries_device_prefix(
            self, current_prefix="/dev/sd", new_prefix="/dev/sd"):
        fstab_chroot_path = "etc/fstab"
        fstab_contents = self._read_file(fstab_chroot_path).decode()
        LOG.debug("Contents of /%s: %s", fstab_chroot_path, fstab_contents)
        fstab_contents_lines = fstab_contents.splitlines()

        found = False
        regex = "^(%s)" % current_prefix
        for i, line in enumerate(fstab_contents_lines):
            if re.match(regex, line):
                found = True
                LOG.debug(
                    "Found FSTAB line starting with '%s': %s" % (
                        current_prefix, line))
                fstab_contents_lines[i] = re.sub(regex, new_prefix, line)

        if found:
            self._event_manager.progress_update(
                "Replacing all /etc/fstab entries prefixed with "
                "'%s' to '%s'" % (current_prefix, new_prefix))
            self._exec_cmd_chroot(
                "mv -f /%s /%s.bak" % (fstab_chroot_path, fstab_chroot_path))
            self._write_file(
                fstab_chroot_path, "\n".join(fstab_contents_lines))

    def _set_selinux_autorelabel(self):
        LOG.debug("setting autorelabel on /")
        try:
            self._exec_cmd_chroot(
                "touch /.autorelabel")
        except Exception as err:
            LOG.warning("Failed to set autorelabel: %r" % err)
