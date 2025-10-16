# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import abc
import itertools
import os
import re
import uuid

from oslo_log import log as logging
from six import with_metaclass
import yaml

from coriolis import exception
from coriolis.osmorphing.netpreserver import factory
from coriolis import utils

GRUB2_SERIAL = "serial --word=8 --stop=1 --speed=%d --parity=%s --unit=0"
LOG = logging.getLogger(__name__)


# Required OS release fields which are expected from the OSDetect tools.
# 'schemas.CORIOLIS_DETECTED_OS_MORPHING_INFO_SCHEMA' schema:
REQUIRED_DETECTED_OS_FIELDS = [
    "os_type", "distribution_name", "release_version",
    "friendly_release_name"]
DEFAULT_CLOUD_USER = "cloud-user"


class BaseOSMorphingTools(object, with_metaclass(abc.ABCMeta)):

    def __init__(
            self, conn, os_root_dir, os_root_device, hypervisor,
            event_manager, detected_os_info, osmorphing_parameters,
            operation_timeout):

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
        self._osmorphing_parameters = osmorphing_parameters
        self._osmorphing_operation_timeout = operation_timeout

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
    def get_installed_packages(self):
        pass

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
                 event_manager, detected_os_info, osmorphing_parameters,
                 operation_timeout=None):
        super(BaseLinuxOSMorphingTools, self).__init__(
            conn, os_root_dir, os_root_dev, hypervisor, event_manager,
            detected_os_info, osmorphing_parameters, operation_timeout)
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

    def get_update_grub2_command(self):
        raise NotImplementedError()

    def _test_path(self, chroot_path):
        chroot_path = chroot_path.lstrip('/')
        path = os.path.join(self._os_root_dir, chroot_path)
        return utils.test_ssh_path(self._ssh, path)

    def _read_file(self, chroot_path):
        chroot_path = chroot_path.lstrip('/')
        path = os.path.join(self._os_root_dir, chroot_path)
        return utils.read_ssh_file(self._ssh, path)

    def _write_file(self, chroot_path, content):
        chroot_path = chroot_path.lstrip('/')
        path = os.path.join(self._os_root_dir, chroot_path)
        utils.write_ssh_file(self._ssh, path, content)

    def _list_dir(self, chroot_path):
        chroot_path = chroot_path.lstrip('/')
        path = os.path.join(self._os_root_dir, chroot_path)
        return utils.list_ssh_dir(self._ssh, path)

    def _exec_cmd(self, cmd, timeout=None):
        if not timeout:
            timeout = self._osmorphing_operation_timeout
        try:
            return utils.exec_ssh_cmd(
                self._ssh, cmd, environment=self._environment, get_pty=True,
                timeout=timeout)
        except exception.MinionMachineCommandTimeout as ex:
            raise exception.OSMorphingSSHOperationTimeout(
                cmd=cmd, timeout=timeout) from ex

    def _exec_cmd_chroot(self, cmd, timeout=None):
        if not timeout:
            timeout = self._osmorphing_operation_timeout
        try:
            return utils.exec_ssh_cmd_chroot(
                self._ssh, self._os_root_dir, cmd,
                environment=self._environment, get_pty=True, timeout=timeout)
        except exception.MinionMachineCommandTimeout as ex:
            raise exception.OSMorphingSSHOperationTimeout(
                cmd=cmd, timeout=timeout) from ex

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

    def _disable_systemd_service(self, service_name):
        self._exec_cmd_chroot("systemctl disable %s.service" % service_name)

    def _disable_upstart_service(self, service_name):
        self._exec_cmd_chroot(
            "echo manual | tee /etc/init/%s.override" % service_name)

    def _get_os_release(self):
        return self._read_config_file("etc/os-release", check_exists=True)

    def _read_config_file(self, chroot_path, check_exists=False):
        chroot_path = chroot_path.lstrip('/')
        full_path = os.path.join(self._os_root_dir, chroot_path)
        return utils.read_ssh_ini_config_file(
            self._ssh, full_path, check_exists=check_exists)

    def _read_config_file_sudo(self, chroot_path, check_exists=False):
        if self._test_path_chroot(chroot_path) is False:
            if check_exists:
                raise IOError("could not find %s" % chroot_path)
            return {}
        content = self._read_file_sudo(chroot_path).decode()
        config = utils.parse_ini_config(content)
        return config

    def _copy_resolv_conf(self):
        resolv_conf = "etc/resolv.conf"
        resolv_conf_path = os.path.join(self._os_root_dir, resolv_conf)
        resolv_conf_path_old = "%s.old" % resolv_conf_path

        if self._test_path(resolv_conf):
            self._exec_cmd(
                "sudo mv -f %s %s" % (resolv_conf_path, resolv_conf_path_old))
        self._exec_cmd('sudo cp -L --remove-destination /etc/resolv.conf %s' %
                       resolv_conf_path)

    def _restore_resolv_conf(self):
        resolv_conf = "etc/resolv.conf"
        resolv_conf_old = "%s.old" % resolv_conf
        resolv_conf_path = os.path.join(self._os_root_dir, resolv_conf)
        resolv_conf_path_old = "%s.old" % resolv_conf_path

        if self._test_path(resolv_conf_old):
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

    def _write_cloud_init_mods_config(self, cloud_cfg):
        cloud_cfgs_dir = "/etc/cloud/cloud.cfg.d"
        cloud_config_path = f"{cloud_cfgs_dir}/99_coriolis.cfg"
        if not self._test_path(cloud_cfgs_dir):
            self._exec_cmd_chroot("mkdir -p %s" % cloud_cfgs_dir)
        self._event_manager.progress_update(
            "Customizing cloud-init configuration")
        new_cloud_cfg = yaml.dump(cloud_cfg, Dumper=yaml.SafeDumper)
        self._write_file_sudo(cloud_config_path, new_cloud_cfg)

    def _disable_installer_cloud_config(self):
        installer_config_path = "/etc/cloud/cloud.cfg.d/99-installer.cfg"
        if self._test_path(installer_config_path):
            self._event_manager.progress_update(
                "Disabling installer-generated cloud-config")
            self._exec_cmd_chroot(
                f"mv {installer_config_path} {installer_config_path}.bak")

    def _ensure_cloud_init_not_disabled(self):
        disabler_file = "/etc/cloud/cloud-init.disabled"
        system_conf_disabler = "/etc/systemd/system.conf"
        grub_conf_disabler = "/etc/default/grub"
        if self._test_path(disabler_file):
            self._exec_cmd_chroot(f"rm {disabler_file}")
        if self._test_path(system_conf_disabler):
            self._exec_cmd_chroot(
                "sed -i '/cloud-init=disabled/d' %s" % system_conf_disabler)
        if self._test_path(grub_conf_disabler):
            self._exec_cmd_chroot(
                "sed -i '/cloud-init=disabled/d' %s" % grub_conf_disabler)
            self._execute_update_grub()

    def _reset_cloud_init_run(self):
        self._exec_cmd_chroot("cloud-init clean --logs")

    def _get_default_cloud_user(self):
        cloud_cfg_path = 'etc/cloud/cloud.cfg'
        if not self._test_path(cloud_cfg_path):
            return DEFAULT_CLOUD_USER
        cloud_cfg_content = self._read_file_sudo(cloud_cfg_path)
        cloud_cfg = yaml.load(cloud_cfg_content, Loader=yaml.SafeLoader)
        return cloud_cfg.get('system_info', {}).get('default_user', {}).get(
            'name', DEFAULT_CLOUD_USER)

    def _create_cloudinit_user(self):
        cloud_user = self._get_default_cloud_user()
        if not self._check_user_exists(cloud_user):
            self._exec_cmd_chroot("useradd %s" % cloud_user)

    def _configure_cloud_init(self):
        cloud_cfg_mods = {}
        if "cloud-init" not in self.get_packages()[0]:
            return

        self._disable_installer_cloud_config()
        self._ensure_cloud_init_not_disabled()
        self._reset_cloud_init_run()

        if self._osmorphing_parameters.get('retain_user_credentials', False):
            cloud_cfg_user_retention = {
                'disable_root': False,
                'ssh_pwauth': True,
                'users': None
            }
            cloud_cfg_mods.update(cloud_cfg_user_retention)
        else:
            self._create_cloudinit_user()

        if not self._osmorphing_parameters.get('set_dhcp', True):
            disabled_network_config = {"network": {"config": "disabled"}}
            cloud_cfg_mods.update(disabled_network_config)

        self._write_cloud_init_mods_config(cloud_cfg_mods)

    def _test_path_chroot(self, path):
        # This method uses _exec_cmd_chroot() instead of SFTP stat()
        # because in some situations, the SSH user used may not have
        # execute rights on one or more of the folders that lead up
        # to the file we are testing. In such cases, you simply get
        # a permission denied error. Using _exec_cmd_chroot(),
        # ensures you always run as root.
        if path.startswith('/') is False:
            path = "/%s" % path
        exists = self._exec_cmd_chroot(
            '[ -f "%s" ] && echo 1 || echo 0' % path).decode().rstrip('\n')
        return exists == "1"

    def _read_file_sudo(self, chroot_path):
        if chroot_path.startswith("/") is False:
            chroot_path = "/%s" % chroot_path
        contents = self._exec_cmd_chroot(
            'cat %s' % chroot_path)
        return contents

    def _read_grub_config(self, config):
        if self._test_path_chroot(config) is False:
            raise IOError("could not find %s" % config)
        contents = self._read_file_sudo(config).decode()
        ret = {}
        for line in contents.split('\n'):
            if line.startswith("#"):
                continue
            details = line.split("=", 1)
            if len(details) != 2:
                continue
            ret[details[0]] = details[1].strip('"')
        return ret

    def _get_grub_config_obj(self, grub_conf=None):
        grub_conf = grub_conf or "/etc/default/grub"
        if self._test_path_chroot(grub_conf) is False:
            raise IOError("could not find %s" % grub_conf)
        tmp_file = self._exec_cmd_chroot("mktemp").decode().rstrip('\n')
        self._exec_cmd_chroot(
            "/bin/cp -fp %s %s" % (grub_conf, tmp_file))
        config_file = self._read_grub_config(tmp_file)
        config_obj = {
            "source": grub_conf,
            "location": tmp_file,
            "contents": config_file,
        }
        return config_obj

    def _validate_grub_config_obj(self, config_obj):
        if type(config_obj) is not dict:
            raise ValueError("invalid configObj")

        missing = []

        for key in ("location", "source", "contents"):
            if not config_obj.get(key):
                missing.append(key)

        if len(missing) > 0:
            raise ValueError(
                "Invalid configObj. Missing: %s" % ", ".join(missing))

    def set_grub_value(self, option, value, config_obj, replace=True):
        self._validate_grub_config_obj(config_obj)

        def append_to_cfg(opt, val):
            cmd = "sed -ie '$a%(o)s=\"%(v)s\"' %(cfg)s" % {
                "o": opt,
                "v": val,
                "cfg": config_obj["location"]
            }
            self._exec_cmd_chroot(cmd)

        def replace_in_cfg(opt, val):
            cmd = "sed -i 's|^%(o)s=.*|%(o)s=\"%(v)s\"|g' %(cfg)s" % {
                "o": opt,
                "v": val,
                "cfg": config_obj["location"]
            }
            self._exec_cmd_chroot(cmd)

        if config_obj["contents"].get(option, False):
            if replace:
                replace_in_cfg(option, value)
        else:
            append_to_cfg(option, value)
        cfg = self._read_file_sudo(config_obj["location"]).decode()
        LOG.warning("TEMP CONFIG IS: %r" % cfg)

    def _set_grub2_cmdline(self, config_obj, options, clobber=False):
        kernel_cmd_def = config_obj["contents"].get(
            "GRUB_CMDLINE_LINUX_DEFAULT")
        kernel_cmd = config_obj["contents"].get(
            "GRUB_CMDLINE_LINUX")
        replace = kernel_cmd is not None

        if clobber:
            opt = " ".join(options)
            self.set_grub_value(
                "GRUB_CMDLINE_LINUX", opt, config_obj, replace=replace)
            return
        kernel_cmd_def = kernel_cmd_def or ""
        kernel_cmd = kernel_cmd or ""
        to_add = []
        for option in options:
            if option not in kernel_cmd_def and option not in kernel_cmd:
                to_add.append(option)
        if len(to_add):
            kernel_cmd = "%s %s" % (kernel_cmd, " ".join(to_add))
            self.set_grub_value(
                "GRUB_CMDLINE_LINUX", kernel_cmd, config_obj, replace=replace)

    def _execute_update_grub(self):
        update_cmd = self.get_update_grub2_command()
        self._exec_cmd_chroot(update_cmd)

    def _apply_grub2_config(self, config_obj,
                            execute_update_grub=True):
        self._validate_grub_config_obj(config_obj)
        self._exec_cmd_chroot(
            "mv -f %s %s" % (
                config_obj["location"], config_obj["source"]))
        if execute_update_grub:
            self._execute_update_grub()

    def _set_grub2_console_settings(self, consoles=None, speed=None,
                                    parity=None, grub_conf=None,
                                    execute_update_grub=True):
        # This method updates the GRUB2 config file and adds serial console
        # support.
        #
        # param: consoles: list: Consoles you wish to enable on the migrated
        # instance. By default, this method ensures: tty0 and ttyS0
        # param: speed: int: Baud rate for the serial console
        # param: parity: string: Options are: no, odd, even
        # param: grub_conf: string: Path to grub2 config

        valid_parity = ["no", "odd", "even"]
        if parity and parity not in valid_parity:
            raise ValueError(
                "Valid values for parity are: %s" % ", ".join(valid_parity))

        speed = speed or 115200
        parity = parity or "no"
        consoles = consoles or ["tty0", "ttyS0"]

        if type(consoles) is not list:
            raise ValueError("invalid consoles option")

        serial_cmd = GRUB2_SERIAL % (int(speed), parity)

        config_obj = self._get_grub_config_obj(grub_conf)
        self.set_grub_value("GRUB_SERIAL_COMMAND", serial_cmd, config_obj)

        options = []
        for console in consoles:
            c = "console=%s" % console
            options.append(c)

        self._set_grub2_cmdline(config_obj, options)
        self._apply_grub2_config(
            config_obj, execute_update_grub)

    def _add_net_udev_rules(self, net_ifaces_info):
        udev_file = "etc/udev/rules.d/70-persistent-net.rules"
        if not self._test_path(udev_file):
            if net_ifaces_info:
                content = utils.get_udev_net_rules(net_ifaces_info)
                self._write_file_sudo(udev_file, content)

    def _setup_network_preservation(self, nics_info) -> None:
        net_ifaces_info = dict()

        net_preserver_class = factory.get_net_preserver(self)
        LOG.info("Using network preserver class: %s", net_preserver_class)
        if not net_preserver_class:
            raise exception.CoriolisException(
                "Could not find any valid static network configuration")
        netpreserver = net_preserver_class(self)
        netpreserver.parse_network()
        LOG.info("Parsed network configuration: %s",
                 netpreserver.interface_info)

        for nic in nics_info:
            nic_mac = nic.get('mac_address')
            nic_ips = nic.get('ip_addresses')
            if not nic_mac:
                LOG.warning(
                    "Parsed NIC '%s' info does not contain MAC address"
                    % nic.get('name'))
                continue

            matching_ifaces = dict()
            for iface, info in netpreserver.interface_info.items():
                mac_address = info.get('mac_address')
                ip_addresses = info.get('ip_addresses', [])
                if mac_address and mac_address == nic_mac:
                    LOG.info(
                        "Found matching interface for NIC '%s' with MAC '%s'",
                        nic.get('name'), nic_mac)
                    matching_ifaces[iface] = nic_mac
                    break
                if ip_addresses and nic_ips:
                    if set(ip_addresses) & set(nic_ips):
                        LOG.info(
                            "Found matching interface for NIC '%s' with MAC "
                            "'%s'", nic.get('name'), nic_mac)
                        matching_ifaces[iface] = nic_mac
                        break
            if not matching_ifaces:
                LOG.warning(
                    "Could not find a matching guest interface for NIC '%s' "
                    "with MAC address '%s'", nic, nic_mac)
            net_ifaces_info.update(matching_ifaces)

        self._add_net_udev_rules(net_ifaces_info.items())

        return
