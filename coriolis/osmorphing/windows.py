# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import base64
import copy
import ipaddress
import json
import os
import re
import uuid

from oslo_log import log as logging

from coriolis import constants
from coriolis import exception
from coriolis.osmorphing import base
from coriolis.osmorphing.osdetect import windows as windows_osdetect
from coriolis import utils

LOG = logging.getLogger(__name__)

WINDOWS_CLIENT_IDENTIFIER = windows_osdetect.WINDOWS_CLIENT_IDENTIFIER
WINDOWS_SERVER_IDENTIFIER = windows_osdetect.WINDOWS_SERVER_IDENTIFIER

SERVICE_START_AUTO = 2
SERVICE_START_MANUAL = 3
SERVICE_START_DISABLED = 4

SERVICE_PATH_FORMAT = "HKLM:\\%s\\ControlSet001\\Services\\%s"
CLOUDBASEINIT_SERVICE_NAME = "cloudbase-init"
CLOUDBASE_INIT_DEFAULT_PLUGINS = [
    'cloudbaseinit.plugins.common.mtu.MTUPlugin',
    'cloudbaseinit.plugins.windows.ntpclient'
    '.NTPClientPlugin',
    'cloudbaseinit.plugins.common.sethostname'
    '.SetHostNamePlugin',
    'cloudbaseinit.plugins.windows.createuser'
    '.CreateUserPlugin',
    'cloudbaseinit.plugins.common.networkconfig'
    '.NetworkConfigPlugin',
    'cloudbaseinit.plugins.windows.licensing'
    '.WindowsLicensingPlugin',
    'cloudbaseinit.plugins.common.sshpublickeys'
    '.SetUserSSHPublicKeysPlugin',
    'cloudbaseinit.plugins.windows.extendvolumes'
    '.ExtendVolumesPlugin',
    'cloudbaseinit.plugins.common.userdata.UserDataPlugin',
    'cloudbaseinit.plugins.common.setuserpassword.'
    'SetUserPasswordPlugin',
    'cloudbaseinit.plugins.windows.winrmlistener.'
    'ConfigWinRMListenerPlugin',
    'cloudbaseinit.plugins.windows.winrmcertificateauth.'
    'ConfigWinRMCertificateAuthPlugin',
    'cloudbaseinit.plugins.common.localscripts'
    '.LocalScriptsPlugin',
]

CLOUDBASE_INIT_DEFAULT_METADATA_SVCS = [
    'cloudbaseinit.metadata.services.httpservice.HttpService',
    'cloudbaseinit.metadata.services'
    '.configdrive.ConfigDriveService',
    'cloudbaseinit.metadata.services.ec2service.EC2Service',
    'cloudbaseinit.metadata.services'
    '.maasservice.MaaSHttpService',
    'cloudbaseinit.metadata.services.cloudstack.CloudStack',
    'cloudbaseinit.metadata.services'
    '.opennebulaservice.OpenNebulaService',
]

REQUIRED_DETECTED_WINDOWS_OS_FIELDS = [
    "version_number", "edition_id", "installation_type", "product_name"]

INTERFACES_PATH_FORMAT = (
    "HKLM:\\%s\\ControlSet001\\Services\\Tcpip\\Parameters\\Interfaces")

STATIC_IP_SCRIPT_TEMPLATE = """
$ErrorActionPreference = "Stop"

function Get-InterfaceByMacAddress {
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory=$true)]
        [Object]$mac_address
    )

    $win_formatted_mac_address = $mac_address.replace(':', '-').toUpper()
    return (Get-NetAdapter | Where MacAddress -eq $win_formatted_mac_address)
}

function Set-IPAddresses {
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory=$true)]
        [Object]$interfaceObj,

        [Parameter(Mandatory=$true)]
        [Array]$ip_addresses,

        [Parameter(Mandatory=$true)]
        [Array]$ips_info
    )

    foreach ($ip in $ip_addresses) {
        $ip_info = $ips_info | Where ip_address -eq $ip
        if ($ip_info.default_gateway) {
            # Remove existing gateway before setting default gateway on the interface
            Get-NetRoute | Where NextHop -eq $ip_info.default_gateway | Remove-NetRoute -Confirm:$false
            New-NetIPAddress -InterfaceIndex $interfaceObj.ifIndex -IPAddress $ip_info.ip_address -PrefixLength $ip_info.prefix_length -DefaultGateway $ip_info.default_gateway
        } else {
            New-NetIPAddress -InterfaceIndex $interfaceObj.ifIndex -IPAddress $ip_info.ip_address -PrefixLength $ip_info.prefix_length
        }

        if ($ip_info.dns_addresses) {
            # Set DNS Addresses on the interface
            Set-DnsClientServerAddress -InterfaceIndex $interfaceObj.ifIndex -ServerAddresses $ip_info.dns_addresses
        }
    }
}

function Invoke-Main {
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory=$true)]
        [Array]$nics_info,

        [Parameter(Mandatory=$true)]
        [Array]$ips_info
    )

    foreach ($nic in $nics_info) {
        $interface = Get-InterfaceByMacAddress $nic.mac_address
        if (-Not $interface) {
            Write-Host "No interface found for MAC address: $($nic.mac_address)"
            continue
        }

        # Removes existing IP configuration for the interface
        Remove-NetIPAddress -InterfaceIndex $interface.ifIndex -Confirm:$false

        Set-IPAddresses $interface $nic.ip_addresses $ips_info
    }
}

$nics_info_json = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String("%(nics_info)s"))
$ips_info_json = [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String("%(ips_info)s"))
$NICS_INFO = ConvertFrom-Json $nics_info_json
$IPS_INFO = ConvertFrom-Json $ips_info_json

Invoke-Main $NICS_INFO $IPS_INFO
"""  # noqa


class BaseWindowsMorphingTools(base.BaseOSMorphingTools):

    @classmethod
    def get_required_detected_os_info_fields(cls):
        base_fields = copy.deepcopy(base.REQUIRED_DETECTED_OS_FIELDS)
        base_fields.extend(REQUIRED_DETECTED_WINDOWS_OS_FIELDS)
        return base_fields

    @classmethod
    def check_os_supported(cls, detected_os_info):
        # TODO(aznashwan): add more detailed checks for Windows:
        if detected_os_info['os_type'] == constants.OS_TYPE_WINDOWS:
            return True
        return False

    def __init__(
            self, conn, os_root_dir, os_root_device, hypervisor,
            event_manager, detected_os_info, osmorphing_parameters,
            operation_timeout=None):
        super(BaseWindowsMorphingTools, self).__init__(
            conn, os_root_dir, os_root_device, hypervisor,
            event_manager, detected_os_info, osmorphing_parameters,
            operation_timeout)

        self._version_number = detected_os_info['version_number']
        self._edition_id = detected_os_info['edition_id']
        self._installation_type = detected_os_info['installation_type']
        self._product_name = detected_os_info['product_name']

    def _get_worker_os_drive_path(self):
        return self._conn.exec_ps_command(
            "(Get-WmiObject Win32_OperatingSystem).SystemDrive")

    def _get_dism_path(self):
        return "%s\\Windows\\System32\\dism.exe" % (
            self._get_worker_os_drive_path())

    def _get_sid(self):
        sid = self._conn.exec_ps_command(
            "(New-Object System.Security.Principal.NTAccount($ENV:USERNAME))."
            "Translate([System.Security.Principal.SecurityIdentifier]).Value")
        LOG.debug("Current user's SID: %s", sid)
        return sid

    def _grant_permissions(self, path, user, perm="(OI)(CI)F"):
        self._conn.exec_command(
            "icacls.exe", [path, "/grant", "%s:%s" % (user, perm)])

    def _revoke_permissions(self, path, user):
        self._conn.exec_command(
            "icacls.exe", [path, "/remove", user])

    def _load_registry_hive(self, subkey, path):
        self._conn.exec_command("reg.exe", ["load", subkey, path])

    def _unload_registry_hive(self, subkey):
        self._conn.exec_command("reg.exe", ["unload", subkey])

    def _get_ps_fl_value(self, data, name):
        m = re.search(r'^%s\s*: (.*)$' % name, data, re.MULTILINE)
        if m:
            return m.groups()[0]

    def _add_dism_driver(self, driver_path):
        LOG.info("Adding driver: %s" % driver_path)
        dism_path = self._get_dism_path()
        try:
            return self._conn.exec_command(
                dism_path,
                ["/add-driver", "/image:%s" % self._os_root_dir,
                 "/driver:\"%s\"" % driver_path, "/recurse",
                 "/forceunsigned"])
        except Exception as ex:
            dism_log_path = "%s\\Windows\\Logs\\DISM\\dism.log" % (
                self._get_worker_os_drive_path())
            if self._conn.test_path(dism_log_path):
                dism_log_contents = self._conn.exec_ps_command(
                    "Get-Content %s" % dism_log_path)
                LOG.error(
                    "Error occured whilst adding driver '%s' through DISM. "
                    "Contents of '%s': %s",
                    driver_path, dism_log_path, dism_log_contents)
            else:
                LOG.warn(
                    "Could not find DISM error logs for failure:'%s'",
                    str(ex))
            raise

    def _mount_disk_image(self, path):
        LOG.info("Mounting disk image: %s" % path)
        return self._conn.exec_ps_command(
            "(Mount-DiskImage '%s' -PassThru | Get-Volume).DriveLetter" %
            path)

    def _dismount_disk_image(self, path):
        LOG.info("Unmounting disk image: %s" % path)
        self._conn.exec_ps_command("Dismount-DiskImage '%s'" % path,
                                   ignore_stdout=True)

    @utils.retry_on_error()
    def _expand_archive(self, path, destination, overwrite=True):
        LOG.info("Expanding archive \"%(path)s\" in \"%(destination)s\"",
                 {"path": path, "destination": destination})

        if self._conn.test_path(destination):
            LOG.info("Destination folder %s already exists" % destination)
            if overwrite:
                if destination.endswith(":\\") or ":\\Windows" in destination:
                    LOG.warn(
                        "Not removing target directory, as it is either the "
                        "root directory or is within the Windows directory")
                else:
                    self._conn.exec_ps_command(
                        "rm -recurse -force %s" % destination)
        self._conn.exec_ps_command(
            "if(([System.Management.Automation.PSTypeName]"
            "'System.IO.Compression.ZipFile').Type -or "
            "[System.Reflection.Assembly]::LoadWithPartialName("
            "'System.IO.Compression.FileSystem')) {"
            "[System.IO.Compression.ZipFile]::ExtractToDirectory('%(path)s', "
            "'%(destination)s')} else {mkdir -Force '%(destination)s'; "
            "$shell = New-Object -ComObject Shell.Application;"
            "$shell.Namespace('%(destination)s').copyhere(($shell.NameSpace("
            "'%(path)s')).items())}" %
            {"path": path, "destination": destination},
            ignore_stdout=True)

    def _set_service_start_mode(self, key_name, service_name, start_mode):
        LOG.info("Setting service start mode: %(service_name)s, "
                 "%(start_mode)s", {"service_name": service_name,
                                    "start_mode": start_mode})
        registry_path = SERVICE_PATH_FORMAT % (key_name, service_name)
        self._conn.exec_ps_command(
            "Set-ItemProperty -Path '%(path)s' -Name 'Start' -Value "
            "%(start_mode)s" %
            {"path": registry_path, "start_mode": start_mode})

    def _create_service(self, key_name, service_name, image_path,
                        display_name, description,
                        start_mode=SERVICE_START_AUTO,
                        service_account="LocalSystem",
                        depends_on=[]):
        LOG.info("Creating service: %s", service_name)
        registry_path = SERVICE_PATH_FORMAT % (key_name, service_name)
        depends_on_ps = "@(%s)" % (",".join(["'%s'" % v for v in depends_on]))

        self._conn.exec_ps_command(
            "$ErrorActionPreference = 'Stop';"
            "New-Item -Path '%(path)s' -Force;"
            "New-ItemProperty -Path '%(path)s' -Name 'ImagePath' -Value "
            "'%(image_path)s' -Type ExpandString -Force;"
            "New-ItemProperty -Path '%(path)s' -Name 'DisplayName' -Value "
            "'%(display_name)s' -Type String -Force;"
            "New-ItemProperty -Path '%(path)s' -Name 'Description' -Value "
            "'%(description)s' -Type String -Force;"
            "New-ItemProperty -Path '%(path)s' -Name 'DependOnService' -Value "
            "%(depends_on)s -Type MultiString -Force;"
            "New-ItemProperty -Path '%(path)s' -Name 'ObjectName' -Value "
            "'%(service_account)s' -Type String -Force;"
            "New-ItemProperty -Path '%(path)s' -Name 'Start' -Value "
            "%(start_mode)s -Type DWord -Force;"
            "New-ItemProperty -Path '%(path)s' -Name 'Type' -Value "
            "16 -Type DWord -Force;"
            "New-ItemProperty -Path '%(path)s' -Name 'ErrorControl' -Value "
            "0 -Type DWord -Force" %
            {"path": registry_path, "image_path": image_path,
             "display_name": display_name, "description": description,
             "depends_on": depends_on_ps, "service_account": service_account,
             "start_mode": start_mode},
            ignore_stdout=True)

    def run_user_script(self, user_script):
        if len(user_script) == 0:
            return

        script_path = "$env:TMP\\coriolis_user_script.ps1"
        try:
            utils.write_winrm_file(
                self._conn,
                script_path,
                user_script)
        except Exception as err:
            raise exception.CoriolisException(
                "Failed to copy user script to target system.") from err

        cmd = ('$ErrorActionPreference = "Stop"; powershell.exe '
               '-NonInteractive -ExecutionPolicy RemoteSigned '
               '-File "%(script)s" "%(os_root_dir)s"') % {
            "script": script_path,
            "os_root_dir": self._os_root_dir,
        }
        try:
            out = self._conn.exec_ps_command(cmd)
            LOG.debug("User script output: %s" % out)
        except Exception as err:
            raise exception.CoriolisException(
                "Failed to run user script.") from err

    def _disable_cloudbase_init(self):
        key_name = str(uuid.uuid4())
        self._load_registry_hive(
            "HKLM\\%s" % key_name,
            "%sWindows\\System32\\config\\SYSTEM" % self._os_root_dir)
        try:
            if self._check_cloudbase_init_exists(key_name):
                self._event_manager.progress_update(
                    "Disabling cloudbase-init")
                self._set_service_start_mode(
                    key_name, CLOUDBASEINIT_SERVICE_NAME,
                    SERVICE_START_DISABLED)
        finally:
            self._unload_registry_hive("HKLM\\%s" % key_name)

    def _check_cloudbase_init_exists(self, key_name):
        reg_service_path = (SERVICE_PATH_FORMAT %
                            (key_name, CLOUDBASEINIT_SERVICE_NAME))
        return self._conn.exec_ps_command(
            "Test-Path %s" % reg_service_path) == "True"

    def _setup_existing_cbslinit_service(self, key_name, image_path,
                                         service_account="LocalSystem"):
        reg_service_path = (SERVICE_PATH_FORMAT %
                            (key_name, CLOUDBASEINIT_SERVICE_NAME))
        self._conn.exec_ps_command(
            "Set-ItemProperty -Path '%s' -Name 'ImagePath' -Value '%s' "
            "-Force" % (reg_service_path, image_path))
        self._conn.exec_ps_command(
            "Set-ItemProperty -Path '%s' -Name 'ObjectName' "
            "-Value '%s' -Force" % (reg_service_path, service_account))

        self._set_service_start_mode(key_name, CLOUDBASEINIT_SERVICE_NAME,
                                     SERVICE_START_AUTO)

    def _get_cbslinit_base_dir(self):
        return "%sCloudbase-Init" % self._os_root_dir

    def _get_cbslinit_scripts_dir(self, base_dir):
        return ("%s\\LocalScripts" % base_dir)

    def _write_local_script(self, base_dir, script_path, priority=50):
        scripts_dir = self._get_cbslinit_scripts_dir(base_dir)
        script = "%s\\%d-%s" % (
            scripts_dir, priority,
            os.path.basename(script_path))

        with open(script_path, 'r') as fd:
            contents = fd.read()
            utils.write_winrm_file(
                self._conn, script, contents)

    def _write_cloudbase_init_conf(self, cloudbaseinit_base_dir,
                                   local_base_dir, com_port="COM1",
                                   metadata_services=None, plugins=None):
        if metadata_services is None:
            metadata_services = CLOUDBASE_INIT_DEFAULT_METADATA_SVCS

        if plugins is None:
            plugins = CLOUDBASE_INIT_DEFAULT_PLUGINS
        elif type(plugins) is not list:
            raise exception.CoriolisException(
                "Invalid plugins parameter. Must be list.")

        LOG.info("Writing Cloudbase-Init configuration files")
        conf_dir = "%s\\conf" % cloudbaseinit_base_dir
        scripts_dir = self._get_cbslinit_scripts_dir(
            cloudbaseinit_base_dir)
        self._conn.exec_ps_command("mkdir '%s' -Force" % conf_dir,
                                   ignore_stdout=True)
        self._conn.exec_ps_command("mkdir '%s' -Force" % scripts_dir,
                                   ignore_stdout=True)

        conf_file_path = "%s\\cloudbase-init.conf" % conf_dir

        conf_content = (
            "[DEFAULT]\r\n"
            "username = Admin\r\n"
            "groups = Administrators\r\n"
            "verbose = true\r\n"
            "bsdtar_path = %(bin_path)s\\bsdtar.exe\r\n"
            "mtools_path = %(bin_path)s\r\n"
            "logdir = %(log_path)s\r\n"
            "local_scripts_path = %(scripts_path)s\r\n"
            "stop_service_on_exit = false\r\n"
            "logfile = cloudbase-init.log\r\n"
            "default_log_levels = \r\n"
            "comtypes=INFO,suds=INFO,iso8601=WARN,requests=WARN\r\n"
            "allow_reboot = false\r\n"
            "plugins = %(plugins)s\r\n"
            "debug = true\r\n"
            "san_policy = OnlineAll\r\n"
            "metadata_services = %(metadata_services)s\r\n"
            "logging_serial_port_settings = %(com_port)s,9600,N,8\r\n" %
            {"bin_path": "%s\\Bin" % local_base_dir,
             "log_path": "%s\\Log" % local_base_dir,
             "scripts_path": "%s\\LocalScripts" % local_base_dir,
             "com_port": com_port,
             "metadata_services": ",".join(metadata_services),
             "plugins": ",".join(plugins)})

        utils.write_winrm_file(
            self._conn,
            conf_file_path,
            conf_content)

        disks_script = os.path.join(
            utils.get_resources_bin_dir(),
            "bring-disks-online.ps1")

        self._write_local_script(
            cloudbaseinit_base_dir, disks_script,
            priority=99)

    def _install_cloudbase_init(self, download_url,
                                metadata_services=None, enabled_plugins=None,
                                com_port="COM1"):
        self._event_manager.progress_update("Adding cloudbase-init")
        cloudbaseinit_base_dir = self._get_cbslinit_base_dir()

        key_name = str(uuid.uuid4())
        self._load_registry_hive(
            "HKLM\\%s" % key_name,
            "%sWindows\\System32\\config\\SYSTEM" % self._os_root_dir)
        try:
            cloudbaseinit_zip_path = "c:\\cloudbaseinit.zip"

            self._event_manager.progress_update("Downloading cloudbase-init")
            utils.retry_on_error(sleep_seconds=5)(
                self._conn.download_file)(
                    download_url,
                    cloudbaseinit_zip_path)

            self._event_manager.progress_update("Installing cloudbase-init")
            self._expand_archive(cloudbaseinit_zip_path,
                                 cloudbaseinit_base_dir)

            log_dir = "%s\\Log" % cloudbaseinit_base_dir
            self._conn.exec_ps_command("mkdir '%s' -Force" % log_dir,
                                       ignore_stdout=True)
            local_base_dir = "C%s" % cloudbaseinit_base_dir[1:]

            self._write_cloudbase_init_conf(
                cloudbaseinit_base_dir, local_base_dir,
                metadata_services=metadata_services,
                plugins=enabled_plugins, com_port=com_port)

            image_path = (
                '"%(path)s\\Bin\\OpenStackService.exe" cloudbase-init '
                '"%(path)s\\Python\\Python.exe" '
                '"%(path)s\\Python\\Scripts\\cloudbase-init.exe" '
                '--config-file "%(path)s\\conf\\cloudbase-init.conf"' % {
                    'path': local_base_dir})

            self._event_manager.progress_update("Enabling cloudbase-init")

            if self._check_cloudbase_init_exists(key_name):
                self._setup_existing_cbslinit_service(key_name, image_path)
            else:
                self._create_service(
                    key_name=key_name,
                    service_name=CLOUDBASEINIT_SERVICE_NAME,
                    image_path=image_path,
                    display_name="Cloud Initialization Service",
                    description="Service wrapper for cloudbase-init")
        finally:
            self._unload_registry_hive("HKLM\\%s" % key_name)

        return cloudbaseinit_base_dir

    def _compile_static_ip_conf_from_registry(self, key_name):
        ips_info = []
        interfaces_reg_path = INTERFACES_PATH_FORMAT % key_name
        interfaces = self._conn.exec_ps_command(
            "(((Get-ChildItem -Path '%s').Name | Select-String -Pattern "
            "'[^\\\\]+$').Matches).Value" % interfaces_reg_path)
        for interface in interfaces.splitlines():
            reg_path = '%s\\%s' % (interfaces_reg_path, interface)
            enable_dhcp = self._conn.exec_ps_command(
                "(Get-ItemProperty -Path '%s').EnableDHCP" % reg_path)
            if enable_dhcp == '0':
                ip_address = self._conn.exec_ps_command(
                    "(Get-ItemProperty -Path '%s').IPAddress" % reg_path)
                subnet_mask = self._conn.exec_ps_command(
                    "(Get-ItemProperty -Path '%s').SubnetMask" % reg_path)
                if not (ip_address and subnet_mask):
                    LOG.warning(
                        "No IP Address or Subnet Mask found for interface: "
                        "'%s'" % interface)
                    continue
                prefix_length = ipaddress.IPv4Network(
                    (0, subnet_mask)).prefixlen
                default_gateway = self._conn.exec_ps_command(
                    "(Get-ItemProperty -Path '%s').DefaultGateway" % reg_path)
                name_server = self._conn.exec_ps_command(
                    "(Get-ItemProperty -Path '%s').NameServer" % reg_path)
                ip_info = {
                    "ip_address": ip_address,
                    "prefix_length": prefix_length,
                    "default_gateway": default_gateway,
                    "dns_addresses": name_server}
                LOG.debug(
                    "Found static IP configuration for interface '%s': "
                    "%s" % (interface, ip_info))
                ips_info.append(ip_info)
            else:
                LOG.debug(
                    "Could not find a static IP configuration for interface: "
                    "'%s'" % interface)
                continue

        return ips_info

    def _get_static_nics_info(self, nics_info, ips_info):
        static_nics_info = []
        reg_ip_addresses = set()

        for ip_info in ips_info:
            if ip_info.get('ip_address'):
                reg_ip_addresses.add(ip_info['ip_address'])

        for nic in nics_info:
            static_nic = copy.deepcopy(nic)
            nic_ips = nic.get('ip_addresses', [])
            if not nic_ips:
                LOG.warning(
                    f"Skipping NIC ('{nic.get('mac_address')}'). It has no "
                    f"detected IP addresses")
                continue
            diff = set(nic_ips) - reg_ip_addresses
            if diff:
                LOG.warning(
                    f"The IP addresses {list(diff)} found on the source "
                    f"VM's NIC were not found in the registry. These IPs will "
                    f"be skipped in the static IP configuration process")
                ip_matches = list(reg_ip_addresses.intersection(set(nic_ips)))
                if not ip_matches:
                    LOG.warning(
                        f"Couldn't find any static IP configuration that "
                        f"matches the addresses {list(nic_ips)} of the source "
                        f"NIC ({nic.get('mac_address')}). Skipping")
                    continue
                static_nic['ip_addresses'] = ip_matches
            static_nics_info.append(static_nic)

        return static_nics_info

    def _write_static_ip_script(self, base_dir, nics_info, ips_info):
        scripts_dir = self._get_cbslinit_scripts_dir(base_dir)
        script_path = "%s\\01-static-ip-config.ps1" % scripts_dir
        nics_info_dump = json.dumps(nics_info)
        ips_info_dump = json.dumps(ips_info)
        contents = STATIC_IP_SCRIPT_TEMPLATE % {
            'nics_info': base64.b64encode(nics_info_dump.encode()).decode(),
            'ips_info': base64.b64encode(ips_info_dump.encode()).decode()}
        utils.write_winrm_file(self._conn, script_path, contents.encode())

    def set_net_config(self, nics_info, dhcp):
        if dhcp:
            return

        key_name = str(uuid.uuid4())
        self._load_registry_hive(
            "HKLM\\%s" % key_name,
            "%sWindows\\System32\\config\\SYSTEM" % self._os_root_dir)
        try:
            cbslinit_base_dir = self._get_cbslinit_base_dir()
            ips_info = self._compile_static_ip_conf_from_registry(key_name)
            LOG.debug(f"Registry static IP configuration: {ips_info}")
            static_nics_info = self._get_static_nics_info(nics_info, ips_info)
            LOG.debug(f"Detected static NICS info: {static_nics_info}")
            if static_nics_info:
                self._write_static_ip_script(
                    cbslinit_base_dir, static_nics_info, ips_info)
            else:
                LOG.warning(
                    "No static IP configuration found on the source VM. "
                    "Static IP configuration will be skipped.")
        finally:
            self._unload_registry_hive("HKLM\\%s" % key_name)

    def get_packages(self):
        return [], []

    def get_installed_packages(self):
        self.installed_packages = []

    def pre_packages_install(self, package_names):
        pass

    def install_packages(self, package_names):
        pass

    def post_packages_install(self, package_names):
        pass

    def pre_packages_uninstall(self, package_names):
        pass

    def uninstall_packages(self, package_names):
        pass

    def post_packages_uninstall(self, package_names):
        pass
