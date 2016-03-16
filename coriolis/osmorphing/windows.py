import os
import re
import uuid

from distutils import version
from oslo_config import cfg
from oslo_log import log as logging

from coriolis import constants
from coriolis import exception
from coriolis.osmorphing import base

opts = [
    cfg.StrOpt('virtio_iso_url',
               default='https://fedorapeople.org/groups/virt/virtio-win/'
               'direct-downloads/stable-virtio/virtio-win.iso',
               help="Location of the virtio-win ISO"),
    cfg.StrOpt('cloudbaseinit_x64_url',
               default="https://www.cloudbase.it/downloads/"
               "CloudbaseInitSetup_x64.zip",
               help="Location of the Cloudbase-Init ZIP for amd64 systems"),
    cfg.StrOpt('cloudbaseinit_x86_url',
               default="https://www.cloudbase.it/downloads/"
               "CloudbaseInitSetup_x86.zip",
               help="Location of the Cloudbase-Init ZIP for amd64 systems"),
]

CONF = cfg.CONF
CONF.register_opts(opts, 'windows_images')

LOG = logging.getLogger(__name__)

SERVICE_START_AUTO = 2
SERVICE_START_MANUAL = 3
SERVICE_START_DISABLED = 4

SERVICE_PATH_FORMAT = "HKLM:\\%s\\ControlSet001\\Services\\%s"
CLOUDBASEINIT_SERVICE_NAME = "cloudbase-init"


class WindowsMorphingTools(base.BaseOSMorphingTools):
    def _check_os(self):
        try:
            (self._version_number,
             self._edition_id,
             self._installation_type,
             self._product_name) = self._get_image_version_info()
            return ('Windows', self._product_name)
        except exception.CoriolisException as ex:
            LOG.debug("Exception during OS detection: %s", ex)

    def pre_packages_install(self):
        if (not self._hypervisor or
                self._hypervisor == constants.HYPERVISOR_KVM):
            self._add_virtio_drivers()

        if self._platform == constants.PLATFORM_OPENSTACK:
            self._add_cloudbase_init()
        else:
            self._disable_cloudbase_init()

    def set_net_config(self, nics_info, dhcp):
        # TODO: implement
        pass

    def _get_dism_path(self):
        return "c:\\Windows\\System32\\dism.exe"

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

    def _get_image_version_info(self):
        key_name = str(uuid.uuid4())

        self._load_registry_hive(
            "HKLM\%s" % key_name,
            "%sWindows\\System32\\config\\SOFTWARE" % self._os_root_dir)
        try:
            version_info_str = self._conn.exec_ps_command(
                "Get-ItemProperty "
                "'HKLM:\%s\Microsoft\Windows NT\CurrentVersion' "
                "| select CurrentVersion, CurrentMajorVersionNumber, "
                "CurrentMinorVersionNumber,  CurrentBuildNumber, "
                "InstallationType, ProductName, EditionID | FL" %
                key_name).replace(self._conn.EOL, os.linesep)
        finally:
            self._unload_registry_hive("HKLM\%s" % key_name)

        version_info = {}
        for n in ["CurrentVersion", "CurrentMajorVersionNumber",
                  "CurrentMinorVersionNumber", "CurrentBuildNumber",
                  "InstallationType", "ProductName", "EditionID"]:
            version_info[n] = self._get_ps_fl_value(version_info_str, n)

        if (not version_info["CurrentMajorVersionNumber"] and
                not version_info["CurrentVersion"]):
            raise exception.CoriolisException(
                "Cannot find Windows version info")

        if version_info["CurrentMajorVersionNumber"]:
            version_str = "%s.%s.%s" % (
                version_info["CurrentMajorVersionNumber"],
                version_info["CurrentMinorVersionNumber"],
                version_info["CurrentBuildNumber"])
        else:
            version_str = "%s.%s" % (
                version_info["CurrentVersion"],
                version_info["CurrentBuildNumber"])

        return (version.LooseVersion(version_str),
                version_info["EditionID"],
                version_info["InstallationType"],
                version_info["ProductName"])

    def _add_dism_driver(self, driver_path):
        LOG.info("Adding driver: %s" % driver_path)
        dism_path = self._get_dism_path()
        return self._conn.exec_command(
            dism_path,
            ["/add-driver", "/image:%s" % self._os_root_dir,
             "/driver:%s" % driver_path, "/recurse", "/forceunsigned"])

    def _mount_disk_image(self, path):
        LOG.info("Mounting disk image: %s" % path)
        return self._conn.exec_ps_command(
            "(Mount-DiskImage '%s' -PassThru | Get-Volume).DriveLetter" %
            path)

    def _dismount_disk_image(self, path):
        LOG.info("Unmounting disk image: %s" % path)
        self._conn.exec_ps_command("Dismount-DiskImage '%s'" % path,
                                   ignore_stdout=True)

    def _add_virtio_drivers(self):
        # TODO: add support for x86
        arch = "amd64"

        CLIENT = 1
        SERVER = 2

        # Ordered by version number
        virtio_dirs = [
            ("xp", version.LooseVersion("5.1"), CLIENT),
            ("2k3", version.LooseVersion("5.2"), SERVER),
            ("2k8", version.LooseVersion("6.0"), SERVER | CLIENT),
            ("w7", version.LooseVersion("6.1"), CLIENT),
            ("2k8R2", version.LooseVersion("6.1"), SERVER),
            ("w8", version.LooseVersion("6.2"), CLIENT),
            ("2k12", version.LooseVersion("6.2"), SERVER),
            ("w8.1", version.LooseVersion("6.3"), CLIENT),
            ("2k12R2", version.LooseVersion("6.3"), SERVER),
            ("w10", version.LooseVersion("10.0"), SERVER | CLIENT),
            ]

        # The list of all possible editions is huge, this is a semplification
        if "Server" in self._edition_id:
            edition_type = SERVER
        else:
            edition_type = CLIENT

        drivers = ["Balloon", "NetKVM", "qxl", "qxldod", "pvpanic", "viorng",
                   "vioscsi", "vioserial", "viostor"]

        self._event_manager.progress_update("Downloading virtio-win drivers")

        virtio_iso_path = "c:\\virtio-win.iso"
        self._conn.download_file(
            CONF.windows_images.virtio_iso_url, virtio_iso_path)

        self._event_manager.progress_update("Adding virtio-win drivers")

        virtio_drive = self._mount_disk_image(virtio_iso_path)
        try:
            for virtio_dir, dir_version, dir_edition_type in reversed(
                    virtio_dirs):
                if self._version_number >= dir_version and (
                        edition_type & dir_edition_type):
                    path = "%s:\\Balloon\\%s\\%s" % (
                        virtio_drive, virtio_dir, arch)
                    if self._conn.test_path(path):
                        break

            driver_paths = ["%s:\\%s\\%s\\%s" % (
                            virtio_drive, d, virtio_dir, arch)
                            for d in drivers]

            sid = self._get_sid()
            # Fails on Nano Server without explicitly granting permissions
            file_repo_path = ("%sWindows\System32\DriverStore\FileRepository" %
                              self._os_root_dir)
            self._grant_permissions(file_repo_path, "*%s" % sid)
            try:
                for driver_path in driver_paths:
                    if self._conn.test_path(driver_path):
                        self._add_dism_driver(driver_path)
            finally:
                self._revoke_permissions(file_repo_path, "*%s" % sid)
        finally:
            self._dismount_disk_image(virtio_iso_path)

    def _expand_archive(self, path, destination):
        LOG.info("Expanding archive \"%(path)s\" in \"%(destination)s\"",
                 {"path": path, "destination": destination})
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
            {"path": registry_path,
             "image_path": image_path,
             "display_name": display_name,
             "description": description,
             "depends_on": depends_on_ps,
             "service_account": service_account,
             "start_mode": start_mode},
            ignore_stdout=True)

    def _write_cloudbase_init_conf(self, cloudbaseinit_base_dir,
                                   local_base_dir, com_port="COM1"):
        LOG.info("Writing Cloudbase-Init configuration files")
        conf_dir = "%s\\conf" % cloudbaseinit_base_dir
        self._conn.exec_ps_command("mkdir '%s' -Force" % conf_dir,
                                   ignore_stdout=True)

        conf_file_path = "%s\\cloudbase-init.conf" % conf_dir

        conf_content = (
            "[DEFAULT]\n"
            "username = Admin\n"
            "groups = Administrators\n"
            "inject_user_password = true\n"
            "config_drive_raw_hhd = true\n"
            "config_drive_cdrom = true\n"
            "config_drive_vfat = true\n"
            "bsdtar_path = %(bin_path)s\\bsdtar.exe\n"
            "mtools_path = %(bin_path)s\n"
            "logdir = %(log_path)s\n"
            "logfile = cloudbase-init.log\n"
            "default_log_levels = "
            "comtypes=INFO,suds=INFO,iso8601=WARN,requests=WARN\n"
            "mtu_use_dhcp_config = true\n"
            "ntp_use_dhcp_config = true\n"
            "allow_reboot = true\n"
            "debug = true\n"
            "logging_serial_port_settings = %(com_port)s,115200,N,8\n" %
            {"bin_path": "%s\\Bin" % local_base_dir,
             "log_path": "%s\\Log" % local_base_dir,
             "com_port": com_port})

        self._conn.write_file(conf_file_path, conf_content.encode())

    def _check_cloudbase_init_exists(self, key_name):
        reg_service_path = (SERVICE_PATH_FORMAT %
                            (key_name, CLOUDBASEINIT_SERVICE_NAME))
        return self._conn.exec_ps_command(
            "Test-Path %s" % reg_service_path) == "True"

    def _disable_cloudbase_init(self):
        key_name = str(uuid.uuid4())
        self._load_registry_hive(
            "HKLM\%s" % key_name,
            "%sWindows\\System32\\config\\SYSTEM" % self._os_root_dir)
        try:
            if self._check_cloudbase_init_exists(key_name):
                self._event_manager.progress_update(
                    "Disabling cloudbase-init")
                self._set_service_start_mode(
                    key_name, CLOUDBASEINIT_SERVICE_NAME,
                    SERVICE_START_DISABLED)
        finally:
            self._unload_registry_hive("HKLM\%s" % key_name)

    def _add_cloudbase_init(self):
        # TODO: add support for x86
        arch = "amd64"
        arch_url_map = {"amd64": CONF.windows_images.cloudbaseinit_x64_url,
                        "x86": CONF.windows_images.cloudbaseinit_x86_url}

        self._event_manager.progress_update("Adding cloudbase-init")

        key_name = str(uuid.uuid4())
        self._load_registry_hive(
            "HKLM\%s" % key_name,
            "%sWindows\\System32\\config\\SYSTEM" % self._os_root_dir)
        try:
            if self._check_cloudbase_init_exists(key_name):
                self._event_manager.progress_update(
                    "Enabling cloudbase-init")
                self._set_service_start_mode(
                    key_name, CLOUDBASEINIT_SERVICE_NAME, SERVICE_START_AUTO)
            else:
                cloudbaseinit_zip_path = "c:\\cloudbaseinit.zip"
                cloudbaseinit_base_dir = "%sCloudbase-Init" % self._os_root_dir

                self._event_manager.progress_update(
                    "Downloading cloudbase-init")
                self._conn.download_file(arch_url_map[arch],
                                         cloudbaseinit_zip_path)

                self._event_manager.progress_update(
                    "Installing cloudbase-init")
                self._expand_archive(cloudbaseinit_zip_path,
                                     cloudbaseinit_base_dir)

                log_dir = "%s\\Log" % cloudbaseinit_base_dir
                self._conn.exec_ps_command("mkdir '%s' -Force" % log_dir,
                                           ignore_stdout=True)

                local_base_dir = "C%s" % cloudbaseinit_base_dir[1:]
                self._write_cloudbase_init_conf(
                    cloudbaseinit_base_dir, local_base_dir)

                image_path = (
                    '""""%(path)s\\Bin\\OpenStackService.exe"""" '
                    'cloudbase-init """"%(path)s\\Python\\Python.exe"""" -c '
                    '""""from cloudbaseinit import shell;shell.main()"""" '
                    '--config-file """"%(path)s\\conf\\cloudbase-init.conf""""'
                    % {'path': local_base_dir})

                self._create_service(
                    key_name=key_name,
                    service_name=CLOUDBASEINIT_SERVICE_NAME,
                    image_path=image_path,
                    display_name="Cloud Initialization Service",
                    description="Service wrapper for cloudbase-init")
        finally:
            self._unload_registry_hive("HKLM\%s" % key_name)
