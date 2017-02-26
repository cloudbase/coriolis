# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import uuid

from distutils import version
from oslo_config import cfg
from oslo_log import log as logging

from coriolis import constants
from coriolis.osmorphing import windows as base_windows

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


class WindowsMorphingTools(base_windows.BaseWindowsMorphingTools):
    def pre_packages_install(self, packages_add):
        if (not self._hypervisor or
                self._hypervisor == constants.HYPERVISOR_KVM):
            self._add_virtio_drivers()

        if self._platform == constants.PLATFORM_OPENSTACK:
            self._add_cloudbase_init()
        else:
            self._disable_cloudbase_init()

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
