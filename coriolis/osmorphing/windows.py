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
]

CONF = cfg.CONF
CONF.register_opts(opts, 'windows_images')

LOG = logging.getLogger(__name__)


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

    def set_net_config(self, nics_info, dhcp):
        # TODO: implement
        pass

    def _get_dism_path(self):
        return "c:\\Windows\\System32\\dism.exe"

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
            for driver_path in driver_paths:
                if self._conn.test_path(driver_path):
                    self._add_dism_driver(driver_path)
        finally:
            self._dismount_disk_image(virtio_iso_path)

    def _add_cloudbase_init(self):
        self._event_manager.progress_update("Adding cloudbase-init")
