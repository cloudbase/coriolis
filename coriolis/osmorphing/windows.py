import os
import re

from distutils import version
from oslo_config import cfg
from oslo_log import log as logging

from coriolis import constants
from coriolis import exception
from coriolis.osmorphing import base

opts = [
    cfg.StrOpt('virtio_iso_url',
               default='https://fedorapeople.org/groups/virt/virtio-win/'
               'direct-downloads/latest-virtio/virtio-win.iso',
               help="Location of the virtio-win ISO"),
]

CONF = cfg.CONF
CONF.register_opts(opts, 'windows_images')

LOG = logging.getLogger(__name__)


class WindowsMorphingTools(base.BaseOSMorphingTools):
    def _check_os(self):
        try:
            version = self._get_image_version()
            return ('Windows', version)
        except exception.CoriolisException:
            pass

    def pre_packages_install(self):
        if self._hypervisor == constants.HYPERVISOR_KVM:
            self._add_virtio_drivers()

        if self._platform == constants.PLATFORM_OPENSTACK:
            self._add_cloudbase_init()

    def set_net_config(self, nics_info, dhcp):
        # TODO: implement
        pass

    def _get_dism_features(self):
        LOG.info("Getting image features")
        return self._conn.exec_command(
            "dism.exe", ["/get-features",  "/image:%s" % self._os_root_dir])

    def _add_dism_driver(self, driver_path):
        LOG.info("Adding driver: %s" % driver_path)
        return self._conn.exec_command(
            "dism.exe /add-driver /image:%s /driver:%s /recurse /forceunsigned"
            % (self._os_root_dir, driver_path))

    def _get_image_version(self):
        features = self._get_dism_features()
        m = re.search(r'^Image Version: (.*)$',
                      features.replace(self._conn.EOL, os.linesep),
                      re.MULTILINE)
        if not m:
            raise exception.CoriolisException("Could not find OS version")
        return m.groups()[0]

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
        self._event_manager.progress_update("Adding virtio-win drivers")

        # TODO: add support for x86
        arch = "amd64"

        # Ordered by version number
        # TODO(alexpilotti): distinguish client and server
        virtio_dirs = [
            ("xp", version.LooseVersion("5.1")),
            ("2k3", version.LooseVersion("5.2")),
            ("2k8", version.LooseVersion("6.0")),
            ("w7", version.LooseVersion("6.1")),
            ("2k8R2", version.LooseVersion("6.1")),
            ("w8", version.LooseVersion("6.2")),
            ("2k12", version.LooseVersion("6.2")),
            ("w8.1", version.LooseVersion("6.3")),
            ("2k12R2", version.LooseVersion("6.3")),
            ("w10", version.LooseVersion("10.0")),
            ]

        drivers = ["Balloon", "NetKVM", "qxldod", "pvpanic", "viorng",
                   "vioscsi", "vioserial", "viostor"]

        virtio_iso_path = "c:\\virtio-win.iso"
        self._conn.download_file(
            CONF.windows_images.virtio_iso_url, virtio_iso_path)

        virtio_drive = self._mount_disk_image(virtio_iso_path)
        try:
            image_version = version.LooseVersion(self._version)

            for virtio_dir, dir_version in reversed(virtio_dirs):
                if image_version >= dir_version:
                    path = "%s:\\Balloon\\%s\\%s" % (
                        virtio_drive, virtio_dir, arch)
                    if self._conn.test_path(path):
                        break

            driver_paths = ["%s:\\%s\\%s\\%s" % (
                            virtio_drive, d, virtio_dir, arch)
                            for d in drivers]
            for driver_path in driver_paths:
                self._add_dism_driver(driver_path)
        finally:
            self._dismount_disk_image(virtio_iso_path)

    def _add_cloudbase_init(self):
        self._event_manager.progress_update("Adding cloudbase-init")
