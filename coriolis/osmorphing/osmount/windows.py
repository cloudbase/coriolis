import os
import re

from oslo_log import log as logging

from coriolis import exception
from coriolis.osmorphing.osmount import base
from coriolis import utils
from coriolis import wsman

LOG = logging.getLogger(__name__)


class WindowsMountTools(base.BaseOSMountTools):
    def _connect(self, connection_info):
        host = connection_info["ip"]
        port = connection_info.get("port", 5986)
        username = connection_info["username"]
        password = connection_info.get("password")
        cert_pem = connection_info.get("cert_pem")
        cert_key_pem = connection_info.get("cert_key_pem")
        url = "https://%s:%s/wsman" % (host, port)

        LOG.info("Connection info: %s", str(connection_info))

        LOG.info("Waiting for connectivity on host: %(host)s:%(port)s",
                 {"host": host, "port": port})
        utils.wait_for_port_connectivity(host, port)

        self._event_manager.progress_update(
            "Connecting to WinRM host: %(host)s:%(port)s" %
            {"host": host, "port": port})

        conn = wsman.WSManConnection()
        conn.connect(url=url, username=username, password=password,
                     cert_pem=cert_pem, cert_key_pem=cert_key_pem)
        self._conn = conn

    def get_connection(self):
        return self._conn

    def check_os(self):
        try:
            version_info = self._conn.exec_ps_command(
                "(get-ciminstance Win32_OperatingSystem).Caption")
            LOG.debug("Windows version: %s", version_info)
            return True
        except exception.CoriolisException:
            pass

    def _refresh_storage(self):
        self._conn.exec_ps_command(
            "Update-HostStorageCache", ignore_stdout=True)

    def _bring_all_disks_online(self):
        LOG.info("Bringing offline disks online")
        self._conn.exec_ps_command(
            "Get-Disk |? IsOffline | Set-Disk -IsOffline $False",
            ignore_stdout=True)

    def _set_all_disks_rw_mode(self):
        LOG.info("Setting RW mode on RO disks")
        self._conn.exec_ps_command(
            "Get-Disk |? IsReadOnly | Set-Disk -IsReadOnly $False",
            ignore_stdout=True)

    def _bring_disk_offline(self, drive_letter):
        self._conn.exec_ps_command(
            "Get-Volume |? DriveLetter -eq \"%s\" | Get-Partition | "
            "Get-Disk | Set-Disk -IsOffline $True" % drive_letter,
            ignore_stdout=True)

    def _get_system_drive(self):
        return self._conn.exec_ps_command("$env:SystemDrive")

    def _get_fs_roots(self):
        return self._conn.exec_ps_command(
            "(get-psdrive -PSProvider FileSystem).Root").split(self._conn.EOL)

    def mount_os(self):
        self._refresh_storage()
        self._bring_all_disks_online()
        self._set_all_disks_rw_mode()
        fs_roots = self._get_fs_roots()
        system_drive = self._get_system_drive()

        for fs_root in [r for r in fs_roots if not r[:-1] == system_drive]:
            if self._conn.test_path("%sWindows\\System32" % fs_root):
                return fs_root, []

    def dismount_os(self, dirs):
        for dir in dirs:
            drive_letter = dir.split(":")[0]
            self._bring_disk_offline(drive_letter)
