# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import re
import uuid

from oslo_log import log as logging

from coriolis import exception
from coriolis.osmorphing.osmount import base
from coriolis import utils
from coriolis import wsman

LOG = logging.getLogger(__name__)


class WindowsMountTools(base.BaseOSMountTools):
    def _connect(self):
        connection_info = self._connection_info

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

    def _run_diskpart_script(self, script):
        """ Writes the given script data to a file and runs diskpart.exe,
        returning the output. """
        tempdir = self._conn.exec_ps_command("$env:TEMP")

        # NOTE: diskpart is interactive, so we must either pipe it its input
        # or write a couple of one-line files and use `diskpart /s`
        filepath = r"%s\%s.txt" % (tempdir, uuid.uuid4())
        self._conn.write_file(filepath, bytes(script, 'utf-8'))

        return self._conn.exec_ps_command("diskpart.exe /s '%s'" % filepath)

    def _service_disks_with_status(
            self, status, service_script_with_id_fmt,
            logmsg_fmt="Operating on disk with index '%s'"):
        """ Uses diskpart.exe to detect all disks with the given 'status', and
        execute the given service script after formatting the disk ID in. """
        disk_list_script = "LIST DISK\r\nEXIT"

        disk_entry_re = r"\s+Disk (%s)\s+%s\s+"
        search_disk_entry_re = disk_entry_re % ("[0-9]+", status)
        # NOTE: some disk operations performed on one disk may have an effect
        # on others (ex: importing a Dynamic Disk which is part of a Dynamic
        # Disk Group also imports the other ones), so we must take care to
        # re-status before performing any operation on any disk => O(n**2)
        disk_list = self._run_diskpart_script(disk_list_script)
        servicable_disk_ids = [m.group(1) for m in [
            re.match(search_disk_entry_re, l) for l in disk_list.split("\r\n")]
            if m is not None]
        for disk_id in servicable_disk_ids:
            curr_disk_entry_re = disk_entry_re % (disk_id, status)

            disk_list = self._run_diskpart_script("LIST DISK\r\nEXIT")
            for line in disk_list.split("\r\n"):
                if re.match(curr_disk_entry_re, line):
                    LOG.info(logmsg_fmt, disk_id)
                    self._run_diskpart_script(
                        service_script_with_id_fmt % disk_id)
                    break

    def _set_foreign_disks_rw_mode(self):
        # NOTE: in case a Dynamic Disk (which will show up as 'Foreign' to
        # the worker) is part of a Dynamic Disk group, ALL disks from that
        # group must be R/W in order to import it (importing one will
        # trigger the importing of all of them)
        set_rw_foreign_disk_script_fmt = (
            "SELECT DISK %s\r\nATTRIBUTES DISK CLEAR READONLY\r\nEXIT")
        self._service_disks_with_status(
            "Foreign", set_rw_foreign_disk_script_fmt,
            logmsg_fmt="Clearing R/O flag on foreign disk with ID '%s'.")

    def _import_foreign_disks(self):
        """ Uses diskpart.exe to import all disks which are foreign to the
        worker. Needed when servicing installations on Dynamic Disks. """
        # NOTE: foreign disks are not exposed via the APIs the PowerShell
        # disk cmdlets use, thus any disk which is foreign is is likely
        # still RO, which is why we must change the RO attribute as well:
        import_disk_script_fmt = (
            "SELECT DISK %s\r\nIMPORT\r\nEXIT")
        self._service_disks_with_status(
            "Foreign", import_disk_script_fmt,
            logmsg_fmt="Importing foreign disk with ID '%s'.")

    def _bring_all_disks_online(self):
        online_disk_script_fmt = "SELECT DISK %s\r\nONLINE DISK\r\nEXIT"
        self._service_disks_with_status(
            "Offline", online_disk_script_fmt,
            logmsg_fmt="Bringing offline disk with ID %s online.")

    def _set_basic_disks_rw_mode(self):
        # NOTE: The PowerShell cmdlets use APIs which do not expose foreign
        # disks at all (Dynamic disks will always show up as foreign to the
        # worker), thus this method will only work for basic disks.
        # Dynamic Disks are set to R/W upon their importing in
        # self._import_foreign_disks()
        LOG.info("Setting RW mode on RO basic disks")
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
        self._set_basic_disks_rw_mode()
        self._set_foreign_disks_rw_mode()
        self._import_foreign_disks()
        self._refresh_storage()
        fs_roots = self._get_fs_roots()
        system_drive = self._get_system_drive()

        for fs_root in [r for r in fs_roots if not r[:-1] == system_drive]:
            if self._conn.test_path("%sWindows\\System32" % fs_root):
                return fs_root, [], None

        raise exception.OperatingSystemNotFound("root partition not found")

    def dismount_os(self, dirs):
        for dir in dirs:
            drive_letter = dir.split(":")[0]
            self._bring_disk_offline(drive_letter)
