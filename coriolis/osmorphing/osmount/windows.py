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
        self._event_manager.progress_update(
            "Connecting to WinRM host: %(host)s:%(port)s" %
            {"host": host, "port": port})

        self._conn = wsman.WSManConnection.from_connection_info(
            connection_info, self._osmount_operation_timeout)

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
        self._run_diskpart_script("RESCAN")

    def _run_diskpart_script(self, script):
        """Executes the given script with diskpart.exe.

        Writes the given script data to a file and runs diskpart.exe,
        returning the output.
        """
        tempdir = self._conn.exec_ps_command("$env:TEMP")

        # NOTE: diskpart is interactive, so we must either pipe it its input
        # or write a couple of one-line files and use `diskpart /s`
        filepath = r"%s\%s.txt" % (tempdir, uuid.uuid4())
        self._conn.write_file(filepath, bytes(script, 'utf-8'))

        return self._conn.exec_ps_command("diskpart.exe /s '%s'" % filepath)

    def _service_disks_with_status(
            self, status, service_script_with_id_fmt, skip_on_error=False,
            logmsg_fmt="Operating on disk with index '%s'",
            disk_ids_to_skip=None):
        """Executes given service script on detected disks.

        Uses diskpart.exe to detect all disks with the given 'status', and
        execute the given service script after formatting the disk ID in.
        """
        if disk_ids_to_skip is None:
            disk_ids_to_skip = []
        disk_list_script = "LIST DISK\r\nEXIT"

        disk_entry_re = r"\s+Disk (%s)\s+%s\s+"
        search_disk_entry_re = disk_entry_re % ("[0-9]+", status)
        # NOTE: some disk operations performed on one disk may have an effect
        # on others (ex: importing a Dynamic Disk which is part of a Dynamic
        # Disk Group also imports the other ones), so we must take care to
        # re-status before performing any operation on any disk => O(n**2)
        disk_list = self._run_diskpart_script(disk_list_script)
        servicable_disk_ids = [
            m.group(1)
            for m
            in
            [re.match(search_disk_entry_re, d)
             for d in disk_list.split("\r\n")] if m is not None]
        LOG.debug(
            "Servicing disks with status '%s' (%s) from disk list: %s",
            status, servicable_disk_ids, disk_list)
        for disk_id in servicable_disk_ids:
            if disk_id in disk_ids_to_skip:
                LOG.warn('Skipping disk with ID: %s', disk_id)
                continue
            curr_disk_entry_re = disk_entry_re % (disk_id, status)

            disk_list = self._run_diskpart_script(disk_list_script)
            for line in disk_list.split("\r\n"):
                if re.match(curr_disk_entry_re, line):
                    LOG.info(logmsg_fmt, disk_id)
                    script = service_script_with_id_fmt % disk_id
                    try:
                        self._run_diskpart_script(script)
                    except Exception as ex:
                        if skip_on_error:
                            LOG.warn(
                                "Exception ocurred while servicing disk '%s' "
                                "with status '%s'.Skipping running script '%s'"
                                ". Error message: %s" %
                                (disk_id, status, script, ex))
                        else:
                            raise
                    break

    def _get_disk_ids_from_drive_letters(self, drive_letters):
        disk_ids = []
        volume_entry_re = r"^\s+Volume ([0-9]+)\s+([A-Z]+)\s+"

        def _get_disk_ids(vol_id):
            vol_detail_script = (
                "SELECT VOLUME %s\r\nDETAIL VOLUME\r\nEXIT" % vol_id)
            vol_details = self._run_diskpart_script(vol_detail_script)
            vol_disk_re = r"^\*?\s+Disk ([0-9]+)\s+"
            return [m.group(1) for m in [
                re.match(vol_disk_re, v) for v in vol_details.split('\r\n')]
                if m is not None]

        volume_list_script = "LIST VOLUME\r\nEXIT"
        vol_list = self._run_diskpart_script(volume_list_script)
        for volume in vol_list.split('\r\n'):
            m = re.match(volume_entry_re, volume)
            if m:
                vol_id, letter = m.groups()
                if letter in drive_letters:
                    disk_ids += _get_disk_ids(vol_id)

        return disk_ids

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
        """Imports foreign disks.

        Uses diskpart.exe to import all disks which are foreign to the
        worker. Needed when servicing installations on Dynamic Disks.
        """
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
        # NOTE (aznashwan): there is a chance that some disks on Windows
        # worker VMs won't be able to be brought online, but they may not be
        # the boot disk and thus can be skipped on error and still have a good
        # chance that the OSMorphing process will complete successfully.
        self._service_disks_with_status(
            "Offline", online_disk_script_fmt, skip_on_error=True,
            logmsg_fmt="Bringing offline disk with ID %s online.")

    def _set_basic_disks_rw_mode(self):
        set_rw_foreign_disk_script_fmt = (
            "SELECT DISK %s\r\nATTRIBUTES DISK CLEAR READONLY\r\nEXIT")
        self._service_disks_with_status(
            "Online", set_rw_foreign_disk_script_fmt,
            logmsg_fmt="Clearing R/O flag on disk with ID '%s'.",
            skip_on_error=True)

    def _bring_disks_offline(self, drives_to_skip=None):
        disk_ids_to_skip = None
        if drives_to_skip:
            disk_ids_to_skip = self._get_disk_ids_from_drive_letters(
                drives_to_skip)
        offline_disk_script_fmt = "SELECT DISK %s\r\nOFFLINE DISK\r\nEXIT"
        self._service_disks_with_status(
            "Online", offline_disk_script_fmt, skip_on_error=True,
            logmsg_fmt="Bringing online disk with ID %s offline.",
            disk_ids_to_skip=disk_ids_to_skip)

    def _get_system_drive(self):
        return self._conn.exec_ps_command("$env:SystemDrive")

    def _get_fs_roots(self, fail_if_empty=False):
        drives = self._conn.exec_ps_command(
            "(get-psdrive -PSProvider FileSystem).Root").split(self._conn.EOL)
        if len(drives) == 0 and fail_if_empty:
            raise exception.CoriolisException("No filesystems found")
        return drives

    def _bring_nonsystem_disks_offline(self):
        drives = self._ignore_devices.copy()
        drives.append(self._get_system_drive())
        drives_to_skip = [ltr.split(":")[0] for ltr in drives]
        self._bring_disks_offline(drives_to_skip)

    def _rebring_disks_online(self):
        self._bring_nonsystem_disks_offline()
        self._bring_all_disks_online()

    def _set_volumes_drive_letter(self):
        enable_default_drive_letter_script_fmt = (
            "SELECT VOLUME %s\r\n"
            "ATTRIBUTES VOLUME CLEAR NODEFAULTDRIVELETTER\r\nEXIT")
        volume_list_script = "LIST VOLUME\r\nEXIT"
        volume_entry_re = r"\s+Volume ([0-9]+)\s+(.*)"

        volume_list = self._run_diskpart_script(volume_list_script)
        unhidden_volume_ids = [m.group(1) for m in [
            re.match(volume_entry_re, v) for v in volume_list.split("\r\n")]
            if m is not None and "HIDDEN" not in m.group(2).upper()]
        for vol_id in unhidden_volume_ids:
            try:
                LOG.info(
                    "Clearing NODEFAULTDRIVELETTER flag on volume %s" %
                    vol_id)
                script = enable_default_drive_letter_script_fmt % vol_id
                self._run_diskpart_script(script)
            except Exception as ex:
                LOG.warn(
                    "Exception occurred while clearing flags on volume '%s'. "
                    "Skipping running script '%s'. Error message: %s" % (
                        vol_id, script, ex))
        self._rebring_disks_online()

    def mount_os(self):
        self._refresh_storage()
        self._bring_all_disks_online()
        self._set_basic_disks_rw_mode()
        self._set_foreign_disks_rw_mode()
        self._import_foreign_disks()
        self._set_volumes_drive_letter()
        self._refresh_storage()
        fs_roots = utils.retry_on_error(sleep_seconds=5)(self._get_fs_roots)(
            fail_if_empty=True)
        system_drive = self._get_system_drive()

        for fs_root in [r for r in fs_roots if not r[:-1] == system_drive]:
            if self._conn.test_path("%sWindows\\System32" % fs_root):
                return fs_root, None

        raise exception.OperatingSystemNotFound("root partition not found")

    def dismount_os(self, root_drive):
        self._bring_nonsystem_disks_offline()
