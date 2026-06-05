# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import re
import uuid

from oslo_log import log as logging

from coriolis import constants
from coriolis import exception
from coriolis.osmorphing.osmount import base
from coriolis import utils
from coriolis import wsman

LOG = logging.getLogger(__name__)


class WindowsMountTools(base.BaseOSMountTools):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # A list of BitLocker encrypted volumes that were unlocked
        # by us. We'll use a first-boot script to resume BitLocker.
        self._unlocked_volumes: list[str] = []

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
        except exception.NotAuthorized:
            # NOTE: Unauthorized exceptions should be propagated.
            raise
        except exception.CoriolisException:
            LOG.debug(
                "Failed Windows OSMount OS check: %s",
                utils.get_exception_details())
            pass
        return False

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
        # disk cmdlets use, thus any disk which is foreign is likely
        # still RO, which is why we must change the RO attribute as well:
        import_disk_script_fmt = (
            "SELECT DISK %s\r\nIMPORT\r\nEXIT")
        self._service_disks_with_status(
            "Foreign", import_disk_script_fmt,
            logmsg_fmt="Importing foreign disk with ID '%s'.")

    def _bring_disks_online(self, disk_nums=None):
        if disk_nums is None:
            disk_nums = self._conn.exec_ps_command(
                "(Get-Disk | Where-Object { $_.IsOffline -eq $True }"
                ").Number").splitlines()
        for disk_num in disk_nums:
            try:
                self._conn.exec_ps_command(
                    f"Set-Disk -IsOffline $False {disk_num}")
            except exception.CoriolisException:
                LOG.warning(
                    f"Failed setting disk {disk_num} online. Error was: "
                    f"{utils.get_exception_details()}")

    def _set_basic_disks_rw_mode(self):
        read_only_disk_nums = self._conn.exec_ps_command(
            "(Get-Disk | Where-Object { $_.IsReadOnly -eq $True }).Number")
        for disk_num in read_only_disk_nums.splitlines():
            try:
                self._conn.exec_ps_command(
                    f"Set-Disk -IsReadOnly $False {disk_num}")
            except exception.CoriolisException:
                LOG.warning(
                    f"Failed setting disk {disk_num} RW flag. Error was: "
                    f"{utils.get_exception_details()}")

    def _get_system_drive(self):
        return self._conn.exec_ps_command("$env:SystemDrive")

    def _get_fs_roots(self, fail_if_empty=False):
        drives = self._conn.exec_ps_command(
            "(get-psdrive -PSProvider FileSystem).Root").split(self._conn.EOL)
        if len(drives) == 0 and fail_if_empty:
            raise exception.CoriolisException("No filesystems found")
        return drives

    def _bring_nonboot_disks_offline(self, disk_nums=None):
        if disk_nums is None:
            disk_nums = self._conn.exec_ps_command(
                "(Get-Disk | Where-Object { $_.IsBoot -eq $False }"
                ").Number").splitlines()
        for disk_num in disk_nums:
            try:
                self._conn.exec_ps_command(
                    "Set-Disk -IsOffline $True %s" % disk_num)
            except exception.CoriolisException:
                LOG.warning(
                    "Failed setting disk %s offline. Error was: %s",
                    disk_num, utils.get_exception_details())

    def _rebring_disks_online(self, disk_nums=None):
        self._bring_nonboot_disks_offline(disk_nums=disk_nums)
        self._bring_disks_online(disk_nums=disk_nums)

    def _set_volumes_drive_letter(self):
        disk_nums = []
        partitions = self._conn.exec_ps_command(
            'Get-Partition | Where-Object { $_.Type -eq "Basic" -and '
            '$_.NoDefaultDriveLetter -eq $True } | Select-Object -Property '
            'DiskNumber,PartitionNumber')
        if partitions:
            LOG.debug(f"Partitions without default drive letter: {partitions}")
            for part in partitions.splitlines():
                part_line = part.split()
                if not len(part_line) > 1:
                    LOG.debug(f"Skipping partition line: {part_line}")
                    continue
                disk_num, part_num = part_line[:2]
                if not disk_num.isnumeric() or not part_num.isnumeric():
                    LOG.debug(f"Skipping partition line: {part_line}")
                    continue
                try:
                    self._conn.exec_ps_command(
                        f'Set-Partition -NoDefaultDriveLetter $False '
                        f'-DiskNumber {disk_num} -PartitionNumber {part_num}')
                    disk_nums.append(disk_num)
                except exception.CoriolisException:
                    LOG.warning(
                        f"Failed setting default drive letter on partition "
                        f"number '{part_num}' of disk number '{disk_num}'. "
                        f"Error was: {utils.get_exception_details()}")
            self._rebring_disks_online(disk_nums=disk_nums)

    def _get_encrypted_volume_ids(self):
        out = self._conn.exec_ps_command(
            'gwmi -ns "Root\\CIMV2\\Security\\MicrosoftVolumeEncryption" '
            '-class Win32_EncryptableVolume | % {$_.DeviceID}')
        return [x for x in out.replace("\r\n", "\n").split("\n") if x]

    def _unlock_encrypted_volume(self, volume_id: str, recovery_password: str):
        self._conn.exec_ps_command(
            f'manage-bde -unlock "{volume_id}" '
            f'-RecoveryPassword "{recovery_password}"')

    def _suspend_bitlocker(self, volume_id: str):
        """Suspend BitLocker until the next reboot for a given volume.

        It doesn't decrypt the device, it just adds a publicly accessible
        BitLocker protector that automatically unlocks the volume.

        When the replica instance boots, the TPM protector will be reconfigured
        automatically. Unfortunately the '-RebootCount' parameter isn't
        honored, perhaps due to the fact that the disks are attached to a
        separate VM. For this reason, we'll use a first-boot script to resume
        BitLocker explicitly.
        """
        self._conn.exec_ps_command(f'Suspend-BitLocker "{volume_id}"')

    def _unlock_encrypted_volumes(self):
        recovery_password = self._osmorphing_info.get(
            constants.ENCRYPTED_DISKS_PASS)
        if not recovery_password:
            LOG.info("No encrypted disk password specified, "
                     "skipping BitLocker unlock.")
            return

        encrypted_volume_ids = self._get_encrypted_volume_ids()
        if not encrypted_volume_ids:
            LOG.warning("Received encrypted disk password but no "
                        "BitLocker encrypted volumes found.")
            return

        unlocked = False
        for encrypted_volume_id in encrypted_volume_ids:
            try:
                self._unlock_encrypted_volume(
                    encrypted_volume_id, recovery_password)
                LOG.info(
                    "Successfully unlocked BitLocker encrypted volume: %s",
                    encrypted_volume_id)
                unlocked = True
            except Exception:
                LOG.info(
                    "Could not unlock volume %s using the specified "
                    "recovery password.",
                    encrypted_volume_id)
                continue

            # Suspend BitLocker until the replica boots.
            #
            # We'll intentionally propagate the failure if we managed to
            # unlock the volume but failed to suspend BitLocker.
            self._suspend_bitlocker(encrypted_volume_id)
            self._unlocked_volumes.append(encrypted_volume_id)

        if not unlocked:
            raise exception.CoriolisException(
                "Could not unlock any volume using the specified "
                "BitLocker recovery password.")

    def install_encryption_firstboot_setup(
        self,
        os_root_dir,
        os_morphing_tools,
    ):
        if not self._unlocked_volumes:
            LOG.info(
                "No unlocked BitLocker volumes, skipping first-boot setup.")
            return

        # We'll inject a first-boot script to resume BitLocker explicitly.
        # Unfortunately the "-RebootCount" parameter of "Suspend-BitLocker"
        # isn't honored, perhaps due to the fact that the disks are attached
        # to a different VM.
        script_content = ""
        for encrypted_volume_id in self._unlocked_volumes:
            LOG.info(
                "Resuming BitLocker after first boot, volume: %s",
                encrypted_volume_id)
            script_content += f'Resume-BitLocker "{encrypted_volume_id}"\r\n'

        # Resume BitLocker after bringing the disks online, which has a script
        # priority of 10.
        os_morphing_tools.register_firstboot_script(
            script_content,
            user_provided=False,
            script_filename="11-bitlocker-firstboot.ps1")

    def mount_os(self):
        self._set_basic_disks_rw_mode()
        self._bring_disks_online()
        self._unlock_encrypted_volumes()
        self._set_volumes_drive_letter()
        fs_roots = utils.retry_on_error(sleep_seconds=5)(self._get_fs_roots)(
            fail_if_empty=True)
        system_drive = self._get_system_drive()

        for fs_root in [r for r in fs_roots if not r[:-1] == system_drive]:
            if self._conn.test_path("%sWindows\\System32" % fs_root):
                return fs_root, None

        raise exception.OperatingSystemNotFound("root partition not found")

    def dismount_os(self, root_drive):
        self._bring_nonboot_disks_offline()

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

        cmd = f'& "{script_path}"; exit $LASTEXITCODE'
        try:
            out = self._conn.exec_ps_command(cmd)
            LOG.debug("User script output: %s" % out)
        except Exception as err:
            raise exception.CoriolisException(
                "Failed to run user script.") from err
