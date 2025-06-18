# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

from coriolis.osmorphing.osmount import windows
from coriolis.tests import test_base

GET_PARTITION_OUTPUT = """
disknumber partitionnumber
---------- ---------------
         2               2
         3               4"""


class CoriolisTestException(Exception):
    pass


class WindowsMountToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the WindowsMountTools class."""

    @mock.patch.object(windows.wsman, 'WSManConnection')
    def setUp(self, mock_wsman_connection):
        super(WindowsMountToolsTestCase, self).setUp()
        self.event_manager = mock.MagicMock()
        self.ssh = mock.MagicMock()
        self.conn_info = {
            "ip": "127.0.0.1",
            "username": "random_username",
            "password": "random_password",
            "pkey": "random_pkey"
        }
        self.status = "Online"
        self.service_script_with_id_fmt = "SELECT DISK %s\r\nONLINE DISK"
        self.tools = windows.WindowsMountTools(
            self.conn_info, self.event_manager, mock.sentinel.ignore_devices,
            mock.sentinel.operation_timeout)
        self.tools._conn = mock_wsman_connection

    @mock.patch.object(windows.wsman.WSManConnection, 'from_connection_info')
    def test__connect(self, mock_from_connection_info):
        result = self.tools._connect()
        self.assertIsNone(result)

        mock_from_connection_info.assert_called_once_with(
            self.conn_info, mock.sentinel.operation_timeout)

    def test_get_connection(self):
        result = self.tools.get_connection()
        self.assertEqual(result, self.tools._conn)

    def test_check_os(self):
        with self.assertLogs('coriolis.osmorphing.osmount.windows',
                             level=logging.DEBUG):
            result = self.tools.check_os()
            self.assertTrue(result)

        self.tools._conn.exec_ps_command.assert_called_once_with(
            "(get-ciminstance Win32_OperatingSystem).Caption")

    def test_check_os_not_authorized(self):
        self.tools._conn.exec_ps_command.side_effect = (
            windows.exception.NotAuthorized)

        self.assertRaises(
            windows.exception.NotAuthorized, self.tools.check_os)

    def test_check_os_with_exception(self):
        self.tools._conn.exec_ps_command.side_effect = (
            windows.exception.CoriolisException)

        with self.assertLogs('coriolis.osmorphing.osmount.windows',
                             level=logging.DEBUG):
            result = self.tools.check_os()
            self.assertFalse(result)

    @mock.patch.object(windows.uuid, 'uuid4')
    def test__run_diskpart_script(self, mock_uuid4):
        script = "random_script"
        self.tools._conn.exec_ps_command.return_value = "random_tempdir"

        mocked_file_path = r"%s\%s.txt" % (
            self.tools._conn.exec_ps_command.return_value,
            mock_uuid4.return_value)

        result = self.tools._run_diskpart_script(script)
        self.assertEqual(result, 'random_tempdir')

        self.tools._conn.write_file.assert_called_once_with(
            mocked_file_path, b"random_script")

        self.tools._conn.exec_ps_command.assert_has_calls([
            mock.call("$env:TEMP"),
            mock.call("diskpart.exe /s '%s'" % mocked_file_path)
        ])

    @mock.patch.object(windows.WindowsMountTools, '_run_diskpart_script')
    def test__service_disks_with_status(self, mock_run_script):
        disk_ids_to_skip = ["1", "2"]
        mock_run_script.side_effect = [
            "  Disk 0 Online\n  Disk 1 Online\n  Disk 2 Online\n",
            "  Disk 0 Online\n  Disk 1 Online\n  Disk 2 Online\n",
            Exception()
        ]

        with self.assertLogs('coriolis.osmorphing.osmount.windows',
                             level=logging.WARN):
            self.tools._service_disks_with_status(
                self.status, self.service_script_with_id_fmt,
                skip_on_error=True,
                logmsg_fmt="Operating on disk with index '%s'",
                disk_ids_to_skip=disk_ids_to_skip)

    def test__service_disks_with_status_no_skip_ids(self):
        result = self.tools._service_disks_with_status(
            self.status, mock.sentinel.service_script_with_id_fmt)
        self.assertIsNone(result)

    @mock.patch.object(windows.WindowsMountTools, '_run_diskpart_script')
    def test__service_disks_with_status_disk_skipped(self, mock_run_script):
        disk_ids_to_skip = ["0", "1", "2"]
        mock_run_script.side_effect = [
            "  Disk 0 Online\n  Disk 1 Online\n  Disk 2 Online\n",
            "  Disk 0 Online\n  Disk 1 Online\n  Disk 2 Online\n",
            CoriolisTestException()
        ]

        with self.assertLogs('coriolis.osmorphing.osmount.windows',
                             level=logging.WARN):
            self.tools._service_disks_with_status(
                self.status, self.service_script_with_id_fmt,
                skip_on_error=False,
                logmsg_fmt="Operating on disk with index '%s'",
                disk_ids_to_skip=disk_ids_to_skip)

    @mock.patch.object(windows.WindowsMountTools, '_service_disks_with_status')
    def test__set_foreign_disks_rw_mode(self, mock_service_disks_with_status):
        result = self.tools._set_foreign_disks_rw_mode()
        self.assertIsNone(result)

        mock_service_disks_with_status.assert_called_once_with(
            "Foreign",
            "SELECT DISK %s\r\nATTRIBUTES DISK CLEAR READONLY\r\nEXIT",
            logmsg_fmt="Clearing R/O flag on foreign disk with ID '%s'."
        )

    @mock.patch.object(windows.WindowsMountTools, '_service_disks_with_status')
    def test__import_foreign_disks(self, mock_service_disks_with_status):
        result = self.tools._import_foreign_disks()
        self.assertIsNone(result)

        mock_service_disks_with_status.assert_called_once_with(
            "Foreign",
            "SELECT DISK %s\r\nIMPORT\r\nEXIT",
            logmsg_fmt="Importing foreign disk with ID '%s'."
        )

    def test__bring_disks_online(self):
        self.tools._conn.exec_ps_command.return_value = "1\n2"
        result = self.tools._bring_disks_online()
        self.assertIsNone(result)

        self.tools._conn.exec_ps_command.assert_has_calls([
            mock.call(
                "(Get-Disk | Where-Object { $_.IsOffline -eq $True }).Number"),
            mock.call("Set-Disk -IsOffline $False 1"),
            mock.call("Set-Disk -IsOffline $False 2"),
        ])

    def test__bring_disks_online_with_exception(self):
        self.tools._conn.exec_ps_command.side_effect = [
            "1\n2", None, windows.exception.CoriolisException]
        with self.assertLogs('coriolis.osmorphing.osmount.windows',
                             level=logging.WARNING):
            self.tools._bring_disks_online()

    def test__bring_disks_online_disk_nums(self):
        result = self.tools._bring_disks_online(disk_nums=['1', 2])
        self.assertIsNone(result)

        self.tools._conn.exec_ps_command.assert_has_calls([
            mock.call("Set-Disk -IsOffline $False 1"),
            mock.call("Set-Disk -IsOffline $False 2"),
        ])

    def test__set_basic_disks_rw_mode(self):
        self.tools._conn.exec_ps_command.return_value = "1\n2"
        result = self.tools._set_basic_disks_rw_mode()
        self.assertIsNone(result)

        self.tools._conn.exec_ps_command.assert_has_calls([
            mock.call("(Get-Disk | "
                      "Where-Object { $_.IsReadOnly -eq $True }).Number"),
            mock.call("Set-Disk -IsReadOnly $False 1"),
            mock.call("Set-Disk -IsReadOnly $False 2"),
        ])

    def test__set_basic_disks_rw_mode_with_exception(self):
        self.tools._conn.exec_ps_command.side_effect = [
            "1\n2", None, windows.exception.CoriolisException]
        with self.assertLogs('coriolis.osmorphing.osmount.windows',
                             level=logging.WARNING):
            self.tools._set_basic_disks_rw_mode()

    def test__get_system_drive(self):
        result = self.tools._get_system_drive()
        self.assertEqual(result, self.tools._conn.exec_ps_command.return_value)

        self.tools._conn.exec_ps_command.assert_called_once_with(
            "$env:SystemDrive")

    def test__get_fs_roots(self):
        self.tools._conn.exec_ps_command.return_value = "C:\\\nD:\\\nE:\\"
        self.tools._conn.EOL = "\n"

        result = self.tools._get_fs_roots()
        self.assertEqual(result, ["C:\\", "D:\\", "E:\\"])

        self.tools._conn.exec_ps_command.assert_called_once_with(
            "(get-psdrive -PSProvider FileSystem).Root")

    def test__get_fs_roots_with_exception(self):
        self.assertRaises(
            windows.exception.CoriolisException, self.tools._get_fs_roots,
            fail_if_empty=True)

    def test__bring_nonboot_disks_offline(self):
        self.tools._conn.exec_ps_command.return_value = "1\n2\n3"

        result = self.tools._bring_nonboot_disks_offline()
        self.assertIsNone(result)

        self.tools._conn.exec_ps_command.assert_has_calls([
            mock.call(
                "(Get-Disk | Where-Object { $_.IsBoot -eq $False }).Number"),
            mock.call("Set-Disk -IsOffline $True 1"),
            mock.call("Set-Disk -IsOffline $True 2"),
            mock.call("Set-Disk -IsOffline $True 3")
        ])

    def test__bring_nonboot_disks_offline_with_exception(self):
        self.tools._conn.exec_ps_command.side_effect = [
            "1\n2\n3",
            None,
            windows.exception.CoriolisException,
            None,
        ]

        with self.assertLogs('coriolis.osmorphing.osmount.windows',
                             level=logging.WARNING):
            self.tools._bring_nonboot_disks_offline()

    def test__bring_nonboot_disks_offline_disk_nums(self):
        result = self.tools._bring_nonboot_disks_offline(disk_nums=['1', 2])
        self.assertIsNone(result)

        self.tools._conn.exec_ps_command.assert_has_calls([
            mock.call('Set-Disk -IsOffline $True 1'),
            mock.call('Set-Disk -IsOffline $True 2'),
        ])

    @mock.patch.object(windows.WindowsMountTools,
                       '_bring_nonboot_disks_offline')
    @mock.patch.object(windows.WindowsMountTools, '_bring_disks_online')
    def test__rebring_disks_online(self, bring_disks_online_mock,
                                   bring_disks_offline_mock):
        result = self.tools._rebring_disks_online()
        self.assertIsNone(result)
        bring_disks_offline_mock.assert_called()
        bring_disks_online_mock.assert_called()

    @mock.patch.object(windows.WindowsMountTools, '_rebring_disks_online')
    def test__set_volumes_drive_letter(self, rebring_disks_mock):
        self.tools._conn.exec_ps_command.return_value = GET_PARTITION_OUTPUT
        result = self.tools._set_volumes_drive_letter()
        self.assertIsNone(result)

        self.tools._conn.exec_ps_command.assert_has_calls([
            mock.call(
                'Get-Partition | Where-Object { $_.Type -eq "Basic" -and '
                '$_.NoDefaultDriveLetter -eq $True } | Select-Object -Property'
                ' DiskNumber,PartitionNumber'),
            mock.call(
                'Set-Partition -NoDefaultDriveLetter $False -DiskNumber 2 '
                '-PartitionNumber 2'),
            mock.call(
                'Set-Partition -NoDefaultDriveLetter $False -DiskNumber 3 '
                '-PartitionNumber 4'),
        ])
        rebring_disks_mock.assert_called_once_with(disk_nums=['2', '3'])

    @mock.patch.object(windows.WindowsMountTools, '_get_system_drive')
    @mock.patch.object(windows.WindowsMountTools, '_get_fs_roots')
    @mock.patch.object(windows.WindowsMountTools, '_set_volumes_drive_letter')
    @mock.patch.object(windows.WindowsMountTools, '_set_basic_disks_rw_mode')
    @mock.patch.object(windows.WindowsMountTools, '_bring_disks_online')
    def test_mount_os(self, mock_bring_disks_online, mock_set_rw_mode,
                      mock_set_drive_letters, mock_get_fs_roots,
                      mock_get_system_drive):
        mock_get_fs_roots.return_value = ["C:\\", "D:\\", "E:\\"]
        mock_get_system_drive.return_value = "C:"
        self.tools._conn.test_path.side_effect = [True, False]

        result = self.tools.mount_os()
        self.assertEqual(('D:\\', None), result)

        mock_bring_disks_online.assert_called_once_with()
        mock_set_rw_mode.assert_called_once_with()
        mock_set_drive_letters.assert_called_once_with()

    def test_mount_os_no_root_partition(self):
        self.tools._conn.test_path.return_value = False
        self.tools._conn.exec_ps_command.return_value = "C:\\\nD:\\\nE:\\"
        self.tools._conn.EOL = "\n"

        self.assertRaises(windows.exception.OperatingSystemNotFound,
                          self.tools.mount_os)

    def test_dismount_os(self):
        root_drive = "C:\\"

        result = self.tools.dismount_os(root_drive)
        self.assertIsNone(result)

        self.tools._conn.exec_ps_command.assert_called_once_with(
            '(Get-Disk | Where-Object { $_.IsBoot -eq $False }).Number')
