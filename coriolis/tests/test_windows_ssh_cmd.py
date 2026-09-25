# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import windows_ssh_cmd
from coriolis.tests import test_base


class WinrmToSshCmdTestCase(test_base.CoriolisBaseTestCase):
    def test_unwrap_winrm_driver_quotes(self):
        self.assertEqual(
            windows_ssh_cmd.unwrap_winrm_arg_quotes(
                '/driver:"G:\\Balloon\\2k19\\amd64"'
            ),
            "/driver:G:\\Balloon\\2k19\\amd64",
        )

    def test_winrm_exec_to_ssh_dism_quotes_match_plain_path(self):
        driver = "G:\\Balloon\\2k19\\amd64"
        quoted = windows_ssh_cmd.winrm_exec_to_ssh(
            "C:\\Windows\\System32\\dism.exe",
            [
                "/add-driver",
                "/image:F:\\",
                '/driver:"%s"' % driver,
                "/recurse",
                "/forceunsigned",
            ],
        )
        plain = windows_ssh_cmd.winrm_exec_to_ssh(
            "C:\\Windows\\System32\\dism.exe",
            [
                "/add-driver",
                "/image:F:\\",
                "/driver:%s" % driver,
                "/recurse",
                "/forceunsigned",
            ],
        )
        self.assertEqual(quoted, plain)
        self.assertNotIn('/driver:"', quoted)

    def test_winrm_exec_to_ssh_icacls_grant(self):
        grant = "*S-1-5-21-841993420-4016469602-3165386176-500:(OI)(CI)F"
        result = windows_ssh_cmd.winrm_exec_to_ssh(
            "icacls.exe",
            [
                "F:\\Windows\\System32\\DriverStore\\FileRepository",
                "/grant",
                grant,
            ],
        )
        self.assertEqual(
            result,
            'cmd.exe /c "icacls.exe '
            'F:\\Windows\\System32\\DriverStore\\FileRepository '
            '/grant ""%s"""' % grant,
        )

    def test_format_windows_command_quotes_spaces(self):
        result = windows_ssh_cmd.format_windows_command(
            "reg.exe", ["load", "HKLM\\x", "C:\\Program Files\\hive"]
        )
        self.assertEqual(result, 'reg.exe load HKLM\\x "C:\\Program Files\\hive"')

    def test_format_windows_command_quotes_icacls_grant(self):
        grant = "*S-1-5-21-841993420-4016469602-3165386176-500:(OI)(CI)F"
        result = windows_ssh_cmd.format_windows_command(
            "icacls.exe",
            [
                "F:\\Windows\\System32\\DriverStore\\FileRepository",
                "/grant",
                grant,
            ],
        )
        self.assertEqual(
            result,
            'icacls.exe F:\\Windows\\System32\\DriverStore\\FileRepository '
            '/grant "%s"' % grant,
        )

    def test_escape_trailing_backslash_for_ssh(self):
        odd = "dism.exe /get-drivers /image:F:\\"
        self.assertEqual(
            windows_ssh_cmd.escape_trailing_backslash_for_ssh(odd),
            "dism.exe /get-drivers /image:F:\\\\",
        )
        self.assertEqual(
            windows_ssh_cmd.escape_trailing_backslash_for_ssh("reg.exe unload HKLM\\x"),
            "reg.exe unload HKLM\\x",
        )
        even = "cmd /c dir C:\\\\"
        self.assertEqual(
            windows_ssh_cmd.escape_trailing_backslash_for_ssh(even),
            even,
        )

    def test_winrm_exec_to_ssh_dism_image_trailing_backslash(self):
        result = windows_ssh_cmd.winrm_exec_to_ssh(
            "dism.exe", ["/Get-Drivers", "/image:F:\\"]
        )
        self.assertEqual(
            result,
            'cmd.exe /c "dism.exe /Get-Drivers /image:F:\\\\"',
        )
