# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import base64
import json
import logging
from unittest import mock

from coriolis import exception
from coriolis.osmorphing import windows
from coriolis.tests import test_base

WIN_VERSION_PS_OUTPUT = """
CurrentVersion            : 6.3
CurrentMajorVersionNumber : 10
CurrentMinorVersionNumber : 0
CurrentBuildNumber        : 20348
InstallationType          : Server
ProductName               : Windows Server 2022 Datacenter Evaluation
EditionID                 : ServerDatacenterEval
"""


class CoriolisTestException(Exception):
    pass


class BaseWindowsMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseWindowsMorphingTools class."""

    def setUp(self):
        super(BaseWindowsMorphingToolsTestCase, self).setUp()
        self.detected_os_info = {
            'os_type': 'windows',
            'distribution_name': 'Windows',
            'release_version': '10',
            'friendly_release_name': mock.sentinel.friendly_release_name,
            'version_number': mock.sentinel.version_number,
            'edition_id': mock.sentinel.edition_id,
            'installation_type': mock.sentinel.installation_type,
            'product_name': mock.sentinel.product_name,
        }
        self.conn = mock.MagicMock()
        self.event_manager = mock.MagicMock()
        self.os_root_dir = 'C:\\'
        self.morphing_tools = windows.BaseWindowsMorphingTools(
            self.conn, self.os_root_dir,
            mock.sentinel.os_root_dev, mock.sentinel.hypervisor,
            self.event_manager, self.detected_os_info,
            mock.sentinel.osmorphing_parameters,
            mock.sentinel.operation_timeout)

    def test_get_required_detected_os_info_fields(self):
        result = windows.BaseWindowsMorphingTools.\
            get_required_detected_os_info_fields()

        expected_fields = (
            windows.base.REQUIRED_DETECTED_OS_FIELDS +
            windows.REQUIRED_DETECTED_WINDOWS_OS_FIELDS
        )

        self.assertEqual(result, expected_fields)

    def test_check_os_supported(self):
        result = windows.BaseWindowsMorphingTools.check_os_supported(
            self.detected_os_info)

        self.assertTrue(result)

    def test_check_os_supported_not_supported(self):
        self.detected_os_info['os_type'] = 'unsupported'

        result = windows.BaseWindowsMorphingTools.check_os_supported(
            self.detected_os_info)

        self.assertFalse(result)

    def test__get_worker_os_drive_path(self):
        result = self.morphing_tools._get_worker_os_drive_path()

        self.conn.exec_ps_command.assert_called_once_with(
            "(Get-WmiObject Win32_OperatingSystem).SystemDrive")

        self.assertEqual(result, self.conn.exec_ps_command.return_value)

    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_get_worker_os_drive_path'
    )
    def test__get_dism_path(self, mock_get_worker_os_drive_path):
        mock_get_worker_os_drive_path.return_value = 'C:'

        result = self.morphing_tools._get_dism_path()

        mock_get_worker_os_drive_path.assert_called_once_with()

        self.assertEqual(
            result,
            "C:\\Windows\\System32\\dism.exe"
        )

    def test__get_sid(self):
        result = self.morphing_tools._get_sid()

        self.conn.exec_ps_command.assert_called_once_with(
            "(New-Object System.Security.Principal.NTAccount($ENV:USERNAME))."
            "Translate([System.Security.Principal.SecurityIdentifier]).Value"
        )

        self.assertEqual(result, self.conn.exec_ps_command.return_value)

    def test__grant_permissions(self):
        self.morphing_tools._grant_permissions(
            mock.sentinel.path, mock.sentinel.user, perm="(OI)(CI)F")

        self.conn.exec_command.assert_called_once_with(
            'icacls.exe',
            [mock.sentinel.path, '/grant', 'sentinel.user:(OI)(CI)F'])

    def test__revoke_permissions(self):
        self.morphing_tools._revoke_permissions(
            mock.sentinel.path, mock.sentinel.user)

        self.conn.exec_command.assert_called_once_with(
            'icacls.exe',
            [mock.sentinel.path, '/remove', mock.sentinel.user])

    def test__load_registry_hive(self):
        self.morphing_tools._load_registry_hive(
            mock.sentinel.subkey, mock.sentinel.path)

        self.conn.exec_command.assert_called_once_with(
            'reg.exe',
            ['load', mock.sentinel.subkey, mock.sentinel.path])

    def test__unload_registry_hive(self):
        self.morphing_tools._unload_registry_hive(mock.sentinel.subkey)

        self.conn.exec_command.assert_called_once_with(
            'reg.exe',
            ['unload', mock.sentinel.subkey])

    def test__get_ps_fl_value(self):
        result = self.morphing_tools._get_ps_fl_value(
            WIN_VERSION_PS_OUTPUT, 'InstallationType')

        self.assertEqual(result, 'Server')

    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_get_worker_os_drive_path'
    )
    def test__add_dism_driver(self, mock_get_worker_os_drive_path):
        mock_get_worker_os_drive_path.return_value = "C:"
        result = self.morphing_tools._add_dism_driver(
            mock.sentinel.driver_path)

        self.assertEqual(result, self.conn.exec_command.return_value)
        self.conn.exec_command.assert_called_once_with(
            "C:\\Windows\\System32\\dism.exe",
            ['/add-driver', '/image:%s' % self.os_root_dir,
             '/driver:"%s"' % mock.sentinel.driver_path,
             '/recurse', '/forceunsigned'])

        mock_get_worker_os_drive_path.assert_called_once_with()

    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_get_worker_os_drive_path'
    )
    def test__add_dism_driver_exception(self, mock_get_worker_os_drive_path):
        mock_get_worker_os_drive_path.return_value = "C:"
        self.conn.test_path.return_value = True
        self.conn.exec_command.side_effect = CoriolisTestException

        with self.assertLogs('coriolis.osmorphing.windows',
                             level=logging.ERROR):
            self.assertRaises(CoriolisTestException,
                              self.morphing_tools._add_dism_driver,
                              mock.sentinel.driver_path)
        mock_get_worker_os_drive_path.assert_called()
        self.conn.test_path.assert_called_once_with(
            "C:\\Windows\\Logs\\DISM\\dism.log")

    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_get_worker_os_drive_path'
    )
    def test__add_dism_driver_not_found(self, mock_get_worker_os_drive_path):
        self.conn.test_path.return_value = False
        self.conn.exec_command.side_effect = CoriolisTestException

        with self.assertLogs('coriolis.osmorphing.windows',
                             level=logging.WARN):
            self.assertRaises(CoriolisTestException,
                              self.morphing_tools._add_dism_driver,
                              mock.sentinel.driver_path)
        mock_get_worker_os_drive_path.assert_called()

    def test__mount_disk_image(self):
        result = self.morphing_tools._mount_disk_image(
            mock.sentinel.path)

        self.conn.exec_ps_command.assert_called_once_with(
            "(Mount-DiskImage '%s' -PassThru | Get-Volume).DriveLetter" %
            mock.sentinel.path)

        self.assertEqual(result, self.conn.exec_ps_command.return_value)

    def test__dismount_disk_image(self):
        self.morphing_tools._dismount_disk_image(mock.sentinel.path)

        self.conn.exec_ps_command.assert_called_once_with(
            "Dismount-DiskImage '%s'" % mock.sentinel.path, ignore_stdout=True)

    def test__expand_archive(self):
        self.conn.test_path.return_value = True
        destination = 'C:\\Windows\\destination'

        with self.assertLogs('coriolis.osmorphing.windows',
                             level=logging.WARN):
            self.morphing_tools._expand_archive(
                mock.sentinel.archive_path, destination)

        self.conn.exec_ps_command.assert_called_once()

    def test__expand_archive_remove_destination(self):
        self.conn.test_path.return_value = True

        destination = 'C:\\test\\destination'

        self.morphing_tools._expand_archive(
            mock.sentinel.archive_path, destination)

        self.conn.exec_ps_command.assert_has_calls([
            mock.call("rm -recurse -force %s" % destination),
            mock.call(
                "if(([System.Management.Automation.PSTypeName]"
                "'System.IO.Compression.ZipFile').Type -or "
                "[System.Reflection.Assembly]::LoadWithPartialName("
                "'System.IO.Compression.FileSystem')) {"
                "[System.IO.Compression.ZipFile]::ExtractToDirectory("
                "'%(path)s', '%(destination)s')} else {mkdir -Force "
                "'%(destination)s'; $shell = New-Object -ComObject "
                "Shell.Application;$shell.Namespace("
                "'%(destination)s').copyhere(($shell.NameSpace("
                "'%(path)s')).items())}" %
                {"path": mock.sentinel.archive_path,
                 "destination": destination},
                ignore_stdout=True)
        ])

    def test__set_service_start_mode(self):
        self.morphing_tools._set_service_start_mode(
            mock.sentinel.key_name, mock.sentinel.service_name,
            mock.sentinel.start_mode)

        registry_path = windows.SERVICE_PATH_FORMAT % (
            mock.sentinel.key_name, mock.sentinel.service_name)
        expected_command = (
            "Set-ItemProperty -Path '%s' -Name 'Start' -Value %s" %
            (registry_path, mock.sentinel.start_mode))

        self.conn.exec_ps_command.assert_called_once_with(expected_command)

    def test__create_service(self):
        service_account = 'LocalSystem'
        depends_on = [mock.sentinel.depends_on]

        self.morphing_tools._create_service(
            mock.sentinel.key_name, mock.sentinel.service_name,
            mock.sentinel.image_path, mock.sentinel.display_name,
            mock.sentinel.description, windows.SERVICE_START_AUTO,
            service_account, depends_on)

        registry_path = windows.SERVICE_PATH_FORMAT % (
            mock.sentinel.key_name, mock.sentinel.service_name)
        depends_on_ps = "@('%s')" % mock.sentinel.depends_on

        expected_commands = (
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
            {"path": registry_path, "image_path": mock.sentinel.image_path,
             "display_name": mock.sentinel.display_name,
             "description": mock.sentinel.description,
             "depends_on": depends_on_ps, "service_account": service_account,
             "start_mode": windows.SERVICE_START_AUTO}
        )

        self.conn.exec_ps_command.assert_called_once_with(
            expected_commands, ignore_stdout=True)

    @mock.patch.object(windows.utils, 'write_winrm_file')
    def test_run_user_script(self, mock_write_winrm_file):
        user_script = 'echo "Hello, World!"'
        script_path = "$env:TMP\\coriolis_user_script.ps1"

        self.morphing_tools.run_user_script(user_script)

        mock_write_winrm_file.assert_called_once_with(
            self.conn, script_path, user_script)
        self.conn.exec_ps_command.assert_called_once_with(
            ('$ErrorActionPreference = "Stop"; powershell.exe '
             '-NonInteractive -ExecutionPolicy RemoteSigned '
             '-File "%(script)s" "%(os_root_dir)s"') % {
                 "script": script_path,
                 "os_root_dir": self.os_root_dir,
            }
        )

    @mock.patch.object(windows.utils, 'write_winrm_file')
    def test_run_user_script_empty_script(self, mock_write_winrm_file):
        result = self.morphing_tools.run_user_script("")

        self.assertIsNone(result)
        mock_write_winrm_file.assert_not_called()
        self.conn.exec_ps_command.assert_not_called()

    @mock.patch.object(windows.utils, 'write_winrm_file')
    def test_run_user_script_raises_exception_on_write_winrm_file(
            self, mock_write_winrm_file):
        user_script = 'echo "Hello, World!"'
        mock_write_winrm_file.side_effect = exception.CoriolisException

        self.assertRaises(
            exception.CoriolisException,
            self.morphing_tools.run_user_script, user_script)

        self.conn.exec_ps_command.assert_not_called()

    @mock.patch.object(windows.utils, 'write_winrm_file')
    def test_run_user_script_raises_exception_on_exec_ps_command(
            self, mock_write_winrm_file):
        user_script = 'echo "Hello, World!"'
        script_path = "$env:TMP\\coriolis_user_script.ps1"

        self.conn.exec_ps_command.side_effect = exception.CoriolisException

        self.assertRaises(
            exception.CoriolisException,
            self.morphing_tools.run_user_script, user_script)

        mock_write_winrm_file.assert_called_once_with(
            self.conn, script_path, user_script)

    @mock.patch.object(windows.BaseWindowsMorphingTools, '_load_registry_hive')
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_check_cloudbase_init_exists'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_set_service_start_mode'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_unload_registry_hive'
    )
    @mock.patch.object(windows.uuid, 'uuid4')
    def test__disable_cloudbase_init(
            self, mock_uuid4, mock_unload_registry_hive,
            mock_set_service_start_mode, mock_check_cloudbase_init_exists,
            mock_load_registry_hive):
        self.morphing_tools._disable_cloudbase_init()

        mock_check_cloudbase_init_exists.return_value = True

        mock_load_registry_hive.assert_called_once_with(
            "HKLM\\%s" % mock_uuid4.return_value,
            "%sWindows\\System32\\config\\SYSTEM" % self.os_root_dir)

        mock_set_service_start_mode.assert_called_once_with(
            str(mock_uuid4.return_value), windows.CLOUDBASEINIT_SERVICE_NAME,
            windows.SERVICE_START_DISABLED)
        mock_unload_registry_hive.assert_called_once_with(
            "HKLM\\%s" % mock_uuid4.return_value)

    def test__check_cloudbase_init_exists(self):
        self.conn.exec_ps_command.return_value = "True"
        result = self.morphing_tools._check_cloudbase_init_exists(
            mock.sentinel.key_name)

        self.conn.exec_ps_command.assert_called_once_with(
            "Test-Path %s" % (windows.SERVICE_PATH_FORMAT % (
                mock.sentinel.key_name, windows.CLOUDBASEINIT_SERVICE_NAME)))

        self.assertTrue(result)

    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_set_service_start_mode'
    )
    def test__setup_existing_cbslinit_service(
            self, mock_set_service_start_mode):
        self.morphing_tools._setup_existing_cbslinit_service(
            mock.sentinel.key_name, mock.sentinel.image_path)

        reg_service_path = windows.SERVICE_PATH_FORMAT % (
            mock.sentinel.key_name, windows.CLOUDBASEINIT_SERVICE_NAME)

        self.conn.exec_ps_command.assert_has_calls([
            mock.call(
                "Set-ItemProperty -Path '%s' -Name 'ImagePath' -Value '%s' "
                "-Force" % (reg_service_path, mock.sentinel.image_path),
            ),
            mock.call(
                "Set-ItemProperty -Path '%s' -Name 'ObjectName' "
                "-Value 'LocalSystem' -Force" % reg_service_path
            ),
        ])

        mock_set_service_start_mode.assert_called_once_with(
            mock.sentinel.key_name, windows.CLOUDBASEINIT_SERVICE_NAME,
            windows.SERVICE_START_AUTO)

    def test__get_cbslinit_base_dir(self):
        result = self.morphing_tools._get_cbslinit_base_dir()

        self.assertEqual(result, "%sCloudbase-Init" % self.os_root_dir)

    def test__get_cbslinit_scripts_dir(self):
        result = self.morphing_tools._get_cbslinit_scripts_dir('C:')

        self.assertEqual(result, "C:\\LocalScripts")

    @mock.patch.object(windows.utils, 'write_winrm_file')
    def test__write_local_script(self, mock_write_winrm_file):
        script_path = "/script/path/script.sh"

        with mock.patch('builtins.open', mock.mock_open()) as mock_open:
            self.morphing_tools._write_local_script(
                'C:', script_path, priority=50)
            mock_open.assert_called_once_with(script_path, 'r')
            mock_write_winrm_file.assert_called_once_with(
                self.conn,
                'C:\\LocalScripts\\50-script.sh',
                '')

    @mock.patch.object(windows.utils, 'write_winrm_file')
    @mock.patch.object(windows.BaseWindowsMorphingTools, '_write_local_script')
    def test__write_cloudbase_init_conf(
            self, mock_write_local_script, mock_write_winrm_file):
        local_base_dir = "C:\\LocalBaseDir"
        mocked_full_path = windows.os.path.join(
            windows.utils.get_resources_bin_dir(), 'bring-disks-online.ps1')

        self.morphing_tools._write_cloudbase_init_conf(
            'C:\\Cloudbase-Init', local_base_dir, com_port='COM1')

        self.conn.exec_ps_command.assert_has_calls([
            mock.call(
                "mkdir 'C:\\Cloudbase-Init\\conf' -Force",
                ignore_stdout=True),
            mock.call(
                "mkdir 'C:\\Cloudbase-Init\\LocalScripts' -Force",
                ignore_stdout=True),
        ])

        conf_file_path = (
            "C:\\Cloudbase-Init\\conf\\cloudbase-init.conf")
        conf_content = (
            "[DEFAULT]\r\n"
            "username = Admin\r\n"
            "groups = Administrators\r\n"
            "verbose = true\r\n"
            "bsdtar_path = %(bin_path)s\\bsdtar.exe\r\n"
            "mtools_path = %(bin_path)s\r\n"
            "logdir = %(log_path)s\r\n"
            "local_scripts_path = %(scripts_path)s\r\n"
            "stop_service_on_exit = false\r\n"
            "logfile = cloudbase-init.log\r\n"
            "default_log_levels = \r\n"
            "comtypes=INFO,suds=INFO,iso8601=WARN,requests=WARN\r\n"
            "allow_reboot = false\r\n"
            "plugins = %(plugins)s\r\n"
            "debug = true\r\n"
            "san_policy = OnlineAll\r\n"
            "metadata_services = %(metadata_services)s\r\n"
            "logging_serial_port_settings = %(com_port)s,9600,N,8\r\n" %
            {"bin_path": "%s\\Bin" % local_base_dir,
             "log_path": "%s\\Log" % local_base_dir,
             "scripts_path": "%s\\LocalScripts" % local_base_dir,
             "com_port": 'COM1',
             "metadata_services": ",".join(
                 windows.CLOUDBASE_INIT_DEFAULT_METADATA_SVCS),
             "plugins": ",".join(windows.CLOUDBASE_INIT_DEFAULT_PLUGINS)})

        mock_write_winrm_file.assert_called_once_with(
            self.conn, conf_file_path, conf_content)

        mock_write_local_script.assert_called_once_with(
            'C:\\Cloudbase-Init', mocked_full_path, priority=99)

    @mock.patch.object(windows.utils, 'write_winrm_file')
    @mock.patch.object(windows.BaseWindowsMorphingTools, '_write_local_script')
    def test__write_cloudbase_init_conf_with_exception(
            self, mock_write_local_script, mock_write_winrm_file):
        plugins = "invalid plugins"

        self.assertRaises(
            exception.CoriolisException,
            self.morphing_tools._write_cloudbase_init_conf,
            mock.sentinel.cloudbaseinit_base_dir, mock.sentinel.local_base_dir,
            com_port='COM1', plugins=plugins)

        self.conn.exec_ps_command.assert_not_called()
        mock_write_winrm_file.assert_not_called()
        mock_write_local_script.assert_not_called()

    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_unload_registry_hive'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_write_cloudbase_init_conf'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_check_cloudbase_init_exists'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_setup_existing_cbslinit_service'
    )
    @mock.patch.object(windows.BaseWindowsMorphingTools, '_create_service')
    @mock.patch.object(windows.BaseWindowsMorphingTools, '_expand_archive')
    @mock.patch.object(windows.BaseWindowsMorphingTools, '_load_registry_hive')
    @mock.patch.object(windows.uuid, 'uuid4')
    def test__install_cloudbase_init(
            self, mock_uuid4, mock_load_registry_hive,
            mock_expand_archive, mock_create_service,
            mock_setup_existing_cbslinit_service,
            mock_check_cloudbase_init_exists, mock_write_cloudbase_init_conf,
            mock_unload_registry_hive):
        cloudbaseinit_zip_path = 'c:\\cloudbaseinit.zip'
        cloudbaseinit_base_dir = "C:\\Cloudbase-Init"
        local_base_dir = "C%s" % cloudbaseinit_base_dir[1:]

        mock_check_cloudbase_init_exists.return_value = False

        result = self.morphing_tools._install_cloudbase_init(
            mock.sentinel.download_url, com_port='COM1')

        mock_load_registry_hive.assert_called_once_with(
            "HKLM\\%s" % mock_uuid4.return_value,
            "%sWindows\\System32\\config\\SYSTEM" % self.os_root_dir)

        mock_expand_archive.assert_called_once_with(
            cloudbaseinit_zip_path, cloudbaseinit_base_dir)

        self.conn.exec_ps_command.assert_called_once_with(
            "mkdir '%s' -Force" %
            ("%s\\Log" % cloudbaseinit_base_dir),
            ignore_stdout=True)

        mock_write_cloudbase_init_conf.assert_called_once_with(
            cloudbaseinit_base_dir, local_base_dir,
            metadata_services=None, plugins=None, com_port='COM1')
        mock_check_cloudbase_init_exists.assert_called_once_with(
            str(mock_uuid4.return_value))

        expected_image_path = (
            '"%(path)s\\Bin\\OpenStackService.exe" cloudbase-init '
            '"%(path)s\\Python\\Python.exe" '
            '"%(path)s\\Python\\Scripts\\cloudbase-init.exe" '
            '--config-file "%(path)s\\conf\\cloudbase-init.conf"' % {
                'path': local_base_dir})

        mock_setup_existing_cbslinit_service.assert_not_called()
        mock_create_service.assert_called_once_with(
            key_name=str(mock_uuid4.return_value),
            service_name=windows.CLOUDBASEINIT_SERVICE_NAME,
            image_path=expected_image_path,
            display_name='Cloud Initialization Service',
            description='Service wrapper for cloudbase-init',
        )

        mock_unload_registry_hive.assert_called_once_with(
            "HKLM\\%s" % mock_uuid4.return_value)

        self.assertEqual(result, cloudbaseinit_base_dir)

    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_unload_registry_hive'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_check_cloudbase_init_exists'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_setup_existing_cbslinit_service'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_write_cloudbase_init_conf'
    )
    @mock.patch.object(windows.BaseWindowsMorphingTools, '_create_service')
    @mock.patch.object(windows.BaseWindowsMorphingTools, '_expand_archive')
    @mock.patch.object(windows.BaseWindowsMorphingTools, '_load_registry_hive')
    @mock.patch.object(windows.uuid, 'uuid4')
    def test_install_cloudbase_init_existing_service(
            self, mock_uuid4, mock_load_registry_hive, mock_expand_archive,
            mock_create_service, mock_write_cloudbase_init_conf,
            mock_setup_existing_cbslinit_service,
            mock_check_cloudbase_init_exists, mock_unload_registry_hive):
        cloudbaseinit_zip_path = 'c:\\cloudbaseinit.zip'
        cloudbaseinit_base_dir = "C:\\Cloudbase-Init"
        local_base_dir = "C%s" % cloudbaseinit_base_dir[1:]
        mock_check_cloudbase_init_exists.return_value = True

        self.morphing_tools._install_cloudbase_init(mock.sentinel.download_url,
                                                    com_port='COM1')

        expected_image_path = (
            '"%(path)s\\Bin\\OpenStackService.exe" cloudbase-init '
            '"%(path)s\\Python\\Python.exe" '
            '"%(path)s\\Python\\Scripts\\cloudbase-init.exe" '
            '--config-file "%(path)s\\conf\\cloudbase-init.conf"' % {
                'path': local_base_dir})

        mock_load_registry_hive.assert_called_once_with(
            "HKLM\\%s" % mock_uuid4.return_value,
            "%sWindows\\System32\\config\\SYSTEM" % self.os_root_dir)
        mock_expand_archive.assert_called_once_with(
            cloudbaseinit_zip_path, cloudbaseinit_base_dir)
        self.conn.exec_ps_command.assert_called_once_with(
            "mkdir '%s' -Force" %
            ("%s\\Log" % cloudbaseinit_base_dir),
            ignore_stdout=True)

        mock_write_cloudbase_init_conf.assert_called_once_with(
            cloudbaseinit_base_dir, local_base_dir,
            metadata_services=None, plugins=None, com_port='COM1')
        mock_setup_existing_cbslinit_service.assert_called_once_with(
            str(mock_uuid4.return_value), expected_image_path)
        mock_create_service.assert_not_called()
        mock_unload_registry_hive.assert_called_once_with(
            "HKLM\\%s" % mock_uuid4.return_value)

    def test__compile_static_ip_conf_from_registry(self):
        self.conn.exec_ps_command.side_effect = [
            'interface1\ninterface2',
            '0',
            '192.168.1.1',
            '255.255.255.0',
            '192.168.1.254',
            '8.8.8.8',
            '1',
        ]
        interfaces_reg_path = (windows.INTERFACES_PATH_FORMAT %
                               mock.sentinel.key_name)

        result = self.morphing_tools._compile_static_ip_conf_from_registry(
            mock.sentinel.key_name)

        self.conn.exec_ps_command.assert_has_calls([
            mock.call(
                "(((Get-ChildItem -Path '%s').Name | Select-String -Pattern "
                "'[^\\\\]+$').Matches).Value" % interfaces_reg_path),
            mock.call(
                "(Get-ItemProperty -Path '%s\\interface1').EnableDHCP" %
                interfaces_reg_path),
            mock.call(
                "(Get-ItemProperty -Path '%s\\interface1').IPAddress" %
                interfaces_reg_path),
            mock.call(
                "(Get-ItemProperty -Path '%s\\interface1').SubnetMask" %
                interfaces_reg_path),
            mock.call(
                "(Get-ItemProperty -Path '%s\\interface1').DefaultGateway" %
                interfaces_reg_path),
            mock.call(
                "(Get-ItemProperty -Path '%s\\interface1').NameServer"
                % interfaces_reg_path),
            mock.call(
                "(Get-ItemProperty -Path '%s\\interface2').EnableDHCP"
                % interfaces_reg_path),
        ])

        expected_ips_info = [
            {"ip_address": '192.168.1.1',
             "prefix_length": 24,
             "default_gateway": '192.168.1.254',
             "dns_addresses": '8.8.8.8'}
        ]
        self.assertEqual(result, expected_ips_info)

    def test_compile_static_ip_conf_from_registry_no_ip_or_subnet(self):
        self.conn.exec_ps_command.side_effect = [
            'interface1',
            '0',
            None,
            None,
        ]

        interfaces_reg_path = (windows.INTERFACES_PATH_FORMAT %
                               mock.sentinel.key_name)
        with self.assertLogs('coriolis.osmorphing.windows',
                             level=logging.WARNING):
            self.morphing_tools._compile_static_ip_conf_from_registry(
                mock.sentinel.key_name)

        self.conn.exec_ps_command.assert_has_calls([
            mock.call(
                "(((Get-ChildItem -Path '%s').Name | Select-String -Pattern "
                "'[^\\\\]+$').Matches).Value" % interfaces_reg_path),
            mock.call("(Get-ItemProperty -Path '%s\\interface1').EnableDHCP" %
                      interfaces_reg_path),
            mock.call("(Get-ItemProperty -Path '%s\\interface1').IPAddress" % (
                interfaces_reg_path)),
            mock.call("(Get-ItemProperty -Path '%s\\interface1').SubnetMask" %
                      interfaces_reg_path),
        ])

    def test_compile_static_ip_conf_from_registry_no_static_ip(self):
        self.conn.exec_ps_command.side_effect = [
            'interface1',
            '1',
        ]
        interfaces_reg_path = (windows.INTERFACES_PATH_FORMAT %
                               mock.sentinel.key_name)

        with self.assertLogs('coriolis.osmorphing.windows',
                             level=logging.DEBUG):
            self.morphing_tools._compile_static_ip_conf_from_registry(
                mock.sentinel.key_name)
        self.conn.exec_ps_command.assert_has_calls([
            mock.call(
                "(((Get-ChildItem -Path '%s').Name | Select-String -Pattern "
                "'[^\\\\]+$').Matches).Value" % interfaces_reg_path),
            mock.call("(Get-ItemProperty -Path '%s\\interface1').EnableDHCP" %
                      interfaces_reg_path),
        ])

    def test__get_static_nics_info(self):
        static_ipv4 = "10.0.0.16"
        static_ipv6 = "fe80::728a:688:1a92:baec"
        dynamic_ipv4 = "10.0.1.16"
        dynamic_ipv6 = "fe81::728a:688:1a92:baec"
        # detected static IPs
        ips_info = [{"ip_address": static_ipv4},
                    {"ip_address": static_ipv6}]
        nics_info = [
            # no IP addresses on NIC
            {"ip_addresses": [], "mac_address": "00:50:56:92:91:42"},
            # dynamic ipv6 IP
            {"ip_addresses": [static_ipv4, dynamic_ipv6],
             "mac_address": "00:50:56:92:91:43"},
            # both IPs dynamic
            {"ip_addresses": [dynamic_ipv4, dynamic_ipv6],
             "mac_address": "00:50:56:92:91:44"},
            # dynamic ipv4 IP
            {"ip_addresses": [dynamic_ipv4, static_ipv6],
             "mac_address": "00:50:56:92:91:45"}]

        expected_result = [
            {"mac_address": "00:50:56:92:91:43",
             "ip_addresses": ["10.0.0.16"]},
            {"mac_address": "00:50:56:92:91:45",
             "ip_addresses": ["fe80::728a:688:1a92:baec"]}]

        self.assertEqual(
            self.morphing_tools._get_static_nics_info(nics_info, ips_info),
            expected_result)

    @mock.patch.object(windows.utils, 'write_winrm_file')
    def test__write_static_ip_script(self, mock_write_winrm_file):
        nics_info = [
            {'ip_addresses': ["10.1.10.10"]}
        ]
        ips_info = [
            {'ip_address': "10.1.10.10"}
        ]
        self.morphing_tools._write_static_ip_script(
            'C:', nics_info, ips_info)

        contents = windows.STATIC_IP_SCRIPT_TEMPLATE % {
            'nics_info': base64.b64encode(
                json.dumps(nics_info).encode()).decode(),
            'ips_info': base64.b64encode(
                json.dumps(ips_info).encode()).decode(),
        }
        mock_write_winrm_file.assert_called_once_with(
            self.morphing_tools._conn,
            "C:\\LocalScripts\\01-static-ip-config.ps1",
            contents.encode())

    @mock.patch.object(
        windows.BaseWindowsMorphingTools,
        '_compile_static_ip_conf_from_registry'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_write_static_ip_script'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_get_static_nics_info')
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_unload_registry_hive'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_load_registry_hive'
    )
    @mock.patch.object(windows.uuid, 'uuid4')
    def test_set_net_config(self, mock_uuid4, mock_load_registry_hive,
                            mock_unload_registry_hive,
                            mock_get_static_nics_info,
                            mock_write_static_ip_script,
                            mock_compile_static_ip_conf_from_registry):
        dhcp = False
        nics_info = [
            {'ip_addresses': ["10.1.10.10"]}
        ]
        ips_info = [
            {'ip_address': "10.1.10.10"}
        ]
        mock_compile_static_ip_conf_from_registry.return_value = ips_info

        self.morphing_tools.set_net_config(nics_info, dhcp=dhcp)

        mock_load_registry_hive.assert_called_once_with(
            "HKLM\\%s" % mock_uuid4.return_value,
            "%sWindows\\System32\\config\\SYSTEM" % self.os_root_dir)
        mock_compile_static_ip_conf_from_registry.assert_called_once_with(
            str(mock_uuid4.return_value))
        mock_get_static_nics_info.assert_called_once_with(nics_info, ips_info)
        mock_write_static_ip_script.assert_called_once_with(
            "C:\\Cloudbase-Init",
            mock_get_static_nics_info.return_value,
            mock_compile_static_ip_conf_from_registry.return_value)
        mock_unload_registry_hive.assert_called_once_with(
            "HKLM\\%s" % mock_uuid4.return_value)

    @mock.patch.object(
        windows.BaseWindowsMorphingTools,
        '_compile_static_ip_conf_from_registry'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_write_static_ip_script'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_get_static_nics_info')
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_unload_registry_hive'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_load_registry_hive'
    )
    @mock.patch.object(windows.uuid, 'uuid4')
    def test_set_net_config_no_static_info(
            self, mock_uuid4, mock_load_registry_hive,
            mock_unload_registry_hive, mock_get_static_nics_info,
            mock_write_static_ip_script,
            mock_compile_static_ip_conf_from_registry):
        dhcp = False
        nics_info = [
            {'ip_addresses': ["10.1.10.10"]}
        ]
        ips_info = [
            {'ip_address': "10.1.10.10"}
        ]
        mock_compile_static_ip_conf_from_registry.return_value = ips_info
        mock_get_static_nics_info.return_value = []

        self.morphing_tools.set_net_config(nics_info, dhcp=dhcp)

        mock_load_registry_hive.assert_called_once_with(
            "HKLM\\%s" % mock_uuid4.return_value,
            "%sWindows\\System32\\config\\SYSTEM" % self.os_root_dir)
        mock_compile_static_ip_conf_from_registry.assert_called_once_with(
            str(mock_uuid4.return_value))
        mock_get_static_nics_info.assert_called_once_with(nics_info, ips_info)
        mock_write_static_ip_script.assert_not_called()
        mock_unload_registry_hive.assert_called_once_with(
            "HKLM\\%s" % mock_uuid4.return_value)

    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_get_cbslinit_base_dir'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools,
        '_compile_static_ip_conf_from_registry'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_write_static_ip_script'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_unload_registry_hive'
    )
    @mock.patch.object(
        windows.BaseWindowsMorphingTools, '_load_registry_hive'
    )
    @mock.patch.object(windows.BaseWindowsMorphingTools,
                       '_get_static_nics_info')
    def test_set_net_config_with_dhcp(
            self, mock_check_ips_info, mock_load_registry_hive,
            mock_unload_registry_hive, mock_write_static_ip_script,
            mock_get_cbslinit_base_dir,
            mock_compile_static_ip_conf_from_registry):
        dhcp = True

        result = self.morphing_tools.set_net_config(
            mock.sentinel.nics_info, dhcp=dhcp)

        mock_load_registry_hive.assert_not_called()
        mock_get_cbslinit_base_dir.assert_not_called()
        mock_compile_static_ip_conf_from_registry.assert_not_called()
        mock_check_ips_info.assert_not_called()
        mock_write_static_ip_script.assert_not_called()
        mock_unload_registry_hive.assert_not_called()

        self.assertIsNone(result)
