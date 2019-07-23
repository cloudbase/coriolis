# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from distutils import version
import os
import re
import uuid

from oslo_log import log as logging

from coriolis import exception
from coriolis import utils
from coriolis.osmorphing import base

LOG = logging.getLogger(__name__)

SERVICE_START_AUTO = 2
SERVICE_START_MANUAL = 3
SERVICE_START_DISABLED = 4

SERVICE_PATH_FORMAT = "HKLM:\\%s\\ControlSet001\\Services\\%s"


class BaseWindowsMorphingTools(base.BaseOSMorphingTools):
    def _check_os(self):
        try:
            (self._version_number,
             self._edition_id,
             self._installation_type,
             self._product_name) = self._get_image_version_info()
            return ('Windows', self._product_name)
        except exception.CoriolisException as ex:
            LOG.debug("Exception during OS detection: %s", ex)

    def set_net_config(self, nics_info, dhcp):
        # TODO(alexpilotti): implement
        pass

    def _get_worker_os_drive_path(self):
        return self._conn.exec_ps_command(
            "(Get-WmiObject Win32_OperatingSystem).SystemDrive")

    def _get_dism_path(self):
        return "%s\\Windows\\System32\\dism.exe" % (
            self._get_worker_os_drive_path())

    def _get_sid(self):
        sid = self._conn.exec_ps_command(
            "(New-Object System.Security.Principal.NTAccount($ENV:USERNAME))."
            "Translate([System.Security.Principal.SecurityIdentifier]).Value")
        LOG.debug("Current user's SID: %s", sid)
        return sid

    def _grant_permissions(self, path, user, perm="(OI)(CI)F"):
        self._conn.exec_command(
            "icacls.exe", [path, "/grant", "%s:%s" % (user, perm)])

    def _revoke_permissions(self, path, user):
        self._conn.exec_command(
            "icacls.exe", [path, "/remove", user])

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
        try:
            return self._conn.exec_command(
                dism_path,
                ["/add-driver", "/image:%s" % self._os_root_dir,
                 "/driver:\"%s\"" % driver_path, "/recurse", "/forceunsigned"])
        except Exception as ex:
            dism_log_path = "%s\\Windows\\Logs\\DISM\\dism.log" % (
                self._get_worker_os_drive_path())
            if self._conn.test_path(dism_log_path):
                dism_log_contents = self._conn.exec_ps_command(
                    "Get-Content %s" % dism_log_path)
                LOG.error(
                    "Error occured whilst adding driver '%s' through DISM. "
                    "Contents of '%s': %s",
                    driver_path, dism_log_path, dism_log_contents)
            else:
                LOG.warn(
                    "Could not find DISM error logs for failure:'%s'", str(ex))
            raise

    def _mount_disk_image(self, path):
        LOG.info("Mounting disk image: %s" % path)
        return self._conn.exec_ps_command(
            "(Mount-DiskImage '%s' -PassThru | Get-Volume).DriveLetter" %
            path)

    def _dismount_disk_image(self, path):
        LOG.info("Unmounting disk image: %s" % path)
        self._conn.exec_ps_command("Dismount-DiskImage '%s'" % path,
                                   ignore_stdout=True)

    @utils.retry_on_error()
    def _expand_archive(self, path, destination, overwrite=True):
        LOG.info("Expanding archive \"%(path)s\" in \"%(destination)s\"",
                 {"path": path, "destination": destination})

        if self._conn.test_path(destination):
            LOG.info("Destination folder %s already exists" % destination)
            if overwrite:
                if destination.endswith(":\\") or ":\\Windows" in destination:
                    LOG.warn(
                        "Not removing target directory, as it is either the "
                        "root directory or is within the Windows directory")
                else:
                    self._conn.exec_ps_command(
                        "rm -recurse -force %s" % destination)
        self._conn.exec_ps_command(
            "if(([System.Management.Automation.PSTypeName]"
            "'System.IO.Compression.ZipFile').Type -or "
            "[System.Reflection.Assembly]::LoadWithPartialName("
            "'System.IO.Compression.FileSystem')) {"
            "[System.IO.Compression.ZipFile]::ExtractToDirectory('%(path)s', "
            "'%(destination)s')} else {mkdir -Force '%(destination)s'; "
            "$shell = New-Object -ComObject Shell.Application;"
            "$shell.Namespace('%(destination)s').copyhere(($shell.NameSpace("
            "'%(path)s')).items())}" %
            {"path": path, "destination": destination},
            ignore_stdout=True)

    def _set_service_start_mode(self, key_name, service_name, start_mode):
        LOG.info("Setting service start mode: %(service_name)s, "
                 "%(start_mode)s", {"service_name": service_name,
                                    "start_mode": start_mode})
        registry_path = SERVICE_PATH_FORMAT % (key_name, service_name)
        self._conn.exec_ps_command(
            "Set-ItemProperty -Path '%(path)s' -Name 'Start' -Value "
            "%(start_mode)s" %
            {"path": registry_path, "start_mode": start_mode})

    def _create_service(self, key_name, service_name, image_path,
                        display_name, description,
                        start_mode=SERVICE_START_AUTO,
                        service_account="LocalSystem",
                        depends_on=[]):
        LOG.info("Creating service: %s", service_name)
        registry_path = SERVICE_PATH_FORMAT % (key_name, service_name)
        depends_on_ps = "@(%s)" % (",".join(["'%s'" % v for v in depends_on]))

        self._conn.exec_ps_command(
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
            {"path": registry_path,
             "image_path": image_path,
             "display_name": display_name,
             "description": description,
             "depends_on": depends_on_ps,
             "service_account": service_account,
             "start_mode": start_mode},
            ignore_stdout=True)
