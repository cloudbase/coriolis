# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import copy
import os
import re
import uuid

from distutils import version
from oslo_log import log as logging

from coriolis import constants
from coriolis import exception
from coriolis.osmorphing.osdetect import base
from coriolis import utils


WINDOWS_SERVER_IDENTIFIER = "Server"
WINDOWS_CLIENT_IDENTIFIER = "Client"


LOG = logging.getLogger(__name__)

REQUIRED_DETECTED_WINDOWS_OS_FIELDS = [
    "version_number", "edition_id", "installation_type", "product_name"]


class WindowsOSDetectTools(base.BaseOSDetectTools):

    @classmethod
    def returned_detected_os_info_fields(cls):
        base_fields = copy.deepcopy(
            base.REQUIRED_DETECTED_OS_FIELDS)
        base_fields.extend(
            REQUIRED_DETECTED_WINDOWS_OS_FIELDS)
        return base_fields

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
            "HKLM\\%s" % key_name,
            "%sWindows\\System32\\config\\SOFTWARE" % self._os_root_dir)
        try:
            version_info_str = self._conn.exec_ps_command(
                "Get-ItemProperty "
                "'HKLM:\\%s\\Microsoft\\Windows NT\\CurrentVersion' "
                "| select CurrentVersion, CurrentMajorVersionNumber, "
                "CurrentMinorVersionNumber,  CurrentBuildNumber, "
                "InstallationType, ProductName, EditionID | FL" %
                key_name).replace(self._conn.EOL, os.linesep)
        finally:
            self._unload_registry_hive("HKLM\\%s" % key_name)

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

    def detect_os(self):
        version_number = None
        edition_id = None
        installation_type = None
        product_name = None
        try:
            (version_number,
             edition_id,
             installation_type,
             product_name) = self._get_image_version_info()
        except exception.CoriolisException:
            LOG.debug(
                "Exception during Windows OS detection: %s",
                utils.get_exception_details())
            raise

        LOG.debug(
            "Successfully identified Windows release as: Version no.: %s; "
            "Edition id: %s; Install type: %s; Name: %s",
            version_number, edition_id,
            installation_type, product_name)

        typ = WINDOWS_CLIENT_IDENTIFIER
        if 'server' in edition_id.lower():
            typ = WINDOWS_SERVER_IDENTIFIER

        return {
            "version_number": version_number,
            "edition_id": edition_id,
            "installation_type": installation_type,
            "product_name": product_name,
            "os_type": constants.OS_TYPE_WINDOWS,
            "distribution_name": typ,
            "release_version": product_name,
            "friendly_release_name": "Windows %s" % product_name}
