# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import copy

from coriolis import constants
from coriolis.osmorphing.osdetect import base


DETECTED_SUSE_RELEASE_FIELD_NAME = "suse_release_name"
SLES_DISTRO_IDENTIFIER = "SLES"
OPENSUSE_DISTRO_IDENTIFIER = "openSUSE"
OPENSUSE_TUMBLEWEED_VERSION_IDENTIFIER = "Tumbleweed"


class SUSEOSDetectTools(base.BaseLinuxOSDetectTools):

    @classmethod
    def returned_detected_os_info_fields(cls):
        common_fields = super(
            SUSEOSDetectTools, cls).returned_detected_os_info_fields()
        fields = copy.deepcopy(common_fields)
        fields.append(DETECTED_SUSE_RELEASE_FIELD_NAME)
        return fields

    def detect_os(self):
        # TODO(aznashwan): implement separate detection for
        # SLES and openSUSE when the tools for them are separated.
        info = {}
        os_release = self._get_os_release()
        name = os_release.get("NAME")
        if name and (name == "SLES" or name.startswith("openSUSE")):
            distro_name = None
            if name == "SLES":
                distro_name = SLES_DISTRO_IDENTIFIER
            elif name.lower().startswith("opensuse"):
                distro_name = OPENSUSE_DISTRO_IDENTIFIER
            version = os_release.get(
                "VERSION_ID", constants.OS_TYPE_UNKNOWN)
            if 'tumbleweed' in distro_name.lower():
                version = OPENSUSE_TUMBLEWEED_VERSION_IDENTIFIER

            if distro_name:
                info = {
                    "os_type": constants.OS_TYPE_LINUX,
                    "distribution_name": distro_name,
                    DETECTED_SUSE_RELEASE_FIELD_NAME: name,
                    "release_version": version,
                    "friendly_release_name": name}

        # NOTE: should be redundant as all SUSEs which have a
        # SuSE-release but no os-release have been deprecated
        # suse_release_path = "etc/SuSE-release"
        # if self._test_path(suse_release_path):
        #     release_info = self._read_config_file(suse_release_path)
        #     version_id = release_info['VERSION']
        #     patch_level = release_info.get('PATCHLEVEL', None)
        #     if patch_level:
        #         version_id = "%s.%s" % (version_id, patch_level)
        #     return {
        #         "name": 'SUSE',
        #         "version": version_id}

        return info
