# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing import redhat
from coriolis.osmorphing.osdetect import oracle as oracle_detect


ORACLE_DISTRO_IDENTIFIER = oracle_detect.ORACLE_DISTRO_IDENTIFIER


class BaseOracleMorphingTools(redhat.BaseRedHatMorphingTools):

    @classmethod
    def check_os_supported(cls, detected_os_info):
        if detected_os_info['distribution_name'] != (
                ORACLE_DISTRO_IDENTIFIER):
            return False
        return cls._version_supported_util(
            detected_os_info['release_version'], minimum=6)

    def _run_dracut(self):
        self._run_dracut_base('kernel')
        self._run_dracut_base('kernel-uek')

    def _enable_oracle_repos(self):

        major_version = int(self._version.split(".")[0])
        if major_version < 8:
            self._yum_install(['yum-utils'])
            # TODO(apilotti): for ULN users, use the corresponding repos
            # e.g.: ol7_x86_64_addons
            self._exec_cmd_chroot(
                "yum-config-manager --add-repo "
                "http://public-yum.oracle.com/public-yum-ol%s.repo" %
                major_version)

            self._enable_repos = ["ol%s_software_collections" % major_version,
                                  "ol%s_addons" % major_version]
        else:
            self._yum_install(['oraclelinux-release-el%s' % major_version])
            self._exec_cmd_chroot(
                "yum config-manager --enable ol%(release)s_appstream "
                "ol%(release)s_UEKR6" % {"release": major_version})
