# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis import exception
from coriolis.osmorphing import oracle as base_oracle
from coriolis.providers.openstack.osmorphing import redhat


class OracleMorphingTools(base_oracle.BaseOracleMorphingTools,
                          redhat.RedHatMorphingTools):
    def _enable_cloud_init_repos(self):
        self._yum_install(['yum-utils'])

        distro, version = self.check_os()
        major_version = version.split(".")[0]

        # TODO: for ULN users, use the corresponding repos
        # e.g.: ol7_x86_64_addons
        self._exec_cmd_chroot(
            "yum-config-manager --add-repo "
            "http://public-yum.oracle.com/public-yum-ol%s.repo" %
            major_version)

        self._enable_repos = ["ol%s_software_collections" % major_version,
                              "ol%s_addons" % major_version]

        if major_version == "7":
            try:
                self._yum_install(["python-cheetah"], self._enable_repos)
            except exception.CoriolisException:
                # The python-pygments RPM required by python-cheetah is called
                # python27-python-pygments. Force the installation.
                self._yum_install(
                    ["python-markdown", "python27-python-pygments"],
                    self._enable_repos)

                self._exec_cmd_chroot(
                    "rpm -Uvh %s --nodeps" %
                    "http://public-yum.oracle.com/repo/OracleLinux/OL7/addons/"
                    "x86_64/getPackage/python-cheetah-2.4.1-1.el7.x86_64.rpm")

    def pre_packages_install(self, package_names):
        self._enable_repos = []
        super(OracleMorphingTools, self).pre_packages_install(
            package_names)

        if self._platform == constants.PLATFORM_OPENSTACK:
            self._enable_cloud_init_repos()
