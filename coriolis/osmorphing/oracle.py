import re

from coriolis import constants
from coriolis import exception
from coriolis.osmorphing import redhat


class OracleMorphingTools(redhat.RedHatMorphingTools):
    def _check_os(self):
        oracle_release_path = "etc/oracle-release"
        if self._test_path(oracle_release_path):
            release_info = self._read_file(
                oracle_release_path).decode().split('\n')[0].strip()
            m = re.match(r"^(.*) release ([0-9].*)$", release_info)
            if m:
                distro, version = m.groups()
                return (distro, version)

    def install_packages(self, package_names):
        self._yum_install(package_names, self._enable_repos)

    def pre_packages_install(self):
        self._enable_repos = []
        super(OracleMorphingTools, self).pre_packages_install()

        if self._platform == constants.PLATFORM_OPENSTACK:
            self._enable_cloud_init_repos()

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

    def _run_dracut(self):
        self._run_dracut_base('kernel')
        self._run_dracut_base('kernel-uek')
