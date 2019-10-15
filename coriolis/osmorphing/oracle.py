# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import re

from coriolis.osmorphing import redhat


class BaseOracleMorphingTools(redhat.BaseRedHatMorphingTools):
    def _check_os(self):
        oracle_release_path = "etc/oracle-release"
        if self._test_path(oracle_release_path):
            release_info = self._read_file(
                oracle_release_path).decode().splitlines()
            if release_info:
                m = re.match(r"^(.*) release ([0-9].*)$",
                             release_info[0].strip())
                if m:
                    distro, version = m.groups()
                    return (distro, version)

    def _run_dracut(self):
        self._run_dracut_base('kernel')
        self._run_dracut_base('kernel-uek')
