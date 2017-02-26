# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.osmorphing import debian


class BaseUbuntuMorphingTools(debian.BaseDebianMorphingTools):
    def _check_os(self):
        config = self._read_config_file("etc/lsb-release", check_exists=True)
        dist_id = config.get('DISTRIB_ID')
        if dist_id == 'Ubuntu':
            release = config.get('DISTRIB_RELEASE')
            return (dist_id, release)
