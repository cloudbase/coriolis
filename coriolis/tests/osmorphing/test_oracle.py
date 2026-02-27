# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing import oracle
from coriolis.osmorphing.osdetect import oracle as oracle_detect
from coriolis.tests import test_base


class BaseOracleMorphingToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test case for the BaseOracleMorphingTools class."""

    def setUp(self):
        super(BaseOracleMorphingToolsTestCase, self).setUp()
        self.detected_os_info = {
            'os_type': 'linux',
            'distribution_name': oracle_detect.ORACLE_DISTRO_IDENTIFIER,
            'release_version': '6',
            'friendly_release_name': mock.sentinel.friendly_release_name,
        }
        self.oracle_morphing_tools = oracle.BaseOracleMorphingTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.os_root_dir, mock.sentinel.hypervisor,
            mock.sentinel.event_manager, self.detected_os_info,
            mock.sentinel.osmorphing_parameters)

    def test_check_os_supported(self):
        result = oracle.BaseOracleMorphingTools.check_os_supported(
            self.detected_os_info)

        self.assertTrue(result)

    def test_check_os_not_supported(self):
        self.detected_os_info['distribution_name'] = 'unsupported'

        result = oracle.BaseOracleMorphingTools.check_os_supported(
            self.detected_os_info)

        self.assertFalse(result)
