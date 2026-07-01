# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis import constants
from coriolis.osmorphing.osdetect import common
from coriolis.tests import test_base


class CommonOSDetectTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the common os-release detect helper."""

    def test_detect_os_from_os_release_empty_release(self):
        self.assertEqual(
            common.detect_os_from_os_release({}, match_ids={"rocky"}), {})
        self.assertEqual(
            common.detect_os_from_os_release(None, match_ids={"rocky"}), {})

    def test_detect_os_from_os_release_id_mismatch(self):
        os_release = {"ID": "centos", "VERSION_ID": "9"}
        self.assertEqual(
            common.detect_os_from_os_release(
                os_release, match_ids={"rocky"}), {})

    def test_detect_os_from_os_release_missing_version(self):
        os_release = {"ID": "rocky"}
        self.assertEqual(
            common.detect_os_from_os_release(
                os_release, match_ids={"rocky"}), {})

    def test_detect_os_from_os_release_centos_stream(self):
        os_release = {
            "ID": "centos",
            "VERSION_ID": "9",
            "NAME": "CentOS Stream",
        }
        expected = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": common.CENTOS_STREAM_DISTRO_IDENTIFIER,
            "release_version": "9",
            "friendly_release_name": "CentOS Stream Version 9",
        }
        self.assertEqual(
            common.detect_os_from_os_release(
                os_release, match_ids={"centos", "almalinux"}),
            expected)

    def test_detect_os_from_os_release_almalinux_maps_to_centos(self):
        os_release = {
            "ID": "almalinux",
            "VERSION_ID": "9.4",
            "NAME": "AlmaLinux",
        }
        result = common.detect_os_from_os_release(
            os_release, match_ids={"centos", "almalinux"})
        self.assertEqual(result["distribution_name"], "CentOS")
        self.assertEqual(result["release_version"], "9.4")
