# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
import os
from unittest import mock

from coriolis import constants
from coriolis import exception
from coriolis.osmorphing import amazon as amazon_morphing
from coriolis.osmorphing import centos
from coriolis.osmorphing import oracle
from coriolis.osmorphing.osdetect import base
from coriolis.osmorphing import redhat as redhat_morphing
from coriolis.osmorphing import rocky
from coriolis.tests import test_base


class BaseOSDetectToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseOSDetectTools class."""

    @mock.patch.object(base.BaseOSDetectTools, '__abstractmethods__', set())
    def setUp(self):
        super(BaseOSDetectToolsTestCase, self).setUp()
        self.base_os_detect_tools = base.BaseOSDetectTools(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.operation_timeout)

    def test_returned_detected_os_info_fields(self):
        self.assertRaises(
            NotImplementedError,
            self.base_os_detect_tools.returned_detected_os_info_fields
        )

    def test_detect_os(self):
        self.assertRaises(
            NotImplementedError,
            self.base_os_detect_tools.detect_os
        )

    def test_set_environment(self):
        self.base_os_detect_tools.set_environment(mock.sentinel.environment)

        self.assertEqual(
            self.base_os_detect_tools._environment, mock.sentinel.environment
        )


class BaseLinuxOSDetectToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the BaseLinuxOSDetectTools class."""

    @mock.patch.object(
        base.BaseLinuxOSDetectTools, '__abstractmethods__', set()
    )
    def setUp(self):
        super(BaseLinuxOSDetectToolsTestCase, self).setUp()
        self.chroot_path = '/mock/chroot/path'
        self.os_root_dir = '/mock/os/root/dir'
        self.base_os_detect = base.BaseLinuxOSDetectTools(
            mock.sentinel.conn, self.os_root_dir,
            mock.sentinel.operation_timeout)

    def test_returned_detected_os_info_fields(self):
        result = self.base_os_detect.returned_detected_os_info_fields()

        self.assertEqual(
            result, base.REQUIRED_DETECTED_OS_FIELDS
        )

    @mock.patch.object(base.utils, 'read_ssh_file')
    def test__read_file(self, mock_read_ssh_file):
        result = self.base_os_detect._read_file(self.chroot_path)

        mocked_full_path = os.path.join(
            self.base_os_detect._os_root_dir, self.chroot_path)

        mock_read_ssh_file.assert_called_once_with(
            self.base_os_detect._conn, mocked_full_path)

        self.assertEqual(result, mock_read_ssh_file.return_value)

    @mock.patch.object(base.utils, 'read_ssh_ini_config_file')
    def test__read_config_file(self, mock_read_ssh_ini_config):
        result = self.base_os_detect._read_config_file(self.chroot_path)

        mocked_full_path = os.path.join(
            self.base_os_detect._os_root_dir, self.chroot_path)

        mock_read_ssh_ini_config.assert_called_once_with(
            self.base_os_detect._conn, mocked_full_path, check_exists=False)

        self.assertEqual(result, mock_read_ssh_ini_config.return_value)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_read_config_file')
    def test__get_os_release(self, mock_read_config_file):
        result = self.base_os_detect._get_os_release()

        mock_read_config_file.assert_called_once_with(
            "etc/os-release", check_exists=True)

        self.assertEqual(result, mock_read_config_file.return_value)

    @mock.patch.object(base.utils, 'test_ssh_path')
    def test__test_path(self, mock_test_ssh_path):
        result = self.base_os_detect._test_path(self.chroot_path)

        mocked_full_path = os.path.join(
            self.base_os_detect._os_root_dir, self.chroot_path)
        mock_test_ssh_path.assert_called_once_with(
            self.base_os_detect._conn, mocked_full_path)

        self.assertEqual(result, mock_test_ssh_path.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test__exec_cmd(self, mock_exec_ssh_cmd):
        result = self.base_os_detect._exec_cmd(mock.sentinel.cmd, timeout=120)

        mock_exec_ssh_cmd.assert_called_once_with(
            self.base_os_detect._conn, mock.sentinel.cmd,
            environment=self.base_os_detect._environment, get_pty=True,
            timeout=120)

        self.assertEqual(result, mock_exec_ssh_cmd.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test__exec_cmd_without_timeout(self, mock_exec_ssh_cmd):
        result = self.base_os_detect._exec_cmd(mock.sentinel.cmd)

        mock_exec_ssh_cmd.assert_called_once_with(
            self.base_os_detect._conn, mock.sentinel.cmd,
            environment=self.base_os_detect._environment, get_pty=True,
            timeout=self.base_os_detect._osdetect_operation_timeout)

        self.assertEqual(result, mock_exec_ssh_cmd.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd')
    def test__exec_cmd_with_exception(self, mock_exec_ssh_cmd):
        mock_exec_ssh_cmd.side_effect = exception.MinionMachineCommandTimeout

        self.assertRaises(
            exception.OSMorphingSSHOperationTimeout,
            self.base_os_detect._exec_cmd,
            mock.sentinel.cmd
        )

    @mock.patch.object(base.utils, 'exec_ssh_cmd_chroot')
    def test__exec_cmd_chroot(self, mock_exec_ssh_cmd_chroot):
        result = self.base_os_detect._exec_cmd_chroot(
            mock.sentinel.cmd, timeout=120)

        mock_exec_ssh_cmd_chroot.assert_called_once_with(
            self.base_os_detect._conn, self.base_os_detect._os_root_dir,
            mock.sentinel.cmd, environment=self.base_os_detect._environment,
            get_pty=True, timeout=120)

        self.assertEqual(result, mock_exec_ssh_cmd_chroot.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd_chroot')
    def test__exec_cmd_chroot_without_timeout(self, mock_exec_ssh_cmd_chroot):
        result = self.base_os_detect._exec_cmd_chroot(mock.sentinel.cmd)

        mock_exec_ssh_cmd_chroot.assert_called_once_with(
            self.base_os_detect._conn, self.base_os_detect._os_root_dir,
            mock.sentinel.cmd, environment=self.base_os_detect._environment,
            get_pty=True,
            timeout=self.base_os_detect._osdetect_operation_timeout)

        self.assertEqual(result, mock_exec_ssh_cmd_chroot.return_value)

    @mock.patch.object(base.utils, 'exec_ssh_cmd_chroot')
    def test__exec_cmd_chroot_with_exception(self, mock_exec_ssh_cmd_chroot):
        mock_exec_ssh_cmd_chroot.side_effect = [
            exception.MinionMachineCommandTimeout]

        self.assertRaises(
            exception.OSMorphingSSHOperationTimeout,
            self.base_os_detect._exec_cmd_chroot,
            mock.sentinel.cmd
        )


class LinuxOSDetectUsingOSReleaseTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the LinuxOSDetectUsingOSRelease class."""

    def setUp(self):
        super(LinuxOSDetectUsingOSReleaseTestCase, self).setUp()
        self.os_detect = base.LinuxOSDetectUsingOSRelease(
            mock.sentinel.conn, mock.sentinel.os_root_dir,
            mock.sentinel.operation_timeout)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_empty_release(self, mock_get_os_release):
        with self.assertLogs(
                'coriolis.osmorphing.osdetect.base',
                level=logging.WARNING) as logs:
            mock_get_os_release.return_value = {}
            self.assertEqual(self.os_detect.detect_os(), {})
            mock_get_os_release.return_value = None
            self.assertEqual(self.os_detect.detect_os(), {})
        self.assertEqual(len(logs.output), 2)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_missing_name(self, mock_get_os_release):
        mock_get_os_release.return_value = {"ID": "rocky", "VERSION_ID": "8"}
        with self.assertLogs(
                'coriolis.osmorphing.osdetect.base',
                level=logging.WARNING):
            self.assertEqual(self.os_detect.detect_os(), {})

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_missing_version(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "rocky", "NAME": "Rocky Linux"}
        with self.assertLogs(
                'coriolis.osmorphing.osdetect.base',
                level=logging.WARNING):
            self.assertEqual(self.os_detect.detect_os(), {})

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_centos_stream(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "centos",
            "VERSION_ID": "9",
            "NAME": "CentOS Stream",
        }
        expected = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": "CentOS Stream",
            "release_version": "9",
            "friendly_release_name": "CentOS Stream Version 9",
        }
        self.assertEqual(self.os_detect.detect_os(), expected)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_amazon(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "amzn",
            "VERSION_ID": mock.sentinel.version,
            "NAME": amazon_morphing.AMAZON_DISTRO_NAME_IDENTIFIER,
        }

        expected = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": amazon_morphing.AMAZON_DISTRO_NAME_IDENTIFIER,
            "release_version": mock.sentinel.version,
            "friendly_release_name": "Amazon Linux Version %s" % (
                mock.sentinel.version)
        }

        self.assertEqual(self.os_detect.detect_os(), expected)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_rhel(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "rhel",
            "VERSION_ID": "8.4",
            "NAME": "Red Hat Enterprise Linux",
        }

        expected = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": redhat_morphing.RED_HAT_DISTRO_IDENTIFIER,
            "release_version": '8.4',
            "friendly_release_name": "%s Version %s" % (
                redhat_morphing.RED_HAT_DISTRO_IDENTIFIER, '8.4')
        }

        self.assertEqual(self.os_detect.detect_os(), expected)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_centos_linux(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "centos",
            "VERSION_ID": "7.9",
            "NAME": centos.CENTOS_LINUX_DISTRO_IDENTIFIER,
        }

        expected = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": centos.CENTOS_LINUX_DISTRO_IDENTIFIER,
            "release_version": '7.9',
            "friendly_release_name": "%s Version %s" % (
                centos.CENTOS_LINUX_DISTRO_IDENTIFIER, '7.9'),
        }

        self.assertEqual(self.os_detect.detect_os(), expected)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_centos_stream_8(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "centos",
            "VERSION_ID": "8.3",
            "NAME": "CentOS Stream",
        }

        expected = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": centos.CENTOS_STREAM_DISTRO_IDENTIFIER,
            "release_version": '8.3',
            "friendly_release_name": "%s Version %s" % (
                centos.CENTOS_STREAM_DISTRO_IDENTIFIER, '8.3')
        }

        self.assertEqual(self.os_detect.detect_os(), expected)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_centos_stream_10(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "centos",
            "VERSION_ID": "10",
            "NAME": "CentOS Stream",
        }

        expected = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": centos.CENTOS_STREAM_DISTRO_IDENTIFIER,
            "release_version": '10',
            "friendly_release_name": "%s Version %s" % (
                centos.CENTOS_STREAM_DISTRO_IDENTIFIER, '10')
        }

        self.assertEqual(self.os_detect.detect_os(), expected)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_almalinux(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "almalinux",
            "VERSION_ID": "9.4",
            "NAME": centos.ALMALINUX_DISTRO_IDENTIFIER,
            "ID_LIKE": "rhel centos fedora",
        }

        expected = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": centos.ALMALINUX_DISTRO_IDENTIFIER,
            "release_version": '9.4',
            "friendly_release_name": "%s Version %s" % (
                centos.ALMALINUX_DISTRO_IDENTIFIER, '9.4'),
        }

        self.assertEqual(self.os_detect.detect_os(), expected)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_oracle(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "ol",
            "VERSION_ID": "8.4",
            "NAME": oracle.ORACLE_LINUX_SERVER_DISTRO_IDENTIFIER,
        }

        expected = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": (
                oracle.ORACLE_LINUX_SERVER_DISTRO_IDENTIFIER),
            "release_version": '8.4',
            "friendly_release_name": "%s Version %s" % (
                oracle.ORACLE_LINUX_SERVER_DISTRO_IDENTIFIER, '8.4')
        }

        self.assertEqual(self.os_detect.detect_os(), expected)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_rocky(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "rocky",
            "VERSION_ID": "8.4",
            "NAME": "Rocky Linux",
        }

        expected = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": rocky.ROCKY_LINUX_DISTRO_IDENTIFIER,
            "release_version": '8.4',
            "friendly_release_name": "Rocky Linux Version 8.4"
        }

        self.assertEqual(self.os_detect.detect_os(), expected)

    @mock.patch.object(base.BaseLinuxOSDetectTools, '_get_os_release')
    def test_detect_os_rocky_10(self, mock_get_os_release):
        mock_get_os_release.return_value = {
            "ID": "rocky",
            "VERSION_ID": "10.1",
            "NAME": "Rocky Linux",
            "PRETTY_NAME": "Rocky Linux 10.1 (Red Quartz)",
        }

        expected = {
            "os_type": constants.OS_TYPE_LINUX,
            "distribution_name": rocky.ROCKY_LINUX_DISTRO_IDENTIFIER,
            "release_version": '10.1',
            "friendly_release_name": "Rocky Linux Version 10.1"
        }

        self.assertEqual(self.os_detect.detect_os(), expected)
