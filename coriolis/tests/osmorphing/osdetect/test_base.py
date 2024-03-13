# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import os
from unittest import mock

from coriolis import exception
from coriolis.osmorphing.osdetect import base
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
