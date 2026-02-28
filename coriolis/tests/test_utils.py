# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

import datetime
import hashlib
import json
import logging
import os
import socket
from unittest import mock
import uuid

import ddt
from webob import exc

from coriolis import constants
from coriolis import exception
from coriolis.tests import test_base
from coriolis.tests import testutils
from coriolis import utils


class CoriolisTestException(Exception):
    pass


@ddt.ddt
class UtilsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis utils module."""

    def setUp(self):
        super(UtilsTestCase, self).setUp()
        self.mock_func = mock.Mock()
        self.mock_ssh = mock.Mock()
        self.mock_sftp = mock.Mock()
        self.mock_file = mock.Mock()
        self.mock_conn = mock.Mock()
        self.mock_stdout = mock.Mock()
        self.mock_process = mock.Mock()

    @mock.patch('oslo_log.log.setup')
    def test_setup_logging(self, mock_setup):
        utils.setup_logging()
        mock_setup.assert_called_once_with(utils.CONF, 'coriolis')

    @mock.patch.object(utils, 'get_exception_details')
    def test_ignore_exceptions(self, mock_get_details):
        mock_get_details.return_value = 'Test exception details'
        self.mock_func.side_effect = Exception

        with self.assertLogs('coriolis.utils', level=logging.WARN):
            utils.ignore_exceptions(self.mock_func)()

        mock_get_details.assert_called_once_with()

    def test_get_single_result_empty_list(self):
        self.assertRaises(KeyError, utils.get_single_result, [])

    def test_get_single_result_multiple_elements(self):
        self.assertRaises(KeyError, utils.get_single_result, [1, 2])

    def test_get_single_result_single_element(self):
        result = utils.get_single_result([1])
        self.assertEqual(result, 1)

    def test_retry_on_error_no_exception(self):
        result = utils.retry_on_error(
            max_attempts=5, sleep_seconds=0,
            terminal_exceptions=[])(self.mock_func)(
                mock.sentinel.arg1, kwarg1=mock.sentinel.kwarg1)

        self.assertEqual(result, self.mock_func.return_value)
        self.assertEqual(self.mock_func.call_count, 1)
        self.mock_func.assert_called_with(
            mock.sentinel.arg1, kwarg1=mock.sentinel.kwarg1)

    def test_retry_on_error_exception_keyboard_interrupt(self):
        self.mock_func.side_effect = KeyboardInterrupt

        self.assertRaises(KeyboardInterrupt, utils.retry_on_error(
            max_attempts=5, sleep_seconds=0,
            terminal_exceptions=[])(self.mock_func))

        self.assertEqual(self.mock_func.call_count, 1)

    def test_retry_on_error_terminal_exception(self):
        self.mock_func.side_effect = CoriolisTestException

        self.assertRaises(CoriolisTestException, utils.retry_on_error(
            max_attempts=5, sleep_seconds=0,
            terminal_exceptions=[CoriolisTestException])(self.mock_func))

        self.assertEqual(self.mock_func.call_count, 1)

    def test_retry_on_error_exception_retry_max_attempts(self):
        self.mock_func.side_effect = CoriolisTestException

        self.assertRaises(CoriolisTestException, utils.retry_on_error(
            max_attempts=5, sleep_seconds=0,
            terminal_exceptions=[])(self.mock_func))

        self.assertEqual(self.mock_func.call_count, 5)

    def test_get_udev_net_rules(self):
        net_ifaces_info = {"eth0": "AA:BB:CC:DD:EE:FF",
                           "eth1": "FF:EE:DD:CC:BB:AA"}
        expected_result = (
            'SUBSYSTEM=="net", ACTION=="add", DRIVERS=="?*", '
            'ATTR{address}=="aa:bb:cc:dd:ee:ff", '
            'NAME="eth0"\n'
            'SUBSYSTEM=="net", ACTION=="add", DRIVERS=="?*", '
            'ATTR{address}=="ff:ee:dd:cc:bb:aa", '
            'NAME="eth1"\n'
        )

        result = utils.get_udev_net_rules(net_ifaces_info)
        self.assertEqual(result, expected_result)

    @mock.patch.object(utils, 'exec_ssh_cmd')
    def test_parse_os_release(self, mock_ssh_cmd):
        mock_ssh_cmd.return_value = 'ID=ubuntu\nVERSION_ID="20.04"\n'

        result = utils.parse_os_release(self.mock_ssh)

        mock_ssh_cmd.assert_called_once_with(
            self.mock_ssh,
            "[ -f '/etc/os-release' ] && cat /etc/os-release || true")

        self.assertEqual(result, ('ubuntu', '20.04'))

    @mock.patch.object(utils, 'exec_ssh_cmd')
    def test_parse_os_release_no_equal(self, mock_ssh_cmd):
        mock_ssh_cmd.return_value = 'ID=ubuntu\nVERSION_ID="20.04"\nNOEQUAL\n'

        result = utils.parse_os_release(self.mock_ssh)

        mock_ssh_cmd.assert_called_once_with(
            self.mock_ssh,
            "[ -f '/etc/os-release' ] && cat /etc/os-release || true")

        self.assertEqual(result, ("ubuntu", "20.04"))

    @mock.patch.object(utils, 'exec_ssh_cmd')
    def test_parse_os_release_missing_fields(self, mock_ssh_cmd):
        mock_ssh_cmd.return_value = 'NO_ID\nNO_VERSION_ID\n'

        result = utils.parse_os_release(self.mock_ssh)

        mock_ssh_cmd.assert_called_once_with(
            self.mock_ssh,
            "[ -f '/etc/os-release' ] && cat /etc/os-release || true")

        self.assertIsNone(result)

    @mock.patch.object(utils, 'exec_ssh_cmd')
    def test_parse_lsb_release(self, mock_ssh_cmd):
        mock_ssh_cmd.return_value = 'Distributor ID: Ubuntu\nRelease: 20.04\n'

        result = utils.parse_lsb_release(self.mock_ssh)

        mock_ssh_cmd.assert_called_once_with(
            self.mock_ssh, "lsb_release -a || true")

        self.assertEqual(result, ("Ubuntu", "20.04"))

    @mock.patch.object(utils, 'exec_ssh_cmd')
    def test_parse_lsb_release_missing_fields(self, mock_ssh_cmd):
        mock_ssh_cmd.return_value = 'No Distributor ID\nNo Release\n'

        result = utils.parse_lsb_release(self.mock_ssh)

        mock_ssh_cmd.assert_called_once_with(
            self.mock_ssh,
            "lsb_release -a || true")

        self.assertIsNone(result)

    @mock.patch.object(utils, 'parse_os_release')
    def test_get_linux_os_info(self, mock_parse_os_release):
        result = utils.get_linux_os_info(self.mock_ssh)

        mock_parse_os_release.assert_called_once_with(self.mock_ssh)
        self.assertEqual(result, mock_parse_os_release.return_value)

    @mock.patch.object(utils, 'parse_os_release')
    @mock.patch.object(utils, 'parse_lsb_release')
    def test_get_linux_os_info_lsb_release(self, mock_parse_lsb_release,
                                           mock_parse_os_release):
        mock_parse_os_release.return_value = None
        mock_parse_lsb_release.return_value = ("ubuntu", "20.04")

        result = utils.get_linux_os_info(self.mock_ssh)

        mock_parse_os_release.assert_called_once_with(self.mock_ssh)
        mock_parse_lsb_release.assert_called_once_with(self.mock_ssh)

        self.assertEqual(result, ("ubuntu", "20.04"))

    def test_test_ssh_path_exists(self):
        self.mock_sftp.stat.return_value = None
        self.mock_ssh.open_sftp.return_value = self.mock_sftp

        result = utils.test_ssh_path(self.mock_ssh, "/test/file")

        self.mock_ssh.open_sftp.assert_called_once_with()
        self.mock_sftp.stat.assert_called_once_with("/test/file")

        self.assertEqual(result, True)

    def test_test_ssh_path_not_exists(self):
        self.mock_sftp.stat.side_effect = IOError(2,
                                                  "No such file or directory")
        self.mock_ssh.open_sftp.return_value = self.mock_sftp

        result = utils.test_ssh_path(self.mock_ssh, "/nonexistent/path")

        self.mock_ssh.open_sftp.assert_called_once_with()
        self.mock_sftp.stat.assert_called_once_with("/nonexistent/path")

        self.assertEqual(result, False)

    def test_test_ssh_path_raises(self):
        self.mock_sftp.stat.side_effect = IOError(1, "Some other error")
        self.mock_ssh.open_sftp.return_value = self.mock_sftp

        original_test_ssh_path = testutils.get_wrapped_function(
            utils.test_ssh_path)

        self.assertRaises(IOError, original_test_ssh_path, self.mock_ssh,
                          "/nonexistent/path")

    def test_read_ssh_file(self):
        self.mock_sftp.open.return_value = self.mock_file
        self.mock_ssh.open_sftp.return_value = self.mock_sftp

        result = utils.read_ssh_file(self.mock_ssh, "/test/file")

        self.mock_ssh.open_sftp.assert_called_once_with()
        self.mock_sftp.open.assert_called_once_with("/test/file", "rb")

        self.assertEqual(result, self.mock_file.read.return_value)

    def test_write_ssh_file(self):
        self.mock_sftp.open.return_value = self.mock_file
        self.mock_ssh.open_sftp.return_value = self.mock_sftp

        utils.write_ssh_file(self.mock_ssh, "/test/file", b"file content")

        self.mock_ssh.open_sftp.assert_called_once_with()
        self.mock_sftp.open.assert_called_once_with("/test/file", "wb")
        self.mock_file.write.assert_called_once_with(b"file content")

    @mock.patch('base64.b64encode')
    def test_write_winrm_file(self, mock_b64encode):
        self.mock_conn.test_path.return_value = True
        self.mock_conn.exec_ps_command.return_value = None

        utils.write_winrm_file(self.mock_conn, "/test/file", "file content",
                               overwrite=True)

        self.mock_conn.test_path.assert_called_once_with("/test/file")
        self.assertEqual(self.mock_conn.exec_ps_command.call_count, 2)
        mock_b64encode.assert_called_once_with(b"file content")

    @mock.patch('base64.b64encode')
    def test_write_winrm_file_file_does_not_exist(self, mock_b64encode):
        self.mock_conn.test_path.return_value = False
        self.mock_conn.exec_ps_command.return_value = None

        utils.write_winrm_file(self.mock_conn, "/nonexistent/path",
                               "nonexistent-file", overwrite=True)

        self.mock_conn.test_path.assert_called_once_with("/nonexistent/path")
        mock_b64encode.assert_called_once_with(b"nonexistent-file")

    def test_write_winrm_file_file_exists_overwrite_false(self):
        self.mock_conn.test_path.return_value = True
        self.mock_conn.exec_ps_command.return_value = None

        self.assertRaises(exception.CoriolisException, utils.write_winrm_file,
                          self.mock_conn, "/test/file", "file content",
                          overwrite=False)

        self.mock_conn.test_path.assert_called_once_with("/test/file")

    @mock.patch('base64.b64encode')
    def test_write_winrm_file_content_is_not_string(self, mock_b64encode):
        self.mock_conn.test_path.return_value = False
        self.mock_conn.exec_ps_command.return_value = None

        utils.write_winrm_file(self.mock_conn, "/test/file", "file content",
                               overwrite=True)

        self.mock_conn.test_path.assert_called_once_with("/test/file")
        mock_b64encode.assert_called_once_with(b"file content")

    @mock.patch('base64.b64encode')
    def test_write_winrm_file_long_content(self, mock_b64encode):
        self.mock_conn.test_path.return_value = False
        self.mock_conn.exec_ps_command.return_value = None
        mock_b64encode.return_value = b'encoded_content'
        long_content = "a" * 3000

        utils.write_winrm_file(self.mock_conn, "/test/file", long_content,
                               overwrite=True)

        self.mock_conn.test_path.assert_called_once_with("/test/file")
        self.assertEqual(self.mock_conn.exec_ps_command.call_count, 2)
        expected_calls = [mock.call(long_content[:2048].encode()),
                          mock.call(long_content[2048:].encode())]
        mock_b64encode.assert_has_calls(expected_calls)

    def test_list_ssh_dir(self):
        self.mock_ssh.open_sftp.return_value = self.mock_sftp

        result = utils.list_ssh_dir(self.mock_ssh, "/test/file")
        self.assertEqual(result, self.mock_sftp.listdir.return_value)

        self.mock_ssh.open_sftp.assert_called_once_with()
        self.mock_sftp.listdir.assert_called_once_with("/test/file")

    def test_exec_ssh_cmd(self):
        self.mock_stdout.read.return_value = b'output\r\n'
        self.mock_stdout.channel.recv_exit_status.return_value = 0
        self.mock_ssh.exec_command.return_value = (
            None, self.mock_stdout, self.mock_stdout)

        result = utils.exec_ssh_cmd(self.mock_ssh, "command")

        self.mock_ssh.exec_command.assert_called_once_with(
            "command", environment=None, get_pty=False, timeout=None)

        self.assertEqual(result, 'output\n')

    def test_exec_ssh_cmd_timeout_with_timeout(self):
        self.mock_stdout.read.return_value = b'output\n'
        self.mock_stdout.channel.recv_exit_status.return_value = 0
        self.mock_ssh.exec_command.return_value = (None, self.mock_stdout,
                                                   self.mock_stdout)

        result = utils.exec_ssh_cmd(self.mock_ssh, "command", timeout=10)

        self.mock_ssh.exec_command.assert_called_once_with(
            "command", environment=None, get_pty=False, timeout=10.0)

        expected = self.mock_stdout.read.return_value.decode(
            'utf-8', errors='replace')
        self.assertEqual(result, expected)

    def test_exec_ssh_cmd_getpeername_value_error(self):
        self.mock_stdout.read.return_value = b'output\n'
        self.mock_stdout.channel.recv_exit_status.return_value = 0
        self.mock_ssh.exec_command.return_value = (None, self.mock_stdout,
                                                   self.mock_stdout)

        self.mock_ssh.get_transport.return_value.sock.\
            getpeername.side_effect = ValueError

        with self.assertLogs('coriolis.utils', level=logging.WARN):
            output = utils.exec_ssh_cmd(self.mock_ssh, "command")

        self.mock_ssh.exec_command.assert_called_once_with(
            "command", environment=None, get_pty=False, timeout=None)

        expected = self.mock_stdout.read.return_value.decode(
            'utf-8', errors='replace')
        self.assertEqual(output, expected)

    def test_exec_ssh_cmd_timeout(self):
        self.mock_stdout.read.side_effect = socket.timeout
        self.mock_ssh.exec_command.return_value = (None, self.mock_stdout,
                                                   self.mock_stdout)

        self.assertRaises(exception.MinionMachineCommandTimeout,
                          utils.exec_ssh_cmd, self.mock_ssh, "command")

        self.mock_ssh.exec_command.assert_called_once_with(
            "command", environment=None, get_pty=False, timeout=None)

    def test_exec_ssh_cmd_exception(self):
        self.mock_stdout.channel.recv_exit_status.return_value = 1
        self.mock_ssh.exec_command.return_value = (None, self.mock_stdout,
                                                   self.mock_stdout)

        original_exec_ssh_cmd = testutils.get_wrapped_function(
            utils.exec_ssh_cmd)

        self.assertRaises(exception.CoriolisException, original_exec_ssh_cmd,
                          self.mock_ssh, "command")

        self.mock_ssh.exec_command.assert_called_once_with(
            "command", environment=None, get_pty=False, timeout=None)

    def test_exec_ssh_cmd_chroot(self):
        self.mock_stdout.read.return_value = b'output\n'
        self.mock_stdout.channel.recv_exit_status.return_value = 0
        self.mock_ssh.exec_command.return_value = (None, self.mock_stdout,
                                                   self.mock_stdout)

        result = utils.exec_ssh_cmd_chroot(
            self.mock_ssh, "/chroot /bin/bash -c", "command")

        self.mock_ssh.exec_command.assert_called_once_with(
            "sudo -E chroot /chroot /bin/bash -c command",
            environment=None, get_pty=False, timeout=None)

        expected = self.mock_stdout.read.return_value.decode(
            'utf-8', errors='replace')
        self.assertEqual(result, expected)

    def test_check_fs(self):
        self.mock_stdout.read.return_value.replace.return_value = \
            self.mock_stdout.read.return_value
        self.mock_stdout.channel.recv_exit_status.return_value = 0
        self.mock_ssh.exec_command.return_value = (None, self.mock_stdout,
                                                   self.mock_stdout)

        utils.check_fs(self.mock_ssh, "ext4", "/dev/sda1")

        self.mock_ssh.exec_command.assert_called_once_with(
            "sudo fsck -p -t ext4 /dev/sda1", environment=None, get_pty=True,
            timeout=None)

    @mock.patch.object(utils, 'exec_ssh_cmd')
    def test_check_fs_exception(self, mock_exec_ssh_cmd):
        mock_exec_ssh_cmd.side_effect = exception.CoriolisException()

        self.assertRaises(exception.CoriolisException, utils.check_fs,
                          self.mock_ssh, "ext4", "/dev/sda1")

        mock_exec_ssh_cmd.assert_called_once_with(
            self.mock_ssh, "sudo fsck -p -t ext4 /dev/sda1", get_pty=True)

    @mock.patch.object(utils, 'exec_ssh_cmd')
    def test_run_xfs_repair(self, mock_exec_ssh_cmd):
        mock_exec_ssh_cmd.return_value = "/tmp/tmp_dir\n"

        utils.run_xfs_repair(self.mock_ssh, "/dev/sda1")

        expected_calls = [
            mock.call(self.mock_ssh, "mktemp -d"),
            mock.call(self.mock_ssh, "sudo mount /dev/sda1 /tmp/tmp_dir",
                      get_pty=True),
            mock.call(self.mock_ssh, "sudo umount /tmp/tmp_dir",
                      get_pty=True),
            mock.call(self.mock_ssh, "sudo xfs_repair /dev/sda1",
                      get_pty=True),
        ]
        mock_exec_ssh_cmd.assert_has_calls(expected_calls)

    def test_run_xfs_repair_exception(self):
        self.mock_ssh.exec_command.side_effect = Exception()

        with self.assertLogs('coriolis.utils', level=logging.WARN):
            utils.run_xfs_repair(self.mock_ssh, "/dev/sda1")

    @mock.patch('socket.socket')
    def test_check_port_open_success(self, mock_socket):
        mock_socket.return_value.connect.return_value = None

        result = utils._check_port_open('localhost', 8080)
        self.assertEqual(result, True)
        mock_socket.return_value.close.assert_called_once()

    @mock.patch('socket.socket')
    def test_check_port_open_exception(self, mock_socket):
        mock_socket.return_value.connect.side_effect = socket.error

        result = utils._check_port_open('localhost', 8080)
        self.assertEqual(result, False)
        mock_socket.return_value.close.assert_called_once()

    @mock.patch.object(utils, '_check_port_open')
    @mock.patch('time.sleep')
    def test_wait_for_port_connectivity(self, mock_sleep,
                                        mock_check_port_open):
        mock_check_port_open.return_value = True
        mock_sleep.return_value = None

        utils.wait_for_port_connectivity('localhost', 8080)
        mock_check_port_open.assert_called_with('localhost', 8080)

    @mock.patch.object(utils, '_check_port_open')
    @mock.patch('time.sleep')
    def test_wait_for_port_connectivity_exception(self, mock_sleep,
                                                  mock_check_port_open):
        mock_check_port_open.return_value = False
        mock_sleep.return_value = None

        self.assertRaises(exception.CoriolisException,
                          utils.wait_for_port_connectivity, 'localhost', 8080,
                          max_wait=1)
        mock_check_port_open.assert_called_with('localhost', 8080)

    @mock.patch('subprocess.Popen')
    def test_exec_process(self, mock_popen):
        self.mock_process.returncode = 0
        self.mock_process.communicate.return_value = (b'stdout', b'stderr')
        mock_popen.return_value = self.mock_process

        result = utils.exec_process("command")

        mock_popen.assert_called_once_with("command", stdout=-1, stderr=-1)
        self.assertEqual(result, self.mock_process.communicate.return_value[0])

    @mock.patch('subprocess.Popen')
    def test_exec_process_exception(self, mock_popen):
        self.mock_process.returncode = 1
        self.mock_process.communicate.return_value = (b'stdout', b'stderr')
        mock_popen.return_value = self.mock_process

        self.assertRaises(exception.CoriolisException, utils.exec_process,
                          "command")

        mock_popen.assert_called_once_with("command", stdout=-1, stderr=-1)

    @mock.patch.object(utils, 'exec_process')
    def test_get_disk_info(self, mock_exec_process):
        mock_exec_process.return_value = b'{"format": "vpc"}'

        result = utils.get_disk_info('disk_path')

        self.assertEqual(result, {'format': 'vhd'})

        mock_exec_process.assert_called_with([utils.CONF.qemu_img_path,
                                              'info', '--output=json',
                                              'disk_path'])

    @mock.patch.object(utils, 'exec_process')
    def test_convert_disk_format(self, mock_exec_process):
        mock_exec_process.return_value = None

        utils.convert_disk_format('disk_path', 'target_disk_path',
                                  constants.DISK_FORMAT_VHD,
                                  preallocated=True)

        mock_exec_process.assert_called_with([utils.CONF.qemu_img_path,
                                              'convert', '-O', 'vpc', '-o',
                                              'subformat=fixed',
                                              'disk_path',
                                              'target_disk_path'])

    def test_convert_disk_format_not_vhd(self):
        self.assertRaises(NotImplementedError, utils.convert_disk_format,
                          'disk_path', 'target_disk_path', 'not_vhd',
                          preallocated=True)

    @mock.patch.object(utils, 'exec_process')
    @mock.patch.object(utils, 'ignore_exceptions')
    def test_convert_disk_format_exception(self, mock_ignore_exceptions,
                                           mock_exec_process):
        mock_remove = mock.MagicMock()
        mock_exec_process.side_effect = exception.CoriolisException
        mock_ignore_exceptions.return_value = mock_remove

        self.assertRaises(exception.CoriolisException,
                          utils.convert_disk_format, 'disk_path',
                          'target_disk_path', constants.DISK_FORMAT_VHD,
                          preallocated=True)

        mock_exec_process.assert_called_with(
            [utils.CONF.qemu_img_path, 'convert', '-O', 'vpc', '-o',
             'subformat=fixed', 'disk_path', 'target_disk_path'])

        mock_ignore_exceptions.assert_called_with(os.remove)
        mock_remove.assert_called_with('target_disk_path')

    def test_walk_class_hierarchy(self):
        class A:
            pass

        class B(A):
            pass

        class C(B):
            pass

        class D(A):
            pass

        result = list(utils.walk_class_hierarchy(A))
        self.assertEqual(result, [C, B, D])

    def test_walk_class_hierarchy_no_subclasses(self):
        class A:
            pass

        result = list(utils.walk_class_hierarchy(A, encountered=None))
        self.assertEqual(result, [])

    @mock.patch('socket.socket')
    @mock.patch('ssl.SSLContext')
    @mock.patch('OpenSSL.crypto')
    def test_get_ssl_cert_thumbprint(self, mock_crypto, mock_ssl_context,
                                     mock_socket):
        mock_socket.return_value = mock.MagicMock()
        mock_ssl_context.return_value = mock.MagicMock()

        result = utils.get_ssl_cert_thumbprint(
            mock_ssl_context.return_value, 'localhost',
            digest_algorithm='sha1')

        self.assertEqual(result, mock_crypto.load_certificate.return_value.
                         digest.return_value.decode.return_value)
        mock_crypto.load_certificate.return_value.digest.\
            assert_called_once_with('sha1')

    @mock.patch('os.path')
    def test_get_resources_dir(self, mock_path):
        mock_path.dirname.return_value = 'dirname'
        mock_path.abspath.return_value = 'dirname/abs_path'

        result = utils.get_resources_dir()

        self.assertEqual(result, mock_path.join.return_value)

    @mock.patch('os.path')
    def test_get_resources_bin_dir(self, mock_path):
        mock_path.dirname.return_value = 'dirname'
        mock_path.abspath.return_value = 'dirname/abs_path '

        result = utils.get_resources_bin_dir()

        self.assertEqual(result, mock_path.join.return_value)

    def test_serialize_key(self):
        mock_key = mock.MagicMock()
        mock_key.write_private_key.return_value = None

        utils.serialize_key(mock_key, password='password')

        args, _ = mock_key.write_private_key.call_args

        self.assertEqual(args[1], 'password')

    @mock.patch('io.StringIO')
    @mock.patch('paramiko.RSAKey')
    def test_deserialize_key(self, mock_rsa_key, mock_string_io):
        mock_string_io.return_value = mock.MagicMock()

        result = utils.deserialize_key('key', password='password')

        mock_rsa_key.from_private_key.assert_called_with(
            mock_string_io.return_value, 'password')
        self.assertEqual(result, mock_rsa_key.from_private_key.return_value)

    @mock.patch('coriolis.utils.jsonutils.dumps')
    @mock.patch('coriolis.utils.jsonutils.loads')
    def test_to_dict(self, mock_loads, mock_dumps):
        mock_dumps.return_value = 'json'
        obj = mock.MagicMock()

        result = utils.to_dict(obj)

        mock_dumps.assert_called_once_with(obj, default=mock.ANY)
        mock_loads.assert_called_once_with(mock_dumps.return_value)

        self.assertEqual(result, mock_loads.return_value)

    def test_to_dict_with_primitive(self):
        obj = datetime.datetime.now()
        result = utils.to_dict(obj)

        self.assertEqual(result, obj.isoformat())

    def test_load_class(self):
        class_path = 'json.JSONDecoder'
        result = utils.load_class(class_path)

        self.assertEqual(result, json.JSONDecoder)

    def test_check_md5_with_matching_hashes(self):
        data = b'test data'
        md5 = hashlib.md5(data).hexdigest()
        utils.check_md5(data, md5)

    def test_check_md5_with_exception(self):
        data = b'test data'
        md5 = hashlib.md5(b'other data').hexdigest()
        self.assertRaises(exception.CoriolisException, utils.check_md5, data,
                          md5)

    @mock.patch.object(utils, 'secrets')
    def test_get_secret_connection_info_with_secret_ref(self, mock_secrets):
        with self.assertLogs('coriolis.utils', level=logging.INFO):
            result = utils.get_secret_connection_info('context',
                                                      {'secret_ref': 'ref'})

        self.assertEqual(result, mock_secrets.get_secret.return_value)

    def test_get_secret_connection_info_with_no_secret_ref(self):
        result = utils.get_secret_connection_info('context', {})
        self.assertEqual(result, {})

    @ddt.data(123, '123')
    def test_parse_int_value_with_valid_input(self, value):
        result = utils.parse_int_value(value)
        self.assertEqual(result, 123)

    @ddt.data('invalid', '123.45', None)
    def test_parse_int_value_with_invalid_input(self, value):
        self.assertRaises(exception.InvalidInput, utils.parse_int_value, value)

    @ddt.data(
        ('SGVsbG8gd29ybGQ=', False, 'Hello world'),
        ('eyJ0ZXN0IjogInZhbHVlIn0=', True, {'test': 'value'}),
    )
    @ddt.unpack
    def test_decode_base64_param(self, value, is_json, expected):
        result = utils.decode_base64_param(value, is_json=is_json)
        self.assertEqual(result, expected)

    @ddt.data(
        ('invalid', False),
        ('invalid', True),
        ('SGVsbG8gd29ybGQ=', True),
    )
    @ddt.unpack
    def test_decode_base64_param_with_invalid_input(self, value, is_json):
        self.assertRaises(exception.InvalidInput, utils.decode_base64_param,
                          value, is_json=is_json)

    def test_quote_url(self):
        result = utils.quote_url('Hello world')
        self.assertEqual(result, 'Hello%20world')

    @ddt.data(
        ('00-11-22-33-44-55', '00:11:22:33:44:55'),
        ('00:11:22:33:44:55', '00:11:22:33:44:55'),
        ('001122334455', '00:11:22:33:44:55'),
        ('00-11-22-AA-BB-CC', '00:11:22:aa:bb:cc'),
    )
    @ddt.unpack
    def test_normalize_mac_address(self, input, expected):
        result = utils.normalize_mac_address(input)
        self.assertEqual(result, expected)

    @ddt.data(
        'invalid',
        '00112233445566',
        '00-11-22-33-44-GG',
        123456789012,
    )
    def test_normalize_mac_address_with_invalid_input(self, input):
        self.assertRaises(ValueError, utils.normalize_mac_address,
                          input)

    def test_get_url_with_credentials(self):
        url = 'http://example.com'
        username = 'user'
        password = 'pass'
        expected = 'http://user:pass@example.com'

        result = utils.get_url_with_credentials(url, username, password)

        self.assertEqual(result, expected)

    def test_get_url_with_credentials_existing_credentials(self):
        url = 'http://olduser:oldpass@example.com'
        username = 'newuser'
        password = 'newpass'
        expected = 'http://newuser:newpass@example.com'

        result = utils.get_url_with_credentials(url, username, password)

        self.assertEqual(result, expected)

    @ddt.data(
        (
            [
                {'id': '1', 'name': 'Resource1'},
                {'id': '2', 'name': 'Resource2'},
                {'id': '3', 'name': 'Resource1'}
            ],
            ['Resource2', '1', '3']
        ),
        (
            [
                {'id': '1', 'name': 'Resource1'},
                {'id': '2', 'name': 'Resource2'},
                {'id': '3', 'name': 'Resource3'}
            ],
            ['Resource1', 'Resource2', 'Resource3']
        ),
        (
            [
                {'id': '1', 'name': 'Resource1'},
                {'id': '2', 'name': 'Resource2'},
                {'id': '3'}
            ],
            KeyError
        )
    )
    @ddt.unpack
    def test_get_unique_option_ids(self, resources, expected):
        if isinstance(expected, list):
            result = utils.get_unique_option_ids(resources)
            self.assertEqual(sorted(result), sorted(expected))
        else:
            self.assertRaises(expected, utils.get_unique_option_ids, resources)

    def test_get_unique_option_ids_with_custom_keys(self):
        resources = [
            {'custom_id': '1', 'custom_name': 'Resource1'},
            {'custom_id': '2', 'custom_name': 'Resource2'},
            {'custom_id': '3', 'custom_name': 'Resource1'}
        ]
        expected_result = ['Resource2', '1', '3']
        result = utils.get_unique_option_ids(
            resources, id_key='custom_id', name_key='custom_name')
        self.assertEqual(sorted(result), sorted(expected_result))

    def test_bad_request_on_error(self):
        @utils.bad_request_on_error("An error occurred: %s")
        def mock_func():
            return (True, "Everything is fine")

        is_valid, message = mock_func()
        self.assertTrue(is_valid)
        self.assertEqual(message, "Everything is fine")

    def test_bad_request_on_error_httpBadRequest(self):
        @utils.bad_request_on_error("An error occurred: %s")
        def mock_func():
            return (False, "Something is wrong")

        self.assertRaises(exc.HTTPBadRequest, mock_func)

    @ddt.data(
        ({
            "key1": "value1",
            "origin": {"connection_info": "sensitive_info"},
            "destination": {"connection_info": "sensitive_info"},
            "volumes_info": [
                {
                    "key2": "value2",
                    "replica_state": {
                        "key3": "value3",
                        "chunks": "sensitive_info"
                    }
                }
            ]
        }, {
            "key1": "value1",
            "origin": {"connection_info": {"got": "redacted"}},
            "destination": {"connection_info": {"got": "redacted"}},
            "volumes_info": [
                {
                    "key2": "value2",
                    "replica_state": {
                        "key3": "value3",
                        "chunks": ["<redacted>"]
                    }
                }
            ]
        }),
        ({
            "key1": "value1",
            "key2": "value2",
        }, {
            "key1": "value1",
            "key2": "value2",
        }),
    )
    @ddt.unpack
    def test_sanitize_task_info(self, task_info, expected):
        result = utils.sanitize_task_info(task_info)
        self.assertEqual(result, expected)
        self.assertIsInstance(result, dict)

    def test_parse_ini_config(self):
        file_contents = 'key1 = value1\nkey2 = value2\nkey3 = value3'
        expected = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
        }

        result = utils.parse_ini_config(file_contents)
        self.assertEqual(result, expected)
        self.assertIsInstance(result, dict)

    @mock.patch('coriolis.utils.test_ssh_path')
    @mock.patch('coriolis.utils.read_ssh_file')
    @mock.patch('coriolis.utils.parse_ini_config')
    def test_read_ssh_ini_config_file_check_false(self, mock_parse_ini_config,
                                                  mock_read_ssh_file,
                                                  mock_test_ssh_path):
        mock_test_ssh_path.return_value = True

        result = utils.read_ssh_ini_config_file(self.mock_ssh, '/test/file',
                                                check_exists=False)

        self.assertEqual(result, mock_parse_ini_config.return_value)
        mock_read_ssh_file.assert_called_once_with(self.mock_ssh, '/test/file')
        mock_parse_ini_config.assert_called_once_with(
            mock_read_ssh_file.return_value.decode())

    @mock.patch('coriolis.utils.test_ssh_path')
    def test_read_ssh_ini_config_file_check_true_path_not_exists(
            self, mock_test_ssh_path):
        mock_test_ssh_path.return_value = False

        result = utils.read_ssh_ini_config_file(self.mock_ssh, '/test/to/file',
                                                check_exists=True)

        self.assertEqual(result, {})
        mock_test_ssh_path.assert_called_once_with(self.mock_ssh,
                                                   '/test/to/file')

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    @mock.patch('coriolis.utils.write_ssh_file')
    @mock.patch('coriolis.utils.test_ssh_path')
    @mock.patch.object(uuid, 'uuid4')
    def test_write_systemd(self, mock_uuid, mock_test_ssh,
                           mock_write_ssh_file, mock_exec_ssh_cmd):
        mock_uuid.return_value = 'uuid'
        mock_test_ssh.side_effect = [True, False]
        mock_write_ssh_file.return_value = None

        utils._write_systemd(self.mock_ssh, 'cmdline', 'svc_name')

        mock_uuid.assert_called_once_with()
        mock_test_ssh.assert_has_calls([
            mock.call(self.mock_ssh, '/lib/systemd/system'),
            mock.call(self.mock_ssh,
                      '/lib/systemd/system/svc_name.service')])
        mock_write_ssh_file.assert_called_once_with(self.mock_ssh,
                                                    '/tmp/uuid.service',
                                                    mock.ANY)
        mock_exec_ssh_cmd.assert_has_calls([
            mock.call(self.mock_ssh, 'sudo mv /tmp/uuid.service '
                      '/lib/systemd/system/svc_name.service', get_pty=True),
            mock.call(self.mock_ssh, 'sudo restorecon -v '
                      '/lib/systemd/system/svc_name.service', get_pty=True),
            mock.call(self.mock_ssh, 'sudo systemctl daemon-reload',
                      get_pty=True),
            mock.call(self.mock_ssh, 'sudo systemctl start svc_name',
                      get_pty=True)])

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    @mock.patch('coriolis.utils.write_ssh_file')
    @mock.patch('coriolis.utils.test_ssh_path')
    @mock.patch.object(uuid, 'uuid4')
    def test_write_systemd_usr_lib(self, mock_uuid, mock_test_ssh,
                                   mock_write_ssh_file, mock_exec_ssh_cmd):
        mock_uuid.return_value = 'uuid'
        mock_test_ssh.side_effect = [False, False]
        mock_write_ssh_file.return_value = None

        utils._write_systemd(self.mock_ssh, 'cmdline', 'svc_name')

        mock_test_ssh.assert_has_calls([
            mock.call(self.mock_ssh, '/lib/systemd/system'),
            mock.call(self.mock_ssh,
                      '/usr/lib/systemd/system/svc_name.service')])
        mock_exec_ssh_cmd.assert_has_calls([
            mock.call(self.mock_ssh, 'sudo mv /tmp/uuid.service '
                      '/usr/lib/systemd/system/svc_name.service',
                      get_pty=True)])

    @mock.patch('coriolis.utils.test_ssh_path')
    def test_write_systemd_service_exists(self, mock_test_ssh):
        mock_test_ssh.return_value = True

        utils._write_systemd(self.mock_ssh, 'cmdline', 'svc_name')

        mock_test_ssh.assert_has_calls([
            mock.call(self.mock_ssh, '/lib/systemd/system'),
            mock.call(self.mock_ssh,
                      '/lib/systemd/system/svc_name.service')])

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    @mock.patch('coriolis.utils.write_ssh_file')
    @mock.patch('coriolis.utils.test_ssh_path')
    @mock.patch.object(uuid, 'uuid4')
    def test_write_systemd_service_selinux_exception(self, mock_uuid,
                                                     mock_test_ssh,
                                                     mock_write_ssh_file,
                                                     mock_exec_ssh_cmd):
        mock_uuid.return_value = 'uuid'
        mock_test_ssh.side_effect = [True, False]
        mock_write_ssh_file.return_value = None
        mock_exec_ssh_cmd.side_effect = [
            None, exception.CoriolisException(), None, None]

        _write_systemd_undecorated = testutils.get_wrapped_function(
            utils._write_systemd)

        with self.assertLogs('coriolis.utils', level=logging.WARN):
            _write_systemd_undecorated(self.mock_ssh, '/test/file', 'svc_name',
                                       start=True)

        mock_uuid.assert_called_once_with()
        mock_test_ssh.assert_has_calls([
            mock.call(self.mock_ssh, '/lib/systemd/system'),
            mock.call(self.mock_ssh,
                      '/lib/systemd/system/svc_name.service')])
        mock_write_ssh_file.assert_called_once_with(self.mock_ssh,
                                                    '/tmp/uuid.service',
                                                    mock.ANY)

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    @mock.patch('coriolis.utils.write_ssh_file')
    @mock.patch('coriolis.utils.test_ssh_path')
    @mock.patch.object(uuid, 'uuid4')
    def test_test_write_systemd_with_run_as(self, mock_uuid, mock_test_ssh,
                                            mock_write_ssh_file,
                                            mock_exec_ssh_cmd):

        mock_uuid.return_value = 'uuid'
        mock_test_ssh.side_effect = [True, False]

        utils._write_systemd(self.mock_ssh, 'cmdline', 'svc_name',
                             run_as='test_user')

        mock_uuid.assert_called_once_with()
        mock_test_ssh.assert_has_calls([
            mock.call(self.mock_ssh, '/lib/systemd/system'),
            mock.call(self.mock_ssh,
                      '/lib/systemd/system/svc_name.service')])
        mock_write_ssh_file.assert_called_once_with(
            self.mock_ssh, '/tmp/uuid.service',
            utils.SYSTEMD_TEMPLATE % {
                "cmdline": 'cmdline',
                "username": 'test_user',
                "svc_name": 'svc_name'})

        mock_exec_ssh_cmd.assert_has_calls([
            mock.call(self.mock_ssh, 'sudo mv /tmp/uuid.service '
                      '/lib/systemd/system/svc_name.service', get_pty=True),
            mock.call(self.mock_ssh, 'sudo restorecon -v '
                      '/lib/systemd/system/svc_name.service', get_pty=True),
            mock.call(self.mock_ssh, 'sudo systemctl daemon-reload',
                      get_pty=True),
            mock.call(self.mock_ssh, 'sudo systemctl start svc_name',
                      get_pty=True)])

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    @mock.patch('coriolis.utils.write_ssh_file')
    @mock.patch('coriolis.utils.test_ssh_path')
    @mock.patch.object(uuid, 'uuid4')
    def test_write_upstart(self, mock_uuid, mock_test_ssh,
                           mock_write_ssh_file, mock_exec_ssh_cmd):
        mock_uuid.return_value = 'uuid'
        mock_test_ssh.return_value = False
        mock_write_ssh_file.return_value = None

        utils._write_upstart(self.mock_ssh, 'cmdline', 'svc_name')

        mock_uuid.assert_called_once_with()
        mock_test_ssh.assert_called_once_with(
            self.mock_ssh, '/etc/init/svc_name.conf')
        mock_write_ssh_file.assert_called_once_with(self.mock_ssh,
                                                    '/tmp/uuid.conf',
                                                    mock.ANY)
        mock_exec_ssh_cmd.assert_has_calls([
            mock.call(self.mock_ssh, 'sudo mv /tmp/uuid.conf '
                      '/etc/init/svc_name.conf', get_pty=True),
            mock.call(self.mock_ssh, 'start svc_name')])

    @mock.patch('coriolis.utils.test_ssh_path')
    def test_write_upstart_service_exists(self, mock_test_ssh):
        mock_test_ssh.return_value = True

        utils._write_upstart(self.mock_ssh, 'cmdline', 'svc_name')

        mock_test_ssh.assert_called_once_with(
            self.mock_ssh, '/etc/init/svc_name.conf')

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    @mock.patch('coriolis.utils.write_ssh_file')
    @mock.patch('coriolis.utils.test_ssh_path')
    @mock.patch.object(uuid, 'uuid4')
    def test_write_upstart_with_run_as(self, mock_uuid, mock_test_ssh,
                                       mock_write_ssh_file,
                                       mock_exec_ssh_cmd):
        mock_uuid.return_value = 'uuid'
        mock_test_ssh.return_value = False

        utils._write_upstart(self.mock_ssh, 'cmdline', 'svc_name',
                             run_as='test-user')

        mock_uuid.assert_called_once_with()
        mock_test_ssh.assert_called_once_with(
            self.mock_ssh, '/etc/init/svc_name.conf')
        mock_write_ssh_file.assert_called_once_with(
            self.mock_ssh, '/tmp/uuid.conf',
            utils.UPSTART_TEMPLATE % {
                "cmdline": 'sudo -u test-user -- cmdline',
                "svc_name": 'svc_name'})

        mock_exec_ssh_cmd.assert_has_calls([
            mock.call(self.mock_ssh, 'sudo mv /tmp/uuid.conf '
                      '/etc/init/svc_name.conf', get_pty=True),
            mock.call(self.mock_ssh, 'start svc_name')])

    @mock.patch('coriolis.utils._write_systemd')
    @mock.patch('coriolis.utils.test_ssh_path')
    def test_create_service_systemd(self, mock_test_ssh, mock_write_systemd):
        mock_test_ssh.return_value = True

        utils.create_service(self.mock_ssh, 'cmdline', 'svc_name',
                             run_as='user', start=True)

        mock_write_systemd.assert_called_once_with(self.mock_ssh, 'cmdline',
                                                   'svc_name', run_as='user',
                                                   start=True)

    @mock.patch('coriolis.utils._write_upstart')
    @mock.patch('coriolis.utils.test_ssh_path')
    def test_create_service_upstart(self, mock_test_ssh, mock_write_upstart):
        mock_test_ssh.side_effect = [False, False, True]

        utils.create_service(self.mock_ssh, 'cmdline', 'svc_name',
                             run_as='user', start=True)

        mock_write_upstart.assert_called_once_with(self.mock_ssh, 'cmdline',
                                                   'svc_name', run_as='user',
                                                   start=True)

    @mock.patch('coriolis.utils._write_systemd')
    @mock.patch('coriolis.utils.test_ssh_path')
    def test_create_service_exception(self, mock_test_ssh, mock_write_systemd):
        mock_test_ssh.return_value = False

        create_svc_undecorated = testutils.get_wrapped_function(
            utils.create_service)

        self.assertRaises(exception.CoriolisException, create_svc_undecorated,
                          self.mock_ssh, 'cmdline', 'svc_name', run_as='user',
                          start=True)

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    @mock.patch('coriolis.utils.test_ssh_path')
    def test_restart_service_with_systemd(self, mock_test_ssh,
                                          mock_exec_ssh_cmd):
        mock_test_ssh.return_value = True

        utils.restart_service(self.mock_ssh, 'svc_name')

        mock_test_ssh.assert_called_once_with(self.mock_ssh,
                                              '/lib/systemd/system')
        mock_exec_ssh_cmd.assert_called_once_with(
            self.mock_ssh, 'sudo systemctl restart svc_name', get_pty=True)

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    @mock.patch('coriolis.utils.test_ssh_path')
    def test_restart_service_with_upstart(self, mock_test_ssh,
                                          mock_exec_ssh_cmd):
        mock_test_ssh.side_effect = [False, False, True]

        utils.restart_service(self.mock_ssh, 'svc_name')

        mock_exec_ssh_cmd.assert_called_once_with(
            self.mock_ssh, 'restart svc_name')

    @mock.patch('coriolis.utils.test_ssh_path')
    def test_restart_service_exception(self, mock_test_ssh):
        mock_test_ssh.return_value = False

        restart_svc_undecorated = testutils.get_wrapped_function(
            utils.restart_service)

        self.assertRaises(exception.UnrecognizedWorkerInitSystem,
                          restart_svc_undecorated, self.mock_ssh, 'svc_name')

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    @mock.patch('coriolis.utils.test_ssh_path')
    def test_start_service_with_systemd(self, mock_test_ssh,
                                        mock_exec_ssh_cmd):
        mock_test_ssh.return_value = True

        utils.start_service(self.mock_ssh, 'svc_name')

        mock_test_ssh.assert_called_once_with(self.mock_ssh,
                                              '/lib/systemd/system')
        mock_exec_ssh_cmd.assert_called_once_with(
            self.mock_ssh, 'sudo systemctl start svc_name', get_pty=True)

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    @mock.patch('coriolis.utils.test_ssh_path')
    def test_start_service_with_upstart(self, mock_test_ssh,
                                        mock_exec_ssh_cmd):
        mock_test_ssh.side_effect = [False, False, True]

        utils.start_service(self.mock_ssh, 'svc_name')

        mock_exec_ssh_cmd.assert_called_once_with(
            self.mock_ssh, 'start svc_name')

    @mock.patch('coriolis.utils.test_ssh_path')
    def test_start_service_exception(self, mock_test_ssh):
        mock_test_ssh.return_value = False

        start_svc_undecorated = testutils.get_wrapped_function(
            utils.start_service)

        self.assertRaises(exception.UnrecognizedWorkerInitSystem,
                          start_svc_undecorated, self.mock_ssh, 'svc_name')

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    @mock.patch('coriolis.utils.test_ssh_path')
    def test_stop_service_with_systemd(self, mock_test_ssh,
                                       mock_exec_ssh_cmd):
        mock_test_ssh.return_value = True

        utils.stop_service(self.mock_ssh, 'svc_name')

        mock_test_ssh.assert_called_once_with(self.mock_ssh,
                                              '/lib/systemd/system')
        mock_exec_ssh_cmd.assert_called_once_with(
            self.mock_ssh, 'sudo systemctl stop svc_name', get_pty=True)

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    @mock.patch('coriolis.utils.test_ssh_path')
    def test_stop_service_with_upstart(self, mock_test_ssh,
                                       mock_exec_ssh_cmd):
        mock_test_ssh.side_effect = [False, False, True]

        utils.stop_service(self.mock_ssh, 'svc_name')

        mock_exec_ssh_cmd.assert_called_once_with(
            self.mock_ssh, 'stop svc_name')

    @mock.patch('coriolis.utils.test_ssh_path')
    def test_stop_service_exception(self, mock_test_ssh):
        mock_test_ssh.return_value = False

        stop_svc_undecorated = testutils.get_wrapped_function(
            utils.stop_service)

        self.assertRaises(exception.UnrecognizedWorkerInitSystem,
                          stop_svc_undecorated, self.mock_ssh, 'svc_name')


@ddt.ddt
class Grub2ConfigEditorTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Grub2ConfigEditor class."""

    def setUp(self):
        super(Grub2ConfigEditorTestCase, self).setUp()
        self.cfg = "test configuration"
        self.parser = utils.Grub2ConfigEditor(self.cfg)

    def test__init__(self):
        result = utils.Grub2ConfigEditor(self.cfg)
        self.assertEqual(
            result._parsed, [{'type': 'raw', 'payload': self.cfg}])

    def test_parse_cfg_comment_line(self):
        self.cfg = '# This is a comment'

        result = self.parser._parse_cfg(self.cfg)
        self.assertEqual(result, [{'type': 'raw', 'payload': self.cfg}])

    def test_parse_cfg_option_line_with_quoted_value(self):
        self.cfg = 'option="value"'
        expected_result = [{'type': 'option', 'payload': self.cfg,
                            'quoted': True, 'option_name': 'option',
                            'option_value':
                            [{'opt_type': 'single', 'opt_val': 'value'}]}]

        result = self.parser._parse_cfg(self.cfg)
        self.assertEqual(result, expected_result)

    def test_parse_cfg_option_line_without_value(self):
        self.cfg = 'option='
        expected_result = [{'type': 'option', 'payload': self.cfg,
                            'quoted': False, 'option_name': 'option',
                            'option_value':
                            [{'opt_type': 'single', 'opt_val': ''}]}]

        result = self.parser._parse_cfg(self.cfg)
        self.assertEqual(result, expected_result)

    def test_parse_cfg_option_line_with_value(self):
        self.cfg = 'option=value'
        expected_result = [{'type': 'option', 'payload': self.cfg,
                            'quoted': False, 'option_name': 'option',
                            'option_value':
                            [{'opt_type': 'single', 'opt_val': 'value'}]}]

        result = self.parser._parse_cfg(self.cfg)
        self.assertEqual(result, expected_result)

    def test_parse_cfg_option_line_with_multiple_values(self):
        self.cfg = 'option=value1 value2'
        expected_result = [{'type': 'option', 'payload': self.cfg,
                            'quoted': False, 'option_name': 'option',
                            'option_value':
                            [{'opt_type': 'single', 'opt_val': 'value1'},
                             {'opt_type': 'single', 'opt_val': 'value2'}]}]

        result = self.parser._parse_cfg(self.cfg)
        self.assertEqual(result, expected_result)

    def test_parse_cfg_option_line_with_key_value(self):
        self.cfg = 'option=key=value'
        expected_result = [{'type': 'option', 'payload': self.cfg,
                            'quoted': False, 'option_name': 'option',
                            'option_value':
                            [{'opt_type': 'key_val', 'opt_val': 'value',
                              'opt_key': 'key'}]}]

        result = self.parser._parse_cfg(self.cfg)
        self.assertEqual(result, expected_result)

    @ddt.data(
        ("not a dict", ValueError),
        ({"opt_type": "invalid"}, ValueError),
        ({"opt_type": "key_val"}, ValueError),
        ({"opt_type": "single"}, ValueError),
        ({"unknown_opt_type"}, ValueError),
        ({"opt_type": "key_val", "opt_key": "key", "opt_val": "val"}, None),
        ({"opt_type": "single", "opt_val": "val"}, None),
    )
    @ddt.unpack
    def test_validate_value(self, value, expected):
        if expected:
            self.assertRaises(expected, self.parser._validate_value, value)
        else:
            self.parser._validate_value(value)

    def test_set_option_updates_existing_option(self):
        self.parser._parsed = [{"option_name": "existing_option",
                                "option_value": ["old_value"]}]
        new_value = {"opt_type": "key_val", "opt_key": "key", "opt_val":
                     "new_value"}
        expected_value = [{"option_name": "existing_option",
                           "option_value": [new_value]}]

        self.parser.set_option("existing_option", new_value)
        self.assertEqual(self.parser._parsed, expected_value)

    def test_set_option_adds_new_option(self):
        self.parser._parsed = [{"option_name": "existing_option",
                                "option_value": ["old_value"]}]
        new_value = {"opt_type": "key_val", "opt_key": "key",
                     "opt_val": "new_value", "quoted": True, "type": "option"}
        expected_value = [{"option_name": "existing_option", "option_value":
                           ["old_value"]}, {"option_name": "new_option",
                                            "option_value": [new_value],
                                            "quoted": True, "type": "option"}]

        self.parser.set_option("new_option", new_value)
        self.assertEqual(self.parser._parsed, expected_value)

    def test_append_to_option_updates_existing_key_val_option(self):
        self.parser._parsed = [{"option_name": "existing_option",
                                "option_value": [{"opt_type": "key_val",
                                                  "opt_key": "key",
                                                  "opt_val": "old_value"}]}]
        new_value = {"opt_type": "key_val", "opt_key": "key",
                     "opt_val": "new_value"}
        expected_value = [{"option_name": "existing_option", "option_value":
                           [new_value]}]

        self.parser.append_to_option("existing_option", new_value)
        self.assertEqual(self.parser._parsed, expected_value)

    def test_append_to_option_ignores_existing_single_option(self):
        self.parser._parsed = [{"option_name": "existing_option",
                                "option_value": [{"opt_type": "single",
                                                  "opt_val": "old_value"}]}]
        new_value = {"opt_type": "single", "opt_val": "old_value"}
        expected_value = [{"option_name": "existing_option",
                           "option_value": [new_value]}]

        self.parser.append_to_option("existing_option", new_value)
        self.assertEqual(self.parser._parsed, expected_value)

    def test_append_to_option_adds_new_single_option(self):
        self.parser._parsed = [{"option_name": "existing_option",
                                "option_value": [{"opt_type": "single",
                                                  "opt_val": "old_value"}]}]
        new_value = {"opt_type": "single", "opt_val": "new_value"}
        expected_value = [{
            "option_name": "existing_option", "option_value":
            [{"opt_type": "single", "opt_val": "old_value"}, new_value]}]

        self.parser.append_to_option("existing_option", new_value)
        self.assertEqual(self.parser._parsed, expected_value)

    def test_append_to_option_adds_new_option(self):
        self.parser._parsed = [{"option_name": "existing_option",
                                "option_value": [{"opt_type": "single",
                                                  "opt_val": "old_value"}]}]
        new_value = {"opt_type": "key_val", "opt_key": "key", "opt_val":
                     "new_value"}
        expected_value = [{
            "option_name": "existing_option", "option_value":
            [{"opt_type": "single", "opt_val": "old_value"}]},
            {"option_name": "new_option", "option_value": [new_value],
             "quoted": True, "type": "option"}]

        self.parser.append_to_option("new_option", new_value)
        self.assertEqual(self.parser._parsed, expected_value)

    @ddt.data(
        (
            [{"type": "raw", "payload": "raw_data"}],
            "raw_data\n"
        ),
        (
            [{"type": "option", "option_name": "option1", "option_value":
              [{"opt_type": "single", "opt_val": "value1"}], "quoted": False}],
            "option1=value1\n"
        ),
        (
            [{"type": "option", "option_name": "option2", "option_value":
              [{"opt_type": "key_val", "opt_key": "key2",
                "opt_val": "value2"}],
                "quoted": True}], "option2=\"key2=value2\"\n"
        ),
        (
            [{"type": "option", "option_name": "option3", "option_value": [],
              "quoted": False}], "option3=\n"
        ),
        (
            [{"type": "option", "option_name": "option4", "option_value":
              [{"opt_type": "single", "opt_val": "value4_1"},
               {"opt_type": "single", "opt_val":
                "value4_2"}], "quoted": False}],
            "option4=\"value4_1 value4_2\"\n"
        ))
    @ddt.unpack
    def test_dump(self, parsed, expected_output):
        self.parser._parsed = parsed
        self.assertEqual(self.parser.dump(), expected_output)
