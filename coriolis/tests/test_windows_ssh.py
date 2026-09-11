# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
import socket
from unittest import mock

import paramiko

from coriolis import exception, windows_ssh
from coriolis.tests import test_base


def _fake_exec_channel(stdout=b"std_out", stderr=b"std_err", exit_code=0):
    """SSH channel that yields stdout/stderr once, then reports exit."""
    out = [stdout]
    err = [stderr]
    channel = mock.Mock()

    def recv_ready():
        return bool(out[0])

    def recv_stderr_ready():
        return bool(err[0])

    def recv(_size):
        data = out[0]
        out[0] = b""
        return data

    def recv_stderr(_size):
        data = err[0]
        err[0] = b""
        return data

    channel.recv_ready.side_effect = recv_ready
    channel.recv_stderr_ready.side_effect = recv_stderr_ready
    channel.recv.side_effect = recv
    channel.recv_stderr.side_effect = recv_stderr
    channel.exit_status_ready.return_value = True
    channel.recv_exit_status.return_value = exit_code
    return channel


class WindowsSSHConnectionTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for WindowsSSHConnection."""

    def setUp(self):
        super(WindowsSSHConnectionTestCase, self).setUp()
        self.conn = windows_ssh.WindowsSSHConnection()
        self.conn._ssh = mock.Mock()
        self.cmd = "test_cmd"
        self.args = ["-RecoveryPassword", "'ShouldNotBeLogged'"]
        self.url = "http://example.com/file"
        self.remote_path = "/remote/path"

    def test__init__timeout(self):
        connection = windows_ssh.WindowsSSHConnection()
        self.assertEqual(connection._conn_timeout, windows_ssh.DEFAULT_TIMEOUT)

    def test__init__timeout_set(self):
        connection = windows_ssh.WindowsSSHConnection(timeout=100)
        self.assertEqual(connection._conn_timeout, 100)

    @mock.patch.object(windows_ssh.WindowsSSHConnection, "_start_ps_session")
    @mock.patch.object(windows_ssh.utils, "connect_ssh")
    @mock.patch.object(windows_ssh.utils, "wait_for_port_connectivity")
    def test_from_connection_info_uses_ssh(
        self, mock_wait_for_port, mock_connect_ssh, mock_start_ps
    ):
        mock_ssh = mock.Mock()
        mock_connect_ssh.return_value = mock_ssh
        connection_info = {
            "ip": "127.0.0.1",
            "username": "user",
            "password": "pass",
            "port": 22,
        }

        result = windows_ssh.WindowsSSHConnection.from_connection_info(connection_info)

        mock_wait_for_port.assert_called_once_with("127.0.0.1", 22)
        mock_connect_ssh.assert_called_once_with(
            hostname="127.0.0.1",
            port=22,
            username="user",
            password="pass",
            pkey=None,
        )
        mock_start_ps.assert_called_once_with()
        self.assertIsInstance(result, windows_ssh.WindowsSSHConnection)
        self.assertIs(result._ssh, mock_ssh)

    @mock.patch.object(windows_ssh.WindowsSSHConnection, "_start_ps_session")
    @mock.patch.object(windows_ssh.utils, "connect_ssh")
    @mock.patch.object(windows_ssh.utils, "wait_for_port_connectivity")
    def test_from_connection_info_omitted_port_defaults_to_ssh(
        self, mock_wait_for_port, mock_connect_ssh, mock_start_ps
    ):
        mock_connect_ssh.return_value = mock.Mock()
        connection_info = {
            "ip": "127.0.0.1",
            "username": "user",
            "password": "pass",
        }

        windows_ssh.WindowsSSHConnection.from_connection_info(connection_info)

        mock_wait_for_port.assert_called_once_with("127.0.0.1", 22)
        mock_connect_ssh.assert_called_once_with(
            hostname="127.0.0.1",
            port=22,
            username="user",
            password="pass",
            pkey=None,
        )
        mock_start_ps.assert_called_once_with()

    @mock.patch.object(windows_ssh.WindowsSSHConnection, "_start_ps_session")
    @mock.patch.object(windows_ssh.utils, "connect_ssh")
    @mock.patch.object(windows_ssh.utils, "wait_for_port_connectivity")
    def test_from_connection_info_keeps_custom_ssh_port(
        self, mock_wait_for_port, mock_connect_ssh, mock_start_ps
    ):
        mock_connect_ssh.return_value = mock.Mock()
        connection_info = {
            "ip": "127.0.0.1",
            "username": "user",
            "password": "pass",
            "port": 2222,
        }

        windows_ssh.WindowsSSHConnection.from_connection_info(connection_info)

        mock_wait_for_port.assert_called_once_with("127.0.0.1", 2222)
        mock_connect_ssh.assert_called_once_with(
            hostname="127.0.0.1",
            port=2222,
            username="user",
            password="pass",
            pkey=None,
        )
        mock_start_ps.assert_called_once_with()

    def test_from_connection_info_missing_keys(self):
        self.assertRaises(
            ValueError,
            windows_ssh.WindowsSSHConnection.from_connection_info,
            {"username": "user", "password": "pass"},
        )

    def test_from_connection_info_invalid_type(self):
        self.assertRaises(
            ValueError,
            windows_ssh.WindowsSSHConnection.from_connection_info,
            "invalid-connection-type",
        )

    def test_from_connection_info_missing_auth(self):
        self.assertRaises(
            ValueError,
            windows_ssh.WindowsSSHConnection.from_connection_info,
            {"ip": "127.0.0.1", "username": "user"},
        )

    def test_disconnect(self):
        ssh = self.conn._ssh
        self.conn._close_ps_session = mock.Mock()
        self.conn.disconnect()
        self.conn._close_ps_session.assert_called_once_with()
        ssh.close.assert_called_once_with()
        self.assertIsNone(self.conn._ssh)

    def test_set_timeout(self):
        self.conn.set_timeout(50)
        self.assertEqual(self.conn._conn_timeout, 50)

    @mock.patch.object(windows_ssh.utils, "connect_ssh")
    def test_connect_invalid_credentials(self, mock_connect_ssh):
        auth_error = paramiko.AuthenticationException("bad auth")
        connect_error = exception.CoriolisException(
            "Failed to setup SSH client: bad auth"
        )
        connect_error.__cause__ = auth_error
        mock_connect_ssh.side_effect = connect_error
        conn = windows_ssh.WindowsSSHConnection()
        self.assertRaises(
            exception.NotAuthorized,
            conn.connect,
            host="127.0.0.1",
            port=22,
            username="user",
            password="pass",
        )

    def test__exec_command(self):
        stdout = mock.Mock()
        stdout.channel = _fake_exec_channel()
        self.conn._ssh.exec_command.return_value = (None, stdout, mock.Mock())

        std_out, std_err, exit_code = self.conn._exec_command(self.cmd, self.args)

        self.assertEqual(std_out, "std_out")
        self.assertEqual(std_err, "std_err")
        self.assertEqual(exit_code, 0)
        self.conn._ssh.exec_command.assert_called_once_with(
            "cmd.exe /c \"test_cmd -RecoveryPassword 'ShouldNotBeLogged'\"",
            timeout=float(self.conn._conn_timeout),
        )

    def test__exec_command_reads_stderr_progress_without_stdout(self):
        stdout = mock.Mock()
        stdout.channel = _fake_exec_channel(stdout=b"", stderr=b"progress" * 100)
        self.conn._ssh.exec_command.return_value = (None, stdout, mock.Mock())

        std_out, std_err, exit_code = self.conn._exec_command("dism.exe", [])

        self.assertEqual(std_out, "")
        self.assertIn("progress", std_err)
        self.assertEqual(exit_code, 0)

    def test__exec_command_timeout(self):
        self.conn._ssh.exec_command.side_effect = socket.timeout
        self.assertRaises(
            exception.OSMorphingSSHOperationTimeout,
            self.conn._exec_command,
            self.cmd,
            self.args,
        )

    def test_exec_command(self):
        stdout = mock.Mock()
        stdout.channel = _fake_exec_channel()
        self.conn._ssh.exec_command.return_value = (None, stdout, mock.Mock())

        sanitized_cmd = "cmd.exe /c \"test_cmd -RecoveryPassword '***'\""
        exp_log = "DEBUG:coriolis.windows_ssh:Executing Windows SSH command: %s" % (
            sanitized_cmd
        )
        with self.assertLogs("coriolis.windows_ssh", level=logging.DEBUG) as log_cm:
            std_out = self.conn.exec_command(self.cmd, self.args)
            self.assertEqual(std_out, "std_out")
        self.assertIn(exp_log, log_cm.output)

    def test_exec_command_exception(self):
        stdout = mock.Mock()
        stdout.channel = _fake_exec_channel(exit_code=1)
        self.conn._ssh.exec_command.return_value = (None, stdout, mock.Mock())
        self.assertRaises(
            exception.CoriolisException, self.conn.exec_command, self.cmd, self.args
        )

    def test_is_reg_exe(self):
        self.assertTrue(windows_ssh._is_reg_exe("reg.exe"))
        self.assertTrue(windows_ssh._is_reg_exe("C:\\Windows\\System32\\reg.exe"))
        self.assertFalse(windows_ssh._is_reg_exe("dism.exe"))

    def test_exec_command_closes_ps_session_for_reg(self):
        self.conn._close_ps_session = mock.Mock()
        stdout = mock.Mock()
        stdout.channel = _fake_exec_channel()
        self.conn._ssh.exec_command.return_value = (None, stdout, mock.Mock())
        self.conn.exec_command("reg.exe", ["unload", "HKLM\\x"])
        self.conn._close_ps_session.assert_called_once_with(wait=True)

    def test_exec_command_keeps_ps_session_for_other_cmds(self):
        self.conn._close_ps_session = mock.Mock()
        stdout = mock.Mock()
        stdout.channel = _fake_exec_channel()
        self.conn._ssh.exec_command.return_value = (None, stdout, mock.Mock())
        self.conn.exec_command("dism.exe", ["/Get-WimInfo"])
        self.conn._close_ps_session.assert_not_called()

    def test_exec_ps_command(self):
        self.conn._invoke_persistent_ps = mock.Mock()
        self.conn._invoke_persistent_ps.return_value = ("std_out\r\n", "", 0)
        result = self.conn.exec_ps_command(self.cmd, include_stderr=False)
        self.conn._invoke_persistent_ps.assert_called_once_with(self.cmd, timeout=None)
        self.assertEqual(result, "std_out")

    def test_exec_ps_command_with_stderr(self):
        self.conn._invoke_persistent_ps = mock.Mock()
        self.conn._invoke_persistent_ps.return_value = ("std_out\n", "stderr", 0)
        result = self.conn.exec_ps_command(self.cmd, include_stderr=True)
        self.assertEqual(result, ("std_out", "stderr"))

    def test_exec_ps_command_nonzero_exit(self):
        self.conn._invoke_persistent_ps = mock.Mock()
        self.conn._invoke_persistent_ps.return_value = ("std_out", "stderr", 1)
        self.assertRaises(
            exception.CoriolisException, self.conn.exec_ps_command, self.cmd
        )

    def test_test_path(self):
        self.conn.exec_ps_command = mock.Mock()
        self.conn.exec_ps_command.return_value = "True"
        result = self.conn.test_path("test_path")
        self.conn.exec_ps_command.assert_called_once_with('Test-Path -Path "test_path"')
        self.assertTrue(result)

    def test_download_file(self):
        self.conn.exec_ps_command = mock.Mock()
        self.conn.download_file(self.url, self.remote_path)
        self.conn.exec_ps_command.assert_called_once()

    def test_download_file_exception(self):
        self.conn.exec_ps_command = mock.Mock()
        self.conn.exec_ps_command.side_effect = exception.CoriolisException
        self.assertRaises(
            exception.CoriolisException,
            self.conn.download_file,
            self.url,
            self.remote_path,
        )

    def test_write_file(self):
        self.conn.exec_ps_command = mock.Mock()
        self.conn.write_file(self.remote_path, b"file content")
        self.conn.exec_ps_command.assert_called_once_with(
            "[IO.File]::WriteAllBytes('%s', [Convert]::FromBase64String('%s'))"
            % (self.remote_path, "ZmlsZSBjb250ZW50"),
            ignore_stdout=True,
        )

    def test_split_on_marker_line(self):
        buf = b"True\r\nCORIOLIS_PS_DONE_abc:0\r\nleftover"
        parsed = windows_ssh._split_on_marker_line(buf, "CORIOLIS_PS_DONE_abc")
        self.assertEqual(parsed[0], b"True\r\n")
        self.assertEqual(parsed[1], "CORIOLIS_PS_DONE_abc:0")
        self.assertEqual(parsed[2], b"leftover")

    def test_split_on_marker_line_incomplete(self):
        buf = b"True\r\nCORIOLIS_PS_DONE_abc:0"
        self.assertIsNone(
            windows_ssh._split_on_marker_line(buf, "CORIOLIS_PS_DONE_abc")
        )

    def test_strip_ps_output_trims_blank_lines(self):
        self.assertEqual(windows_ssh._strip_ps_output("\r\nG\r\n"), "G")
        self.assertEqual(windows_ssh._strip_ps_output("True\r\n"), "True")
        self.assertEqual(windows_ssh._strip_ps_output("\n1\n\n2\n"), "1\r\n2")

    def test_ps_session_is_alive(self):
        channel = mock.Mock()
        channel.closed = False
        channel.exit_status_ready.return_value = False
        transport = mock.Mock()
        transport.is_active.return_value = True
        self.conn._ssh.get_transport.return_value = transport
        self.conn._ps_channel = channel
        self.assertTrue(self.conn._ps_session_is_alive())

    def test_ps_session_is_not_alive_when_channel_closed(self):
        channel = mock.Mock()
        channel.closed = True
        channel.exit_status_ready.return_value = False
        transport = mock.Mock()
        transport.is_active.return_value = True
        self.conn._ssh.get_transport.return_value = transport
        self.conn._ps_channel = channel
        self.assertFalse(self.conn._ps_session_is_alive())

    def test_ps_session_is_not_alive_when_ssh_dead(self):
        self.conn._ssh.get_transport.return_value = None
        self.conn._ps_channel = mock.Mock()
        self.assertFalse(self.conn._ps_session_is_alive())

    def test_ensure_ps_session_restarts_dead_session(self):
        self.conn._ps_session_is_alive = mock.Mock(return_value=False)
        self.conn._restart_ps_session = mock.Mock()
        self.conn._ensure_ps_session()
        self.conn._restart_ps_session.assert_called_once_with()

    def test_ensure_ps_session_restarts_after_failed_alive_check(self):
        self.conn._ps_session_is_alive = mock.Mock(return_value=True)
        self.conn._should_probe_alive = mock.Mock(return_value=True)
        self.conn._ping_ps_session = mock.Mock(return_value=False)
        self.conn._restart_ps_session = mock.Mock()
        self.conn._ensure_ps_session()
        self.conn._ping_ps_session.assert_called_once_with()
        self.conn._restart_ps_session.assert_called_once_with()

    def test_ensure_ps_session_skips_restart_when_alive(self):
        self.conn._ps_session_is_alive = mock.Mock(return_value=True)
        self.conn._should_probe_alive = mock.Mock(return_value=False)
        self.conn._restart_ps_session = mock.Mock()
        self.conn._ensure_ps_session()
        self.conn._restart_ps_session.assert_not_called()

    @mock.patch.object(windows_ssh.time, "monotonic")
    def test_should_probe_alive_after_idle_interval(self, mock_monotonic):
        mock_monotonic.return_value = 100.0
        self.conn._ps_last_alive = 100.0 - windows_ssh.PS_ALIVE_CHECK_INTERVAL
        self.assertTrue(self.conn._should_probe_alive())
        self.conn._ps_last_alive = 90.0
        self.assertFalse(self.conn._should_probe_alive())

    def test_build_ps_wrapper_uses_encoded_command(self):
        wrapper = self.conn._build_ps_wrapper("Test-Path", "abc")
        self.assertIn("CORIOLIS_PS_DONE_abc", wrapper)
        self.assertIn("Invoke-Expression", wrapper)
        self.assertIn("Out-String", wrapper)
        self.assertIn("$ProgressPreference = 'SilentlyContinue'", wrapper)
        self.assertIn("VABlAHMAdAAtAFAAYQB0AGgA", wrapper)

    def test_start_ps_session_waits_for_ready(self):
        stdin = mock.Mock()
        stdout = mock.Mock()
        stdout.channel = mock.Mock()
        self.conn._ssh.exec_command.return_value = (stdin, stdout, mock.Mock())
        self.conn._read_until_marker = mock.Mock(
            return_value=("", "", "CORIOLIS_PS_READY")
        )
        self.conn._host = "10.0.0.1"
        self.conn._port = 22

        self.conn._start_ps_session()

        self.conn._ssh.exec_command.assert_called_once_with(
            windows_ssh._PS_START_COMMAND,
            timeout=float(self.conn._conn_timeout),
        )
        stdin.write.assert_called()
        self.conn._read_until_marker.assert_called_once_with(
            windows_ssh._PS_READY_MARKER, timeout=windows_ssh.PS_START_TIMEOUT
        )
        self.assertIs(self.conn._ps_stdin, stdin)
        self.assertIs(self.conn._ps_channel, stdout.channel)

    def test_read_until_marker(self):
        channel = mock.Mock()
        channel.recv_stderr_ready.return_value = False
        channel.exit_status_ready.return_value = False
        channel.recv_ready.side_effect = [True, False]
        channel.recv.side_effect = [b"True\r\nCORIOLIS_PS_DONE_abc:0\r\n"]
        self.conn._ps_channel = channel
        self.conn._ps_stdout_buf = bytearray()

        stdout, stderr, line = self.conn._read_until_marker(
            "CORIOLIS_PS_DONE_abc", timeout=5
        )

        self.assertEqual(stdout, "True\r\n")
        self.assertEqual(stderr, "")
        self.assertEqual(line, "CORIOLIS_PS_DONE_abc:0")

    def test_read_until_marker_drains_stderr_progress(self):
        channel = mock.Mock()
        channel.exit_status_ready.return_value = False
        channel.recv_stderr_ready.side_effect = [True, False]
        channel.recv_stderr.return_value = b"progress"
        channel.recv_ready.side_effect = [True, False]
        channel.recv.side_effect = [b"CORIOLIS_PS_DONE_abc:0\r\n"]
        self.conn._ps_channel = channel
        self.conn._ps_stdout_buf = bytearray()

        stdout, stderr, line = self.conn._read_until_marker(
            "CORIOLIS_PS_DONE_abc", timeout=5
        )

        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "progress")
        self.assertEqual(line, "CORIOLIS_PS_DONE_abc:0")

    def test_read_until_marker_timeout(self):
        channel = mock.Mock()
        channel.recv_stderr_ready.return_value = False
        channel.exit_status_ready.return_value = False
        channel.recv.side_effect = socket.timeout
        self.conn._ps_channel = channel
        self.conn._ps_stdout_buf = bytearray()
        self.assertRaises(
            exception.OSMorphingSSHOperationTimeout,
            self.conn._read_until_marker,
            "CORIOLIS_PS_DONE_abc",
            timeout=0,
        )

    def test_invoke_persistent_ps(self):
        self.conn._ensure_ps_session = mock.Mock()
        self.conn._write_ps_stdin = mock.Mock()
        self.conn._read_until_marker = mock.Mock(
            return_value=("True\r\n", "", "CORIOLIS_PS_DONE_abc:0")
        )
        with mock.patch.object(windows_ssh.uuid, "uuid4") as mock_uuid:
            mock_uuid.return_value.hex = "abc"
            stdout, stderr, exit_code = self.conn._invoke_persistent_ps("Test-Path")

        self.conn._ensure_ps_session.assert_called_once_with()
        self.conn._write_ps_stdin.assert_called_once()
        self.assertEqual(stdout, "True\r\n")
        self.assertEqual(stderr, "")
        self.assertEqual(exit_code, 0)

    def test_configure_ssh_keepalive(self):
        transport = mock.Mock()
        self.conn._ssh.get_transport.return_value = transport
        self.conn._configure_ssh_keepalive()
        transport.set_keepalive.assert_called_once_with(
            windows_ssh.SSH_KEEPALIVE_INTERVAL
        )
