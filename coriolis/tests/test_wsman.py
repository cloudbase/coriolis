# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import requests
from winrm import protocol

from coriolis import exception
from coriolis.tests import test_base
from coriolis import wsman


class WSManConnectionTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis WSManConnection class."""

    def setUp(self):
        super(WSManConnectionTestCase, self).setUp()
        self.conn = wsman.WSManConnection()
        self.conn._protocol = mock.Mock()
        self.conn._conn_timeout = 10
        self.cmd = "test_cmd"
        self.args = ["arg1", "arg2"]
        self.url = "http://example.com/file"
        self.remote_path = "/remote/path"

    def test__init__timeout(self):
        self.connection = wsman.WSManConnection()
        self.assertEqual(self.connection._conn_timeout, wsman.DEFAULT_TIMEOUT)

    def test__init__timeout_set(self):
        self.connection = wsman.WSManConnection(timeout=100)
        self.assertEqual(self.connection._conn_timeout, 100)

    @mock.patch.object(protocol, 'Protocol')
    def test_connect(self, mock_protocol):
        self.conn.connect('url', 'username', cert_pem='test_cert')
        mock_protocol.assert_called_once_with(
            endpoint='url',
            transport='ssl',
            username='username',
            password=None,
            cert_pem="test_cert",
            cert_key_pem=None
        )

    @mock.patch.object(protocol, 'Protocol')
    def test_connect_no_auth(self, mock_protocol):
        self.conn.connect('url', 'username')
        mock_protocol.assert_called_once_with(
            endpoint='url',
            transport='plaintext',
            username='username',
            password=None,
            cert_pem=None,
            cert_key_pem=None
        )

    @mock.patch.object(wsman.WSManConnection, 'connect')
    @mock.patch('coriolis.utils.wait_for_port_connectivity')
    def test_from_connection_info(self, mock_wait_for_port_connectivity,
                                  mock_connect):
        connection_info = {
            "ip": "127.0.0.1",
            "username": "user",
            "password": "pass",
        }
        result = self.conn.from_connection_info(connection_info)
        mock_wait_for_port_connectivity.assert_called_once_with(
            "127.0.0.1", 5986)
        mock_connect.assert_called_once_with(
            url="https://127.0.0.1:5986/wsman", username="user",
            password="pass", cert_pem=None, cert_key_pem=None)
        self.assertIsInstance(result, self.conn.__class__)

    def test_from_connection_info_missing_keys(self):
        self.assertRaises(ValueError, self.conn.from_connection_info,
                          {"username": "user", "password": "pass"})

    def test_from_connection_info_invalid_type(self):
        self.assertRaises(ValueError, self.conn.from_connection_info,
                          'invalid-connection-type')

    def test_disconnect(self):
        self.conn.disconnect()
        self.assertIsNone(self.conn._protocol)

    def test_set_timeout(self):
        self.conn.set_timeout(self.conn._conn_timeout)
        self.assertEqual(self.conn._protocol.transport.timeout,
                         self.conn._conn_timeout)
        self.assertEqual(self.conn._protocol.timeout,
                         self.conn._conn_timeout)

    def test__exec_command(self):
        self.conn._protocol.get_command_output.return_value = (
            "std_out", "std_err", 0)
        std_out, std_err, exit_code = self.conn._exec_command(
            self.cmd, self.args)
        self.assertEqual(std_out, "std_out")
        self.assertEqual(std_err, "std_err")
        self.assertEqual(exit_code, 0)

        self.conn._protocol.open_shell.assert_called_once_with(
            codepage=wsman.CODEPAGE_UTF8)
        shell_id = self.conn._protocol.open_shell.return_value
        self.conn._protocol.run_command.assert_called_once_with(
            shell_id, self.cmd, self.args)
        command_id = self.conn._protocol.run_command.return_value
        self.conn._protocol.get_command_output.assert_called_once_with(
            shell_id, command_id)
        self.conn._protocol.cleanup_command.assert_called_once_with(
            shell_id, command_id)
        self.conn._protocol.close_shell.assert_called_once_with(shell_id)

    def test__exec_command_exception(self):
        self.conn._protocol.get_command_output.side_effect = requests.\
            exceptions.ReadTimeout
        self.assertRaises(exception.OSMorphingWinRMOperationTimeout,
                          self.conn._exec_command, self.cmd, self.args)
        self.conn._protocol.cleanup_command.assert_called_once_with(
            mock.ANY, mock.ANY)
        self.conn._protocol.close_shell.assert_called_once_with(mock.ANY)

    def test__exec_command_invalid_credentials(self):
        self.conn._protocol.open_shell.side_effect = (
            wsman.winrm_exceptions.InvalidCredentialsError)

        self.assertRaises(exception.NotAuthorized, self.conn._exec_command,
                          self.cmd, self.args)
        self.conn._protocol.close_shell.assert_not_called()

    def test_exec_command(self):
        self.conn._protocol.get_command_output.return_value = (
            "std_out", "std_err", 0)
        std_out = self.conn.exec_command(self.cmd, self.args)
        self.assertEqual(std_out, "std_out")

    def test_exec_command_exception(self):
        self.conn._protocol.get_command_output.return_value = (
            "std_out", "std_err", 1)
        self.assertRaises(exception.CoriolisException, self.conn.exec_command,
                          self.cmd, self.args)

    def test_exec_ps_command(self):
        self.conn.exec_command = mock.Mock()
        self.conn.exec_command.return_value = "std_out\n\n"
        result = self.conn.exec_ps_command(self.cmd)
        self.conn.exec_command.assert_called_once_with(
            "powershell.exe", ["-EncodedCommand", 'dABlAHMAdABfAGMAbQBkAA=='],
            timeout=None)
        self.assertEqual(result, "std_out")

    def test_test_path(self):
        self.conn.exec_ps_command = mock.Mock()
        self.conn.exec_ps_command.return_value = "True"
        result = self.conn.test_path("test_path")
        self.conn.exec_ps_command.assert_called_once_with(
            "Test-Path -Path \"test_path\"")
        self.assertTrue(result)

    def test_download_file(self):
        self.conn.exec_ps_command = mock.Mock()
        self.conn.download_file(self.url, self.remote_path)
        self.conn.exec_ps_command.assert_called_once()

    def test_download_file_exception(self):
        self.conn.exec_ps_command = mock.Mock()
        self.conn.exec_ps_command.side_effect = exception.CoriolisException
        self.assertRaises(exception.CoriolisException, self.conn.download_file,
                          self.url, self.remote_path)
        self.conn.exec_ps_command.assert_called_once()

    def test_write_file(self):
        self.conn.exec_ps_command = mock.Mock()
        self.conn.write_file(self.remote_path, b'file content')
        self.conn.exec_ps_command.assert_called_once_with(
            "[IO.File]::WriteAllBytes('%s', [Convert]::FromBase64String('%s'))"
            % (self.remote_path, 'ZmlsZSBjb250ZW50'), ignore_stdout=True)
