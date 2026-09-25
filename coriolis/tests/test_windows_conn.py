# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import windows_conn, windows_ssh, wsman
from coriolis.tests import test_base


class WindowsConnTestCase(test_base.CoriolisBaseTestCase):
    def test_uses_winrm_default_port(self):
        self.assertTrue(windows_conn.uses_winrm({"ip": "10.0.0.1"}))
        self.assertTrue(windows_conn.uses_winrm({"ip": "10.0.0.1", "port": 5986}))
        self.assertFalse(windows_conn.uses_winrm({"ip": "10.0.0.1", "port": 22}))

    @mock.patch.object(wsman.WSManConnection, "from_connection_info")
    @mock.patch.object(windows_ssh.WindowsSSHConnection, "from_connection_info")
    def test_from_connection_info_winrm(self, mock_ssh, mock_winrm):
        conn_info = {"ip": "10.0.0.1", "port": 5986, "username": "u", "password": "p"}
        windows_conn.from_connection_info(conn_info, timeout=30)
        mock_winrm.assert_called_once_with(conn_info, 30)
        mock_ssh.assert_not_called()

    @mock.patch.object(wsman.WSManConnection, "from_connection_info")
    @mock.patch.object(windows_ssh.WindowsSSHConnection, "from_connection_info")
    def test_from_connection_info_ssh(self, mock_ssh, mock_winrm):
        conn_info = {"ip": "10.0.0.1", "port": 22, "username": "u", "password": "p"}
        windows_conn.from_connection_info(conn_info, timeout=30)
        mock_ssh.assert_called_once_with(conn_info, 30)
        mock_winrm.assert_not_called()
