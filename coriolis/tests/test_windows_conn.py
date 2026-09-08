# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import windows_conn, windows_ssh, wsman
from coriolis.tests import test_base


class WindowsConnTestCase(test_base.CoriolisBaseTestCase):
    def test_connection_protocol_missing_value_is_winrm(self):
        windows_conn.CONF.set_override("protocol", "ssh", group="windows_minion")
        self.addCleanup(
            windows_conn.CONF.clear_override, "protocol", group="windows_minion"
        )
        for info in (
            {"ip": "10.0.0.1"},
            {"ip": "10.0.0.1", "port": 5986},
            {"ip": "10.0.0.1", "port": 22},
            {"ip": "10.0.0.1", "port": 2222},
            None,
        ):
            self.assertEqual(
                windows_conn.CONNECTION_WINRM,
                windows_conn.connection_protocol(info),
            )

    def test_connection_protocol_selects_ssh_or_winrm(self):
        windows_conn.CONF.set_override("protocol", "winrm", group="windows_minion")
        self.addCleanup(
            windows_conn.CONF.clear_override, "protocol", group="windows_minion"
        )
        ssh_info = {"ip": "10.0.0.1", "port": 5986, "protocol": "ssh"}
        winrm_info = {"ip": "10.0.0.1", "port": 22, "protocol": "winrm"}
        self.assertEqual(
            windows_conn.CONNECTION_SSH, windows_conn.connection_protocol(ssh_info)
        )
        self.assertEqual(
            windows_conn.CONNECTION_WINRM,
            windows_conn.connection_protocol(winrm_info),
        )

    @mock.patch.object(wsman.WSManConnection, "from_connection_info")
    @mock.patch.object(windows_ssh.WindowsSSHConnection, "from_connection_info")
    def test_from_connection_info_ssh_ignores_port_and_option(
        self, mock_ssh, mock_winrm
    ):
        windows_conn.CONF.set_override("protocol", "winrm", group="windows_minion")
        self.addCleanup(
            windows_conn.CONF.clear_override, "protocol", group="windows_minion"
        )
        conn_info = {
            "ip": "10.0.0.1",
            "port": 5986,
            "protocol": "ssh",
            "username": "u",
            "password": "p",
        }
        windows_conn.from_connection_info(conn_info, timeout=30)
        mock_ssh.assert_called_once_with(conn_info, 30)
        mock_winrm.assert_not_called()

    @mock.patch.object(wsman.WSManConnection, "from_connection_info")
    @mock.patch.object(windows_ssh.WindowsSSHConnection, "from_connection_info")
    def test_from_connection_info_missing_protocol_uses_winrm(
        self, mock_ssh, mock_winrm
    ):
        windows_conn.CONF.set_override("protocol", "ssh", group="windows_minion")
        self.addCleanup(
            windows_conn.CONF.clear_override, "protocol", group="windows_minion"
        )
        conn_info = {"ip": "10.0.0.1", "port": 22, "username": "u", "password": "p"}
        windows_conn.from_connection_info(conn_info, timeout=30)
        mock_winrm.assert_called_once_with(conn_info, 30)
        mock_ssh.assert_not_called()

    @mock.patch.object(wsman.WSManConnection, "from_connection_info")
    @mock.patch.object(windows_ssh.WindowsSSHConnection, "from_connection_info")
    def test_from_connection_info_port_5986_uses_winrm(self, mock_ssh, mock_winrm):
        windows_conn.CONF.clear_override("protocol", group="windows_minion")
        conn_info = {"ip": "10.0.0.1", "port": 5986, "username": "u", "password": "p"}
        windows_conn.from_connection_info(conn_info, timeout=30)
        mock_winrm.assert_called_once_with(conn_info, 30)
        mock_ssh.assert_not_called()

    @mock.patch.object(wsman.WSManConnection, "from_connection_info")
    @mock.patch.object(windows_ssh.WindowsSSHConnection, "from_connection_info")
    def test_from_connection_info_ssh(self, mock_ssh, mock_winrm):
        conn_info = {
            "ip": "10.0.0.1",
            "port": 2222,
            "protocol": "ssh",
            "username": "u",
            "password": "p",
        }
        windows_conn.from_connection_info(conn_info, timeout=30)
        mock_ssh.assert_called_once_with(conn_info, 30)
        mock_winrm.assert_not_called()

    def test_minion_protocol_default_is_winrm(self):
        windows_conn.CONF.clear_override("protocol", group="windows_minion")
        windows_conn.CONF.clear_override("port", group="windows_minion")
        self.assertEqual(5986, windows_conn.minion_port())
        self.assertEqual(
            {"protocol": "winrm", "port": 5986},
            windows_conn.minion_connection_info(),
        )

    def test_minion_protocol_and_port_are_independent(self):
        windows_conn.CONF.set_override("protocol", "ssh", group="windows_minion")
        windows_conn.CONF.set_override("port", 2222, group="windows_minion")
        self.addCleanup(
            windows_conn.CONF.clear_override, "protocol", group="windows_minion"
        )
        self.addCleanup(
            windows_conn.CONF.clear_override, "port", group="windows_minion"
        )
        self.assertEqual(
            {"protocol": "ssh", "port": 2222},
            windows_conn.minion_connection_info(),
        )
        self.assertEqual(2222, windows_conn.minion_port())
        self.assertEqual(2222, windows_conn.minion_port(windows_conn.CONNECTION_SSH))
        self.assertEqual(2222, windows_conn.minion_port(windows_conn.CONNECTION_WINRM))
        self.assertEqual(
            windows_conn.CONNECTION_WINRM,
            windows_conn.connection_protocol({"port": 2222, "protocol": "winrm"}),
        )

    def test_minion_winrm_custom_port(self):
        windows_conn.CONF.set_override("protocol", "winrm", group="windows_minion")
        windows_conn.CONF.set_override("port", 5987, group="windows_minion")
        self.addCleanup(
            windows_conn.CONF.clear_override, "protocol", group="windows_minion"
        )
        self.addCleanup(
            windows_conn.CONF.clear_override, "port", group="windows_minion"
        )
        self.assertEqual(
            {"protocol": "winrm", "port": 5987},
            windows_conn.minion_connection_info(),
        )
        self.assertEqual(5987, windows_conn.minion_port())
        self.assertEqual(
            windows_conn.CONNECTION_SSH,
            windows_conn.connection_protocol({"port": 22, "protocol": "ssh"}),
        )

    def test_minion_winrm_uses_5986_when_port_is_unset(self):
        windows_conn.CONF.set_override("protocol", "winrm", group="windows_minion")
        windows_conn.CONF.clear_override("port", group="windows_minion")
        self.addCleanup(
            windows_conn.CONF.clear_override, "protocol", group="windows_minion"
        )
        self.assertEqual(5986, windows_conn.minion_port())
        self.assertEqual(5986, windows_conn.minion_port(windows_conn.CONNECTION_WINRM))
        self.assertEqual(22, windows_conn.minion_port(windows_conn.CONNECTION_SSH))

    def test_minion_winrm_keeps_explicit_port_22(self):
        windows_conn.CONF.set_override("protocol", "winrm", group="windows_minion")
        windows_conn.CONF.set_override("port", 22, group="windows_minion")
        self.addCleanup(
            windows_conn.CONF.clear_override, "protocol", group="windows_minion"
        )
        self.addCleanup(
            windows_conn.CONF.clear_override, "port", group="windows_minion"
        )
        self.assertEqual(22, windows_conn.minion_port())
