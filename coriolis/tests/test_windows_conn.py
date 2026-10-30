# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import windows_conn, windows_ssh, wsman
from coriolis.tests import test_base


class WindowsConnTestCase(test_base.CoriolisBaseTestCase):
    def test_uses_winrm_unset_protocol_follows_the_provider_port(self):
        windows_conn.CONF.clear_override("protocol", group="windows_minion")
        self.assertTrue(windows_conn.uses_winrm({"ip": "10.0.0.1"}))
        self.assertTrue(windows_conn.uses_winrm({"ip": "10.0.0.1", "port": 5986}))
        self.assertTrue(
            windows_conn.uses_winrm(
                {"ip": "10.0.0.1", "port": "5986", "protocol": "ssh"}
            )
        )
        self.assertTrue(windows_conn.uses_winrm(None))
        self.assertFalse(windows_conn.uses_winrm({"ip": "10.0.0.1", "port": 22}))
        self.assertFalse(windows_conn.uses_winrm({"ip": "10.0.0.1", "port": 2222}))

    def test_uses_winrm_explicit_ssh_ignores_port_5986(self):
        windows_conn.CONF.set_override("protocol", "ssh", group="windows_minion")
        self.addCleanup(
            windows_conn.CONF.clear_override, "protocol", group="windows_minion"
        )
        self.assertFalse(windows_conn.uses_winrm({"ip": "10.0.0.1", "port": 5986}))
        self.assertFalse(windows_conn.uses_winrm(None))

    def test_uses_winrm_core_winrm_overrides_connection_info(self):
        windows_conn.CONF.set_override("protocol", "winrm", group="windows_minion")
        self.addCleanup(
            windows_conn.CONF.clear_override, "protocol", group="windows_minion"
        )
        self.assertTrue(windows_conn.uses_winrm({"ip": "10.0.0.1", "port": 22}))
        self.assertTrue(windows_conn.uses_winrm({"port": 5986, "protocol": "ssh"}))
        self.assertTrue(windows_conn.uses_winrm(None))

    @mock.patch.object(wsman.WSManConnection, "from_connection_info")
    @mock.patch.object(windows_ssh.WindowsSSHConnection, "from_connection_info")
    def test_from_connection_info_winrm(self, mock_ssh, mock_winrm):
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
        mock_winrm.assert_called_once_with(conn_info, 30)
        mock_ssh.assert_not_called()

    @mock.patch.object(wsman.WSManConnection, "from_connection_info")
    @mock.patch.object(windows_ssh.WindowsSSHConnection, "from_connection_info")
    def test_from_connection_info_winrm_on_port_22(self, mock_ssh, mock_winrm):
        windows_conn.CONF.set_override("protocol", "winrm", group="windows_minion")
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

    def test_minion_connection_default_is_ssh(self):
        windows_conn.CONF.clear_override("protocol", group="windows_minion")
        windows_conn.CONF.clear_override("port", group="windows_minion")
        self.assertEqual("ssh", windows_conn.minion_connection())
        self.assertTrue(windows_conn.minion_uses_ssh())
        self.assertEqual(22, windows_conn.minion_port())

    def test_minion_protocol_and_port_are_independent(self):
        windows_conn.CONF.set_override("protocol", "ssh", group="windows_minion")
        windows_conn.CONF.set_override("port", 2222, group="windows_minion")
        self.addCleanup(
            windows_conn.CONF.clear_override, "protocol", group="windows_minion"
        )
        self.addCleanup(
            windows_conn.CONF.clear_override, "port", group="windows_minion"
        )
        self.assertEqual("ssh", windows_conn.minion_connection())
        self.assertTrue(windows_conn.minion_uses_ssh())
        self.assertEqual(2222, windows_conn.minion_port())
        self.assertFalse(windows_conn.uses_winrm({"port": 2222, "protocol": "winrm"}))

    def test_minion_winrm_custom_port(self):
        windows_conn.CONF.set_override("protocol", "winrm", group="windows_minion")
        windows_conn.CONF.set_override("port", 5987, group="windows_minion")
        self.addCleanup(
            windows_conn.CONF.clear_override, "protocol", group="windows_minion"
        )
        self.addCleanup(
            windows_conn.CONF.clear_override, "port", group="windows_minion"
        )
        self.assertEqual("winrm", windows_conn.minion_connection())
        self.assertFalse(windows_conn.minion_uses_ssh())
        self.assertEqual(5987, windows_conn.minion_port())
        self.assertTrue(windows_conn.uses_winrm({"port": 22, "protocol": "ssh"}))

    def test_minion_winrm_uses_5986_when_port_is_unset(self):
        windows_conn.CONF.set_override("protocol", "winrm", group="windows_minion")
        windows_conn.CONF.clear_override("port", group="windows_minion")
        self.addCleanup(
            windows_conn.CONF.clear_override, "protocol", group="windows_minion"
        )
        self.assertEqual(5986, windows_conn.minion_port())

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

    def test_connection_port_provider_wins(self):
        windows_conn.CONF.set_override("port", 2222, group="windows_minion")
        self.addCleanup(
            windows_conn.CONF.clear_override, "port", group="windows_minion"
        )
        info = {"ip": "10.0.0.1", "port": 5986}
        self.assertEqual(5986, windows_conn.connection_port(info))
        self.assertIs(info, windows_conn.with_provider_port(info))

    def test_connection_port_uses_core_option(self):
        windows_conn.CONF.set_override("port", 2222, group="windows_minion")
        self.addCleanup(
            windows_conn.CONF.clear_override, "port", group="windows_minion"
        )
        self.assertEqual(2222, windows_conn.connection_port({"ip": "10.0.0.1"}))
        self.assertEqual(2222, windows_conn.connection_port(None))
        filled = windows_conn.with_provider_port({"ip": "10.0.0.1"})
        self.assertEqual(2222, filled["port"])
