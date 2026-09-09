# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Pick a Windows minion connection from connection_info port.

Port 5986 uses WinRM. Any other port, including 22, uses SSH.
A missing port uses WinRM, which matches the old os-mount default.
"""

from coriolis import windows_ssh, wsman

WINRM_HTTPS_PORT = 5986


def uses_winrm(connection_info):
    port = (connection_info or {}).get("port")
    if port is None or port == "":
        return True
    return int(port) == WINRM_HTTPS_PORT


def from_connection_info(connection_info, timeout=None):
    """Return WSManConnection or WindowsSSHConnection."""
    if uses_winrm(connection_info):
        conn_cls = wsman.WSManConnection
    else:
        conn_cls = windows_ssh.WindowsSSHConnection
    if timeout is None:
        return conn_cls.from_connection_info(connection_info)
    return conn_cls.from_connection_info(connection_info, timeout)
