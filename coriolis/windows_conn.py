# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Pick a Windows minion connection from connection_info port.

Port 5986 uses WinRM. Any other port, including 22, uses SSH.
A missing port uses WinRM, which matches the old os-mount default.

New Windows minions use [windows_minion] connection_type. ssh is port 22.
winrm is port 5986. A minion that is already deployed keeps the port
saved in connection_info.
"""

from oslo_config import cfg

from coriolis import windows_ssh, wsman

SSH_PORT = 22
WINRM_HTTPS_PORT = 5986
CONNECTION_SSH = "ssh"
CONNECTION_WINRM = "winrm"

windows_minion_opts = [
    cfg.StrOpt(
        "connection_type",
        default=CONNECTION_SSH,
        choices=[CONNECTION_SSH, CONNECTION_WINRM],
        help=(
            "Connection used for a new Windows minion. ssh starts OpenSSH "
            "and uses port 22. winrm uses port 5986. The Windows template "
            "must already accept WinRM when this is winrm. A minion that "
            "is already deployed keeps the port in its connection_info."
        ),
    ),
]

CONF = cfg.CONF
CONF.register_opts(windows_minion_opts, "windows_minion")


def minion_connection():
    """Return ssh or winrm for a new Windows minion."""
    return CONF.windows_minion.connection_type


def minion_uses_ssh():
    """Return True when a new Windows minion uses SSH."""
    return minion_connection() == CONNECTION_SSH


def minion_port():
    """Return 22 for ssh and 5986 for winrm."""
    if minion_uses_ssh():
        return SSH_PORT
    return WINRM_HTTPS_PORT


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
