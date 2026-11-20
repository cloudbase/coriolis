# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Pick a Windows minion connection.

The provider writes protocol and port on connection_info.
The core reads that dict.
A missing protocol uses WinRM.
The port does not select the protocol.
[windows_minion] protocol and port are for the provider.
An unset protocol option keeps WinRM.
"""

from oslo_config import cfg

from coriolis import windows_ssh, wsman

SSH_PORT = 22
WINRM_HTTPS_PORT = 5986
CONNECTION_SSH = "ssh"
CONNECTION_WINRM = "winrm"

windows_minion_opts = [
    cfg.StrOpt(
        "protocol",
        default=None,
        choices=[CONNECTION_SSH, CONNECTION_WINRM],
        help=(
            "Protocol for a new Windows minion. The provider reads this "
            "value and sets connection_info['protocol']. Leave this unset "
            "to use WinRM. ssh selects SSH on a provider that supports it. "
            "winrm selects WinRM. The core reads connection_info and uses "
            "WinRM when protocol is missing."
        ),
    ),
    cfg.IntOpt(
        "port",
        default=None,
        min=1,
        max=65535,
        help=(
            "TCP port for a new Windows minion. The provider reads this "
            "value when it builds connection_info. Leave this unset to use "
            "port 22 for ssh and port 5986 for winrm. A set value is used "
            "for either protocol. The core uses the port on connection_info."
        ),
    ),
]

CONF = cfg.CONF
CONF.register_opts(windows_minion_opts, "windows_minion")


def minion_connection():
    """Return ssh or winrm for a new Windows minion.

    An unset protocol uses WinRM.
    The provider writes this value on connection_info.
    """
    protocol = CONF.windows_minion.protocol
    if protocol in (None, ""):
        return CONNECTION_WINRM
    return protocol


def minion_uses_ssh():
    """Return True when a new Windows minion uses SSH."""
    return minion_connection() == CONNECTION_SSH


def minion_port():
    """Return the TCP port for a new Windows minion.

    An explicit port option wins. An unset port uses 22 for ssh and
    5986 for winrm.
    """
    port = CONF.windows_minion.port
    if port:
        return port
    if minion_uses_ssh():
        return SSH_PORT
    return WINRM_HTTPS_PORT


def connection_port(connection_info):
    """Return the TCP port on connection_info.

    A missing port uses 5986 for WinRM and 22 for SSH.
    """
    raw = (connection_info or {}).get("port")
    if raw not in (None, ""):
        return int(raw)
    if uses_winrm(connection_info):
        return WINRM_HTTPS_PORT
    return SSH_PORT


def _protocol_from_connection_info(connection_info, protocol=None):
    """Return ssh or winrm for this connection.

    An explicit protocol wins. A missing protocol uses WinRM.
    The port does not select the protocol.
    """
    if protocol:
        return protocol
    raw = (connection_info or {}).get("protocol")
    if raw in (None, ""):
        return CONNECTION_WINRM
    return raw


def uses_winrm(connection_info):
    """Return True when this connection uses WinRM."""
    return _protocol_from_connection_info(connection_info) == CONNECTION_WINRM


def from_connection_info(connection_info, timeout=None):
    """Return WSManConnection or WindowsSSHConnection."""
    if uses_winrm(connection_info):
        conn_cls = wsman.WSManConnection
    else:
        conn_cls = windows_ssh.WindowsSSHConnection
    if timeout is None:
        return conn_cls.from_connection_info(connection_info)
    return conn_cls.from_connection_info(connection_info, timeout)
