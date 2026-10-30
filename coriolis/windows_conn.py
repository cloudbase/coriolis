# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Pick a Windows minion connection.

An explicit [windows_minion] protocol selects SSH or WinRM.
connection_info does not override that option.
An unset protocol uses the provider port.
A missing port or port 5986 uses WinRM.
Any other port uses SSH.
A port set by the provider takes precedence.
An explicit [windows_minion] port is next.
A new minion uses SSH when the protocol option is unset.
That minion uses port 22 unless the port option is set.
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
            "Protocol for a Windows minion. Leave this unset to use "
            "WinRM on a missing port or port 5986, and SSH on any other "
            "port. ssh forces SSH. winrm forces WinRM. "
            "A value in connection_info does not override this option. "
            "A new minion uses SSH when this option is unset."
        ),
    ),
    cfg.IntOpt(
        "port",
        default=None,
        min=1,
        max=65535,
        help=(
            "TCP port for a Windows minion. Leave this unset to use port "
            "22 for ssh and port 5986 for winrm. A set value is used for "
            "either protocol. A port in connection_info takes precedence. "
            "This option does not select the protocol. A port in "
            "connection_info selects the protocol only when the protocol "
            "option is unset."
        ),
    ),
]

CONF = cfg.CONF
CONF.register_opts(windows_minion_opts, "windows_minion")


def minion_connection():
    """Return ssh or winrm for a new Windows minion.

    An unset protocol uses SSH for the new minion.
    """
    protocol = CONF.windows_minion.protocol
    if protocol in (None, ""):
        return CONNECTION_SSH
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
    """Return the TCP port for this connection_info.

    A provider port wins. An explicit port option is next.
    A missing port uses 5986 for WinRM and 22 for SSH.
    """
    raw = (connection_info or {}).get("port")
    if raw not in (None, ""):
        return int(raw)
    if CONF.windows_minion.port:
        return CONF.windows_minion.port
    if uses_winrm(connection_info):
        return WINRM_HTTPS_PORT
    return SSH_PORT


def with_provider_port(connection_info):
    """Keep a provider port. Fill a missing port from the core option."""
    if connection_info and connection_info.get("port") not in (None, ""):
        return connection_info
    info = dict(connection_info or {})
    info["port"] = connection_port(connection_info)
    return info


def uses_winrm(connection_info):
    """Return True when this connection uses WinRM.

    An explicit protocol wins. connection_info does not override it.
    An unset protocol uses WinRM for a missing port or port 5986.
    Any other port uses SSH.
    """
    protocol = CONF.windows_minion.protocol
    if protocol == CONNECTION_WINRM:
        return True
    if protocol == CONNECTION_SSH:
        return False
    raw = (connection_info or {}).get("port")
    if raw in (None, ""):
        return True
    return int(raw) == WINRM_HTTPS_PORT


def from_connection_info(connection_info, timeout=None):
    """Return WSManConnection or WindowsSSHConnection."""
    info = with_provider_port(connection_info)
    if uses_winrm(connection_info):
        conn_cls = wsman.WSManConnection
    else:
        conn_cls = windows_ssh.WindowsSSHConnection
    if timeout is None:
        return conn_cls.from_connection_info(info)
    return conn_cls.from_connection_info(info, timeout)
