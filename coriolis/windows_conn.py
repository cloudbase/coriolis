# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Pick a Windows minion connection.

The provider writes protocol and port on connection_info.
The core reads that dict.
A missing protocol uses WinRM.
The port does not select the protocol.
[windows_minion] protocol and port are for the provider.
The default protocol is winrm.
"""

from oslo_config import cfg

from coriolis import windows_ssh, wsman

SSH_PORT = 22
WINRM_HTTPS_PORT = 5986
CONNECTION_SSH = "ssh"
CONNECTION_WINRM = "winrm"
DEFAULT_PROTOCOL_PORTS = {
    CONNECTION_WINRM: WINRM_HTTPS_PORT,
    CONNECTION_SSH: SSH_PORT,
}

windows_minion_opts = [
    cfg.StrOpt(
        "protocol",
        default=CONNECTION_WINRM,
        choices=[CONNECTION_SSH, CONNECTION_WINRM],
        help=(
            "Protocol for a new Windows minion. The provider reads this "
            "value and sets connection_info['protocol']. The default is "
            "winrm. ssh selects SSH on a provider that supports it. "
            "The core reads connection_info and uses WinRM when protocol "
            "is missing."
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


def minion_connection_info():
    """Return protocol and port for a new Windows minion.

    The provider copies these fields onto connection_info.
    The default protocol is winrm.
    """
    protocol = CONF.windows_minion.protocol
    return {"protocol": protocol, "port": minion_port()}


def minion_port(protocol=None):
    """Return the TCP port for a Windows minion."""
    return CONF.windows_minion.port or DEFAULT_PROTOCOL_PORTS.get(
        protocol or CONF.windows_minion.protocol
    )


def connection_protocol(connection_info, protocol=None):
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


def from_connection_info(connection_info, timeout=None):
    """Return WSManConnection or WindowsSSHConnection."""
    if connection_protocol(connection_info) == CONNECTION_WINRM:
        conn_cls = wsman.WSManConnection
    else:
        conn_cls = windows_ssh.WindowsSSHConnection
    return conn_cls.from_connection_info(connection_info, timeout)
