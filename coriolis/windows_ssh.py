# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""SSH connection for Windows OS morphing minions.

The Coriolis worker connects to the minion over OpenSSH. It does not open
WinRM. Morphing commands still run as powershell.exe, diskpart, reg.exe,
and DISM.

Minion requirements
-------------------
The Windows morphing minion must provide all of the following:

* Windows Server 2019 or later.
* OpenSSH Server listening on TCP port 22. A custom SSH port is allowed
  when connection_info['port'] is that port.
* Windows PowerShell 5.1. The executable must be powershell.exe on PATH
  for the SSH user.
* An SSH login that can run elevated morphing commands. Providers usually
  send Administrator. Auth is a password or an SSH private key.
* The SSH user must start powershell.exe with -Command -. The process
  reads commands from stdin. PowerShell 7 (pwsh) is not required.
* diskpart.exe, reg.exe, and DISM must be available. These tools are
  present on Windows Server.
* The Coriolis worker must reach the minion IP on the SSH port.

Not required
------------
* WinRM, HTTPS port 5986, or a WinRM listener.
* PowerShell 7, pwsh.exe, or an OpenSSH Subsystem powershell line.
* PSRP remoting (Enter-PSSession -HostName, pypsrp SSH).
"""

import base64
import socket
import time
import uuid

import paramiko
from oslo_log import log as logging
from oslo_utils import strutils

from coriolis import exception, utils

LOG = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 3600
SSH_PORT = 22
PS_ALIVE_CHECK_INTERVAL = 30
PS_PING_TIMEOUT = 15
PS_START_TIMEOUT = 60
SSH_KEEPALIVE_INTERVAL = 30
_PS_READY_MARKER = "CORIOLIS_PS_READY"
_PS_START_COMMAND = (
    "powershell.exe -NoLogo -NoProfile -NonInteractive "
    "-ExecutionPolicy RemoteSigned -OutputFormat Text -Command -"
)
_PS_BOOTSTRAP = (
    "$ProgressPreference = 'SilentlyContinue'; "
    "[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false; "
    "[Console]::InputEncoding = New-Object System.Text.UTF8Encoding $false; "
    "$OutputEncoding = [Console]::OutputEncoding; "
    "$ErrorActionPreference = 'Continue'; "
    "Write-Output '%s'\r\n" % _PS_READY_MARKER
)


def _is_reg_exe(cmd):
    base = str(cmd).replace("\\", "/").rsplit("/", 1)[-1].lower()
    return base in ("reg", "reg.exe")


_CMD_QUOTE_CHARS = (" ", "\t", '"', "&", "|", "(", ")", "<", ">", "^", "%")


def _quote_cmd_arg(part):
    part_str = str(part)
    if (not part_str) or any(ch in part_str for ch in _CMD_QUOTE_CHARS):
        return '"%s"' % part_str.replace('"', '""')
    return part_str


def _format_windows_command(cmd, args):
    return " ".join(_quote_cmd_arg(p) for p in [cmd] + list(args or []))


def _escape_trailing_backslash_for_ssh(command):
    """Keep a trailing backslash from eating the SSH quote.

    Windows OpenSSH wraps the exec string in double quotes. An odd number
    of trailing backslashes escapes that quote. PowerShell then reports a
    missing string terminator. DISM /image:F:\\ is the usual case.
    """
    n = len(command) - len(command.rstrip("\\"))
    if n % 2 == 1:
        return command + "\\"
    return command


def _wrap_native_command_for_ssh(command):
    """Run native tools via cmd.exe.

    OpenSSH DefaultShell is often PowerShell. PowerShell parses (OI) in
    icacls grants as a command. cmd.exe does not when the grant is quoted.
    """
    command = _escape_trailing_backslash_for_ssh(command)
    return 'cmd.exe /c "%s"' % command.replace('"', '""')


def _build_ssh_exec_command(cmd, args):
    formatted = _format_windows_command(cmd, args)
    wrapped = _wrap_native_command_for_ssh(formatted)
    return _escape_trailing_backslash_for_ssh(wrapped)


def _strip_ps_output(stdout):
    return (stdout or "").strip()


def _drain_ssh_channel(channel, stdout_buf, stderr_buf):
    """Read available SSH bytes. Do not block on one stream.

    Cmdlets such as Expand-Archive write progress to stderr. A blocking
    stdout read leaves that stderr unread. The SSH window fills and the
    remote process waits forever.
    """
    got = False
    while channel.recv_stderr_ready():
        chunk = channel.recv_stderr(65536)
        if not chunk:
            break
        stderr_buf.extend(chunk)
        got = True
    while channel.recv_ready():
        chunk = channel.recv(65536)
        if not chunk:
            break
        stdout_buf.extend(chunk)
        got = True
    return got


def _split_on_marker_line(buf, marker):
    """Return (before, line, after) when a full marker line is present."""
    marker_bytes = marker.encode("ascii")
    idx = buf.find(marker_bytes)
    if idx < 0:
        return None
    rest = buf[idx:]
    nl = rest.find(b"\n")
    if nl < 0:
        return None
    line = rest[:nl].rstrip(b"\r").decode("ascii", errors="replace")
    before = buf[:idx]
    after = rest[nl + 1 :]
    return before, line, after


class WindowsSSHConnection(object):
    """Windows morphing minion connection over SSH.

    See the module docstring for minion requirements. PowerShell cmdlets
    reuse one remote powershell.exe process. Native commands still use one
    SSH exec per call.
    """

    EOL = "\r\n"

    def __init__(self, timeout=None):
        self._ssh = None
        self._conn_timeout = int(timeout or DEFAULT_TIMEOUT)
        self._host = None
        self._port = SSH_PORT
        self._username = None
        self._password = None
        self._pkey = None
        self._ps_stdin = None
        self._ps_channel = None
        self._ps_stdout_buf = bytearray()
        self._ps_last_alive = 0.0

    @classmethod
    def from_connection_info(cls, connection_info, timeout=DEFAULT_TIMEOUT):
        """Return a WindowsSSHConnection for the provided conn info."""
        if not isinstance(connection_info, dict):
            raise ValueError(
                "Windows minion connection must be a dict. Got type '%s', "
                "value: %s" % (type(connection_info), connection_info)
            )

        required_keys = ["ip", "username"]
        missing = [key for key in required_keys if key not in connection_info]
        if missing:
            raise ValueError(
                "The following keys were missing from Windows SSH connection "
                "info %s. Got: %s" % (missing, connection_info)
            )
        if not connection_info.get("password") and not connection_info.get("pkey"):
            raise ValueError(
                "Windows SSH connection info must include password or pkey. "
                "Got: %s" % connection_info
            )

        host = connection_info["ip"]
        port = connection_info.get("port") or SSH_PORT
        username = connection_info["username"]
        password = connection_info.get("password")
        pkey = connection_info.get("pkey")

        LOG.info(
            "Waiting for SSH connectivity on host: %(host)s:%(port)s",
            {"host": host, "port": port},
        )
        utils.wait_for_port_connectivity(host, port)

        conn = cls(timeout)
        conn.connect(
            host=host,
            port=port,
            username=username,
            password=password,
            pkey=pkey,
        )
        return conn

    def connect(self, host, port, username, password=None, pkey=None):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._pkey = pkey
        try:
            self._open_ssh()
        except exception.CoriolisException as ex:
            if isinstance(ex.__cause__, paramiko.AuthenticationException):
                raise exception.NotAuthorized(
                    message="The SSH connection credentials are invalid. "
                    "If you are using a template with a default "
                    "pre-baked username/password, please ensure "
                    "that you have passed the credentials to the "
                    "destination Coriolis plugin you have selected,"
                    " either via the Target Environment parameters "
                    "set when creating the Migration/Replica, or "
                    "by setting it in the destination plugin's "
                    "dedicated section of the coriolis.conf "
                    "static configuration file."
                ) from ex
            raise
        self._start_ps_session()

    def _open_ssh(self):
        self._ssh = utils.connect_ssh(
            hostname=self._host,
            port=self._port,
            username=self._username,
            password=self._password,
            pkey=self._pkey,
        )
        self._ssh.set_log_channel("paramiko.morpher.%s.%s" % (self._host, self._port))
        self._configure_ssh_keepalive()

    def _configure_ssh_keepalive(self):
        if not self._ssh:
            return
        transport = self._ssh.get_transport()
        if transport:
            transport.set_keepalive(SSH_KEEPALIVE_INTERVAL)

    def _ssh_is_alive(self):
        if not self._ssh:
            return False
        transport = self._ssh.get_transport()
        return bool(transport and transport.is_active())

    def _ps_session_is_alive(self):
        if not self._ssh_is_alive():
            return False
        channel = self._ps_channel
        if channel is None:
            return False
        if channel.closed:
            return False
        if channel.exit_status_ready():
            return False
        return True

    def _should_probe_alive(self):
        if not self._ps_last_alive:
            return True
        idle = time.monotonic() - self._ps_last_alive
        return idle >= PS_ALIVE_CHECK_INTERVAL

    def _close_ps_session(self, wait=False, wait_timeout=10):
        stdin = self._ps_stdin
        channel = self._ps_channel
        self._ps_stdin = None
        self._ps_channel = None
        self._ps_stdout_buf = bytearray()
        if stdin is not None:
            try:
                stdin.write(b"exit\r\n")
                stdin.flush()
            except Exception:
                pass
            try:
                stdin.close()
            except Exception:
                pass
        if wait and channel is not None:
            deadline = time.monotonic() + int(wait_timeout)
            while time.monotonic() < deadline:
                try:
                    if channel.exit_status_ready():
                        break
                except Exception:
                    break
                time.sleep(0.05)
        if channel is not None:
            try:
                channel.close()
            except Exception:
                pass

    def _reconnect_ssh(self):
        if self._ssh:
            try:
                self._ssh.close()
            except Exception:
                pass
            self._ssh = None
        LOG.warning(
            "Reconnecting SSH to Windows minion %(host)s:%(port)s",
            {"host": self._host, "port": self._port},
        )
        utils.wait_for_port_connectivity(self._host, self._port)
        self._open_ssh()

    def _restart_ps_session(self):
        self._close_ps_session()
        if not self._ssh_is_alive():
            self._reconnect_ssh()
        self._start_ps_session()

    def _start_ps_session(self):
        self._close_ps_session()
        LOG.info(
            "Starting persistent PowerShell session on %(host)s:%(port)s",
            {"host": self._host, "port": self._port},
        )
        try:
            stdin, stdout, _stderr = self._ssh.exec_command(
                _PS_START_COMMAND, timeout=float(self._conn_timeout)
            )
            self._ps_stdin = stdin
            self._ps_channel = stdout.channel
            self._ps_stdout_buf = bytearray()
            self._write_ps_stdin(_PS_BOOTSTRAP.encode("utf-8"))
            self._read_until_marker(_PS_READY_MARKER, timeout=PS_START_TIMEOUT)
            self._ps_last_alive = time.monotonic()
        except Exception:
            self._close_ps_session()
            raise

    def _write_ps_stdin(self, data):
        try:
            self._ps_stdin.write(data)
            self._ps_stdin.flush()
        except (OSError, EOFError, socket.timeout, paramiko.SSHException) as ex:
            self._close_ps_session()
            raise exception.CoriolisException(
                "PowerShell SSH session write failed: %s" % ex
            ) from ex

    def _read_until_marker(self, marker, timeout):
        deadline = time.monotonic() + int(timeout)
        stderr_buf = bytearray()
        channel = self._ps_channel
        if channel is None:
            raise exception.CoriolisException(
                "PowerShell SSH session is not connected."
            )
        channel.settimeout(0.1)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise exception.OSMorphingSSHOperationTimeout(
                    cmd=marker, timeout=timeout
                )
            try:
                _drain_ssh_channel(channel, self._ps_stdout_buf, stderr_buf)
            except socket.timeout:
                pass

            parsed = _split_on_marker_line(self._ps_stdout_buf, marker)
            if parsed is not None:
                before, line, after = parsed
                self._ps_stdout_buf = bytearray(after)
                stdout = before.decode("utf-8", errors="replace")
                stderr = stderr_buf.decode("utf-8", errors="replace")
                return stdout, stderr, line

            if channel.exit_status_ready() and not channel.recv_ready():
                leftover = self._ps_stdout_buf.decode("utf-8", errors="replace")
                raise exception.CoriolisException(
                    "PowerShell SSH session closed while waiting for "
                    "marker '%s'. Output: %s" % (marker, leftover)
                )
            time.sleep(0.05)

    def _ping_ps_session(self):
        token = uuid.uuid4().hex
        marker = "CORIOLIS_PS_ALIVE_%s" % token
        try:
            self._write_ps_stdin(("Write-Output '%s'\r\n" % marker).encode("utf-8"))
            _stdout, _stderr, line = self._read_until_marker(
                marker, timeout=PS_PING_TIMEOUT
            )
            alive = line.startswith(marker)
            if alive:
                self._ps_last_alive = time.monotonic()
            return alive
        except Exception:
            LOG.debug(
                "PowerShell SSH alive check failed: %s",
                utils.get_exception_details(),
            )
            return False

    def _ensure_ps_session(self):
        if self._ps_session_is_alive():
            if self._should_probe_alive() and not self._ping_ps_session():
                LOG.warning(
                    "PowerShell SSH session failed the alive check. "
                    "Starting a new session."
                )
                self._restart_ps_session()
            return
        LOG.warning("PowerShell SSH session is not alive. Starting a new session.")
        self._restart_ps_session()

    def _release_ps_registry_handles(self):
        """Stop PowerShell so it does not hold loaded hive keys.

        Get-ItemProperty keeps RegistryKey objects in this process.
        Garbage collection does not drop those handles. End the process
        before reg.exe load or unload.
        """
        self._close_ps_session(wait=True)

    def _build_ps_wrapper(self, cmd, token):
        encoded_cmd = base64.b64encode(cmd.encode("utf-16le")).decode()
        marker = "CORIOLIS_PS_DONE_%s" % token
        return (
            "$ProgressPreference = 'SilentlyContinue'; "
            "$__c = [System.Text.Encoding]::Unicode.GetString("
            "[Convert]::FromBase64String('%s')); "
            "$__e = 0; "
            "try { Invoke-Expression -Command $__c } "
            "catch { Write-Error -ErrorRecord $_; $__e = 1 }; "
            "Write-Output ('%s:' + $__e)\r\n" % (encoded_cmd, marker)
        )

    @utils.retry_on_error(
        terminal_exceptions=[
            exception.NotAuthorized,
            exception.OSMorphingSSHOperationTimeout,
        ]
    )
    def _invoke_persistent_ps(self, cmd, timeout=None):
        timeout = int(timeout or self._conn_timeout)
        self._ensure_ps_session()
        token = uuid.uuid4().hex
        marker = "CORIOLIS_PS_DONE_%s" % token
        wrapper = self._build_ps_wrapper(cmd, token)
        self._write_ps_stdin(wrapper.encode("utf-8"))
        stdout, stderr, line = self._read_until_marker(marker, timeout=timeout)
        self._ps_last_alive = time.monotonic()
        prefix = "%s:" % marker
        if not line.startswith(prefix):
            raise exception.CoriolisException(
                "PowerShell SSH session returned an invalid status line: %s" % line
            )
        try:
            exit_code = int(line[len(prefix) :].strip())
        except ValueError as ex:
            raise exception.CoriolisException(
                "PowerShell SSH session returned a non-numeric exit code: %s" % line
            ) from ex
        return stdout, stderr, exit_code

    def disconnect(self):
        self._close_ps_session()
        if self._ssh:
            self._ssh.close()
        self._ssh = None

    def set_timeout(self, timeout):
        if timeout:
            self._conn_timeout = int(timeout)

    def _read_ssh_exec_output(self, channel, timeout, sanitized_cmd):
        """Read stdout and stderr until the SSH exec channel exits."""
        deadline = time.monotonic() + int(timeout)
        stdout_buf = bytearray()
        stderr_buf = bytearray()
        channel.settimeout(0.1)
        while True:
            if time.monotonic() >= deadline:
                raise exception.OSMorphingSSHOperationTimeout(
                    cmd=sanitized_cmd, timeout=timeout
                )
            try:
                _drain_ssh_channel(channel, stdout_buf, stderr_buf)
            except socket.timeout:
                pass
            if channel.exit_status_ready():
                try:
                    _drain_ssh_channel(channel, stdout_buf, stderr_buf)
                except socket.timeout:
                    pass
                if not channel.recv_ready() and not channel.recv_stderr_ready():
                    break
            time.sleep(0.05)
        exit_code = channel.recv_exit_status()
        stdout_str = stdout_buf.decode("utf-8", errors="replace")
        stderr_str = stderr_buf.decode("utf-8", errors="replace")
        return stdout_str, stderr_str, exit_code

    @utils.retry_on_error(
        terminal_exceptions=[
            exception.NotAuthorized,
            exception.OSMorphingSSHOperationTimeout,
        ]
    )
    def _exec_command(self, cmd, args=[], timeout=None, sanitizable=True):
        command = _build_ssh_exec_command(cmd, args)
        if sanitizable:
            sanitized_cmd = strutils.mask_password(command)
        else:
            sanitized_cmd = "***"

        timeout = int(timeout or self._conn_timeout)
        LOG.debug("Executing SSH command: %s", sanitized_cmd)
        try:
            _, stdout, _stderr = self._ssh.exec_command(command, timeout=float(timeout))
            return self._read_ssh_exec_output(stdout.channel, timeout, sanitized_cmd)
        except socket.timeout as ex:
            raise exception.OSMorphingSSHOperationTimeout(
                cmd=sanitized_cmd, timeout=timeout
            ) from ex

    def exec_command(
        self,
        cmd,
        args=[],
        timeout=None,
        sanitizable=True,
        include_stderr=False,
    ):
        if sanitizable:
            sanitized_cmd = strutils.mask_password(_build_ssh_exec_command(cmd, args))
        else:
            sanitized_cmd = "***"
        if _is_reg_exe(cmd):
            self._release_ps_registry_handles()
        LOG.debug("Executing Windows SSH command: %s", sanitized_cmd)
        std_out, std_err, exit_code = self._exec_command(
            cmd, args, timeout=timeout, sanitizable=sanitizable
        )

        if exit_code:
            raise exception.CoriolisException(
                "Command \"%s\" failed with exit code: %s\n"
                "stdout: %s\nstd_err: %s" % (sanitized_cmd, exit_code, std_out, std_err)
            )

        if include_stderr:
            return std_out, std_err
        return std_out

    def exec_ps_command(
        self,
        cmd,
        ignore_stdout=False,
        timeout=None,
        include_stderr=False,
    ):
        LOG.debug("Executing PS command: %s", strutils.mask_password(cmd))
        stdout, stderr, exit_code = self._invoke_persistent_ps(cmd, timeout=timeout)
        if exit_code:
            sanitized_cmd = strutils.mask_password(cmd)
            raise exception.CoriolisException(
                "Command \"%s\" failed with exit code: %s\n"
                "stdout: %s\nstd_err: %s" % (sanitized_cmd, exit_code, stdout, stderr)
            )
        if include_stderr:
            return _strip_ps_output(stdout), stderr
        return _strip_ps_output(stdout)

    def test_path(self, remote_path):
        ret_val = self.exec_ps_command("Test-Path -Path \"%s\"" % remote_path)
        return ret_val == "True"

    def download_file(self, url, remote_path):
        LOG.debug(
            "Downloading: \"%(url)s\" to \"%(path)s\"",
            {"url": url, "path": remote_path},
        )
        try:
            self.exec_ps_command(
                "[Net.ServicePointManager]::SecurityProtocol = "
                "[Net.SecurityProtocolType]::Tls12;"
                "if(!([System.Management.Automation.PSTypeName]'"
                "System.Net.Http.HttpClient').Type) {$assembly = "
                "[System.Reflection.Assembly]::LoadWithPartialName("
                "'System.Net.Http')}; (new-object System.Net.Http.HttpClient)."
                "GetStreamAsync('%(url)s').Result.CopyTo("
                "(New-Object IO.FileStream '%(outfile)s', Create, Write, "
                "None), 1MB)" % {"url": url, "outfile": remote_path},
                ignore_stdout=True,
            )
        except exception.CoriolisException as ex:
            LOG.trace(utils.get_exception_details())
            raise exception.CoriolisException(
                "Failed to download file from URL: %s to path: %s. Please "
                "check logs for more details." % (url, remote_path)
            ) from ex

    def write_file(self, remote_path, content):
        self.exec_ps_command(
            "[IO.File]::WriteAllBytes('%s', [Convert]::FromBase64String('%s'))"
            % (remote_path, base64.b64encode(content).decode()),
            ignore_stdout=True,
        )
