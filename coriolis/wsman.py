# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import base64

from oslo_log import log as logging
import requests
from winrm import protocol
from winrm import exceptions as winrm_exceptions

from coriolis import exception
from coriolis import utils

AUTH_BASIC = "basic"
AUTH_KERBEROS = "kerberos"
AUTH_CERTIFICATE = "certificate"

CODEPAGE_UTF8 = 65001
DEFAULT_TIMEOUT = 3600

LOG = logging.getLogger(__name__)


class WSManConnection(object):
    def __init__(self, timeout=None):
        self._protocol = None
        self._conn_timeout = int(timeout or DEFAULT_TIMEOUT)

    EOL = "\r\n"

    @utils.retry_on_error()
    def connect(self, url, username, auth=None, password=None,
                cert_pem=None, cert_key_pem=None):
        if not auth:
            if cert_pem:
                auth = AUTH_CERTIFICATE
            else:
                auth = AUTH_BASIC

        auth_transport_map = {AUTH_BASIC: 'plaintext',
                              AUTH_KERBEROS: 'kerberos',
                              AUTH_CERTIFICATE: 'ssl'}

        self._protocol = protocol.Protocol(
            endpoint=url,
            transport=auth_transport_map[auth],
            username=username,
            password=password,
            cert_pem=cert_pem,
            cert_key_pem=cert_key_pem)

    @classmethod
    def from_connection_info(cls, connection_info, timeout=DEFAULT_TIMEOUT):
        """ Returns a wsman.WSManConnection object for the provided conn info. """
        if not isinstance(connection_info, dict):
            raise ValueError(
                "WSMan connection must be a dict. Got type '%s', value: %s" % (
                    type(connection_info), connection_info))

        required_keys = ["ip", "username", "password"]
        missing = [key for key in required_keys if key not in connection_info]
        if missing:
            raise ValueError(
                "The following keys were missing from WSMan connection info %s. "
                "Got: %s" % (missing, connection_info))

        host = connection_info["ip"]
        port = connection_info.get("port", 5986)
        username = connection_info["username"]
        password = connection_info.get("password")
        cert_pem = connection_info.get("cert_pem")
        cert_key_pem = connection_info.get("cert_key_pem")
        url = "https://%s:%s/wsman" % (host, port)

        LOG.info("Connection info: %s", str(connection_info))

        LOG.info("Waiting for connectivity on host: %(host)s:%(port)s",
                 {"host": host, "port": port})
        utils.wait_for_port_connectivity(host, port)

        conn = cls(timeout)
        conn.connect(url=url, username=username, password=password,
                     cert_pem=cert_pem, cert_key_pem=cert_key_pem)

        return conn

    def disconnect(self):
        self._protocol = None

    def set_timeout(self, timeout):
        if timeout:
            self._protocol.timeout = timeout
            self._protocol.transport.timeout = timeout

    @utils.retry_on_error(
        terminal_exceptions=[winrm_exceptions.InvalidCredentialsError,
                             exception.OSMorphingWinRMOperationTimeout])
    def _exec_command(self, cmd, args=[], timeout=None):
        timeout = int(timeout or self._conn_timeout)
        self.set_timeout(timeout)
        shell_id = self._protocol.open_shell(codepage=CODEPAGE_UTF8)
        try:
            command_id = self._protocol.run_command(shell_id, cmd, args)
            try:
                (std_out,
                 std_err,
                 exit_code) = self._protocol.get_command_output(
                    shell_id, command_id)
            except requests.exceptions.ReadTimeout:
                raise exception.OSMorphingWinRMOperationTimeout(
                    cmd=("%s %s" % (cmd, " ".join(args))), timeout=timeout)
            finally:
                self._protocol.cleanup_command(shell_id, command_id)

            return (std_out, std_err, exit_code)
        finally:
            self._protocol.close_shell(shell_id)

    def exec_command(self, cmd, args=[], timeout=None):
        LOG.debug("Executing WSMAN command: %s", str([cmd] + args))
        std_out, std_err, exit_code = self._exec_command(
            cmd, args, timeout=timeout)

        if exit_code:
            raise exception.CoriolisException(
                "Command \"%s\" failed with exit code: %s\n"
                "stdout: %s\nstd_err: %s" %
                (str([cmd] + args), exit_code, std_out, std_err))

        return std_out

    def exec_ps_command(self, cmd, ignore_stdout=False, timeout=None):
        LOG.debug("Executing PS command: %s", cmd)
        base64_cmd = base64.b64encode(cmd.encode('utf-16le')).decode()
        return self.exec_command(
            "powershell.exe", ["-EncodedCommand", base64_cmd],
            timeout=timeout)[:-2]

    def test_path(self, remote_path):
        ret_val = self.exec_ps_command("Test-Path -Path \"%s\"" % remote_path)
        return ret_val == "True"

    def download_file(self, url, remote_path):
        LOG.debug("Downloading: \"%(url)s\" to \"%(path)s\"",
                  {"url": url, "path": remote_path})
        # Nano Server does not have Invoke-WebRequest and additionally
        # this is also faster
        self.exec_ps_command(
            "[Net.ServicePointManager]::SecurityProtocol = "
            "[Net.SecurityProtocolType]::Tls12;"
            "if(!([System.Management.Automation.PSTypeName]'"
            "System.Net.Http.HttpClient').Type) {$assembly = "
            "[System.Reflection.Assembly]::LoadWithPartialName("
            "'System.Net.Http')}; (new-object System.Net.Http.HttpClient)."
            "GetStreamAsync('%(url)s').Result.CopyTo("
            "(New-Object IO.FileStream '%(outfile)s', Create, Write, None), "
            "1MB)" % {"url": url, "outfile": remote_path},
            ignore_stdout=True)

    def write_file(self, remote_path, content):
        self.exec_ps_command(
            "[IO.File]::WriteAllBytes('%s', [Convert]::FromBase64String('%s'))"
            % (remote_path, base64.b64encode(content).decode()),
            ignore_stdout=True)
