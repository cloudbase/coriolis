# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import base64

from oslo_log import log as logging
from winrm import protocol
from winrm import exceptions as winrm_exceptions

from coriolis import exception
from coriolis import utils

AUTH_BASIC = "basic"
AUTH_KERBEROS = "kerberos"
AUTH_CERTIFICATE = "certificate"

CODEPAGE_UTF8 = 65001

LOG = logging.getLogger(__name__)


class WSManConnection(object):
    def __init__(self):
        self._protocol = None

    EOL = "\r\n"

    @utils.retry_on_error()
    def connect(self, url, username, auth=None, password=None,
                cert_pem=None, cert_key_pem=None):
        protocol.Protocol.DEFAULT_TIMEOUT = 3600

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

    def disconnect(self):
        self._protocol = None

    @utils.retry_on_error(
        terminal_exceptions=[winrm_exceptions.InvalidCredentialsError])
    def _exec_command(self, cmd, args=[]):
        shell_id = self._protocol.open_shell(codepage=CODEPAGE_UTF8)
        try:
            command_id = self._protocol.run_command(shell_id, cmd, args)
            try:
                (std_out,
                 std_err,
                 exit_code) = self._protocol.get_command_output(
                    shell_id, command_id)
            finally:
                self._protocol.cleanup_command(shell_id, command_id)

            return (std_out, std_err, exit_code)
        finally:
            self._protocol.close_shell(shell_id)

    def exec_command(self, cmd, args=[]):
        LOG.debug("Executing WSMAN command: %s", str([cmd] + args))
        std_out, std_err, exit_code = self._exec_command(cmd, args)

        if exit_code:
            raise exception.CoriolisException(
                "Command \"%s\" failed with exit code: %s\n"
                "stdout: %s\nstd_err: %s" %
                (str([cmd] + args), exit_code, std_out, std_err))

        return std_out

    def exec_ps_command(self, cmd, ignore_stdout=False):
        LOG.debug("Executing PS command: %s", cmd)
        base64_cmd = base64.b64encode(cmd.encode('utf-16le')).decode()
        return self.exec_command(
            "powershell.exe", ["-EncodedCommand", base64_cmd])[:-2]

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
