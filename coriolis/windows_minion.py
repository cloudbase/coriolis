# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Shared Windows minion OpenSSH setup.

The stock OpenSSH firewall rule is Private and blocks edge traversal.
An unidentified NIC does not use the Private profile.
A NAT address needs edge traversal Allow.
The rule must use every profile.

This module only returns the guest script.
The provider must run the script.
The provider must open the SSH port on the platform firewall.
The provider reads [windows_minion] protocol.
An unset value keeps WinRM.
The provider sets connection_info protocol and port.
"""

import base64

from coriolis import exception, windows_conn

# Placeholders are replaced by build_windows_openssh_script.
_OPENSSH_SCRIPT = r"""$ErrorActionPreference = 'Stop'
__CORIOLIS_ACCOUNT_LINE__
$svc = Get-Service -Name sshd -ErrorAction SilentlyContinue
if (-not $svc) {
    $cap = Get-WindowsCapability -Online |
        Where-Object { $_.Name -like 'OpenSSH.Server*' } |
        Select-Object -First 1
    if (-not $cap) { throw 'OpenSSH Server capability is not available' }
    Add-WindowsCapability -Online -Name $cap.Name | Out-Null
}
Set-Service -Name sshd -StartupType Automatic
Start-Service -Name sshd
$sshPort = __CORIOLIS_SSH_PORT__
if ($sshPort -ne 22) {
    $configPath = Join-Path $env:ProgramData 'ssh\sshd_config'
    if (-not (Test-Path $configPath)) {
        throw 'sshd_config is missing. OpenSSH did not create it.'
    }
    $lines = @(Get-Content -Path $configPath)
    $out = New-Object System.Collections.Generic.List[string]
    $set = $false
    foreach ($line in $lines) {
        if (-not $set -and $line -match '^\s*Match\s') {
            $out.Add("Port $sshPort")
            $set = $true
        }
        if (-not $set -and $line -match '^\s*Port\s+\d+\s*$') {
            $out.Add("Port $sshPort")
            $set = $true
            continue
        }
        $out.Add($line)
    }
    if (-not $set) { $out.Insert(0, "Port $sshPort") }
    Set-Content -Path $configPath -Value $out -Encoding ascii
    Restart-Service -Name sshd
}
$ruleName = 'OpenSSH-Server-In-TCP'
$rule = Get-NetFirewallRule -Name $ruleName -ErrorAction SilentlyContinue
if (-not $rule) {
    New-NetFirewallRule -Name $ruleName `
        -DisplayName 'OpenSSH Server (sshd)' -Enabled True `
        -Direction Inbound -Protocol TCP -Action Allow -LocalPort $sshPort |
        Out-Null
}
Set-NetFirewallRule -Name $ruleName -Enabled True -Profile Any `
    -EdgeTraversalPolicy Allow
Get-NetFirewallRule -Name $ruleName | Get-NetFirewallPortFilter |
    Set-NetFirewallPortFilter -LocalPort $sshPort
$keyDir = Join-Path $env:ProgramData 'ssh'
# This file applies to every member of the Administrators group.
$keyFile = Join-Path $keyDir 'administrators_authorized_keys'
New-Item -ItemType Directory -Force -Path $keyDir | Out-Null
$key = @'
__CORIOLIS_PUBLIC_KEY__
'@
$existing = ''
if (Test-Path $keyFile) { $existing = Get-Content -Raw -Path $keyFile }
if ($existing -notlike ('*' + $key + '*')) {
    Add-Content -Path $keyFile -Value $key
}
icacls.exe $keyFile /inheritance:r /grant 'Administrators:F' /grant 'SYSTEM:F' |
    Out-Null
"""


def _ssh_port(port):
    """Return a TCP port from 1 to 65535."""
    try:
        value = int(port)
    except (TypeError, ValueError) as ex:
        raise exception.InvalidInput(
            "Windows OpenSSH port must be an integer from 1 to 65535."
        ) from ex
    if value < 1 or value > 65535:
        raise exception.InvalidInput(
            "Windows OpenSSH port must be an integer from 1 to 65535. Got: %s" % value
        )
    return value


def build_windows_openssh_script(
    public_key,
    username=None,
    password=None,
    cloudbase_init=False,
    port=windows_conn.DEFAULT_PROTOCOL_PORTS[windows_conn.CONNECTION_SSH],
):
    """Return the PowerShell script that starts sshd and opens the SSH port.

    The default port is the SSH entry in ``DEFAULT_PROTOCOL_PORTS``.
    Another port is written to sshd_config.
    Set both username and password to add a ``net user`` line.
    That line sets the password of the given account.
    The account must be in the Administrators group.
    The public key goes to administrators_authorized_keys.
    Every member of that group can use the key.
    One of username or password leaves the password line out.
    The username and the password are single-quoted.
    Set cloudbase_init to prefix ``#ps1_sysnative``.
    """
    account_line = ""
    if username and password:
        account_line = "net.exe user '%s' '%s'" % (
            str(username).replace("'", "''"),
            str(password).replace("'", "''"),
        )
    script = (
        _OPENSSH_SCRIPT.replace(
            "__CORIOLIS_ACCOUNT_LINE__\n",
            (account_line + "\n") if account_line else "",
        )
        .replace("__CORIOLIS_PUBLIC_KEY__", public_key)
        .replace("__CORIOLIS_SSH_PORT__", str(_ssh_port(port)))
    )
    if cloudbase_init:
        script = "#ps1_sysnative\n" + script
    return script


def windows_openssh_agent_command(
    public_key,
    port=windows_conn.DEFAULT_PROTOCOL_PORTS[windows_conn.CONNECTION_SSH],
):
    """Return a guest-agent command that starts Windows OpenSSH."""
    script = build_windows_openssh_script(public_key, port=port)
    encoded = base64.b64encode(script.encode("utf-16le")).decode("ascii")
    return "powershell.exe -NoProfile -NonInteractive -EncodedCommand " + encoded
