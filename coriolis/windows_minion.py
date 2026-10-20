# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Shared Windows minion OpenSSH setup.

The stock OpenSSH firewall rule is Private and blocks edge traversal.
An unidentified NIC does not use the Private profile.
A NAT address needs edge traversal Allow.
The rule must use every profile.

This module only returns the guest script.
The provider must run the script.
The provider must open TCP 22 on the platform firewall.
Core uses SSH when connection_info port is not 5986.
Port 5986, or a missing port, still uses WinRM.

Providers that call this module
-------------------------------
OpenStack.
Nova user data.
cloudbase-init runs the script because it starts with #ps1_sysnative.
The image needs UserDataPlugin.

CloudStack.
deployVirtualMachine user data, base64-encoded by the provider.
Same #ps1_sysnative path as OpenStack.
The script also sets the Administrator password with net user.
The provider opens TCP 22 on the minion public IP.

Proxmox.
QEMU guest agent.
windows_openssh_agent_command() sends powershell.exe -EncodedCommand.
The script has no #ps1_sysnative header.
The template needs the guest agent.
powershell.exe must be on the agent PATH.
The provider sets the password with a separate net user command.

Providers that can call this module
------------------------------------
LXD.
The worker already sets cloud-init user data.
The current text is #cloud-config with ssh_authorized_keys.
That text does not start sshd.
That text does not change the firewall profile.
Replace that text with this script.

libvirt.
The minion config drive already carries user data.
The current text is #cloud-config.
The connection port is 5986.
Send this script and set the port to 22.

STACKIT.
Cloud-init user data already runs a PowerShell script.
That script configures a WinRM listener.
The connection port is 5986.
Run this script instead, and set the port to 22.

AWS.
EC2 runs user data that starts with <powershell>.
It does not run a #ps1_sysnative script.
Use the script body with an <powershell> header.
The current Windows user data opens WinRM on port 5986.

oVirt.
Use this script only when the Windows template runs cloudbase-init.
The provider already allows that initialization path.
windows_openssh_agent_command() does not apply.
The oVirt guest agent reports the IP.
It does not run a QEMU guest-exec command.
A template with only a preset WinRM password has no channel for this script.

VMware vSphere.
OS morphing clones the Windows template from migr_template_map.
The connection is TCP port 22.
The username and password come from the template maps.
The clone spec does not set user data.
The clone spec does not run a guest command.
The template must already accept that login on port 22.
VMware Tools can run the script with StartProgramInGuest.
The provider does not call that API today.
The backup writer still accepts only a Linux minion.
That check is for disk copy, not for the OS morphing minion.
"""

import base64

from coriolis import exception

# Placeholders are replaced by build_windows_openssh_script.
_OPENSSH_SCRIPT = """$ErrorActionPreference = 'Stop'
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
$ruleName = 'OpenSSH-Server-In-TCP'
$rule = Get-NetFirewallRule -Name $ruleName -ErrorAction SilentlyContinue
if (-not $rule) {
    New-NetFirewallRule -Name $ruleName `
        -DisplayName 'OpenSSH Server (sshd)' -Enabled True `
        -Direction Inbound -Protocol TCP -Action Allow -LocalPort 22 |
        Out-Null
}
Set-NetFirewallRule -Name $ruleName -Enabled True -Profile Any `
    -EdgeTraversalPolicy Allow
$keyDir = Join-Path $env:ProgramData 'ssh'
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


def build_windows_openssh_script(
    public_key, username=None, password=None, cloudbase_init=False
):
    """Return the PowerShell script that starts sshd and opens TCP 22.

    Set both username and password to add a ``net user`` line.
    Set cloudbase_init to prefix ``#ps1_sysnative``.
    The password must not contain spaces.
    """
    if (username is None) != (password is None):
        raise exception.InvalidInput(
            "Windows OpenSSH script needs both username and password, or neither."
        )
    account_line = ""
    if username is not None:
        account_line = "net.exe user %s %s" % (username, password)
    script = _OPENSSH_SCRIPT.replace(
        "__CORIOLIS_ACCOUNT_LINE__\n",
        (account_line + "\n") if account_line else "",
    ).replace("__CORIOLIS_PUBLIC_KEY__", public_key)
    if cloudbase_init:
        script = "#ps1_sysnative\n" + script
    return script


def windows_openssh_agent_command(public_key):
    """Return a guest-agent command that starts Windows OpenSSH."""
    script = build_windows_openssh_script(public_key)
    encoded = base64.b64encode(script.encode("utf-16le")).decode("ascii")
    return "powershell.exe -NoProfile -NonInteractive -EncodedCommand " + encoded
