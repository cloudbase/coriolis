# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Convert WinRM native (cmd, args) pairs to an OpenSSH command line.

Osmorphing builds native tool argv for WinRM. WinRM runs argv as-is.
SSH sends one string. OpenSSH DefaultShell is often PowerShell, so the
converter wraps cmd.exe /c and quotes for cmd.exe.

WinRM arguments may contain quotes that were only argv delimiters, such
as /driver:"G:\\path". Those quotes must not stay in the DISM path.
"""

_CMD_QUOTE_CHARS = (" ", "\t", '"', "&", "|", "(", ")", "<", ">", "^", "%")


def unwrap_winrm_arg_quotes(part):
    """Remove WinRM argv quotes from one native argument."""
    text = str(part).strip()
    changed = True
    while changed and text:
        changed = False
        if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
            text = text[1:-1]
            changed = True
            continue
        idx = text.find(':"')
        if idx >= 0 and text.endswith('"'):
            text = text[: idx + 1] + text[idx + 2 : -1]
            changed = True
    return text


def quote_cmd_arg(part):
    part_str = unwrap_winrm_arg_quotes(part)
    if (not part_str) or any(ch in part_str for ch in _CMD_QUOTE_CHARS):
        return '"%s"' % part_str.replace('"', '""')
    return part_str


def format_windows_command(cmd, args):
    return " ".join(quote_cmd_arg(p) for p in [cmd] + list(args or []))


def escape_trailing_backslash_for_ssh(command):
    """Keep a trailing backslash from eating the SSH quote.

    Windows OpenSSH wraps the exec string in double quotes. An odd number
    of trailing backslashes escapes that quote. PowerShell then reports a
    missing string terminator. DISM /image:F:\\ is the usual case.
    """
    n = len(command) - len(command.rstrip("\\"))
    if n % 2 == 1:
        return command + "\\"
    return command


def wrap_native_command_for_ssh(command):
    """Run native tools via cmd.exe.

    OpenSSH DefaultShell is often PowerShell. PowerShell parses (OI) in
    icacls grants as a command. cmd.exe does not when the grant is quoted.
    """
    command = escape_trailing_backslash_for_ssh(command)
    return 'cmd.exe /c "%s"' % command.replace('"', '""')


def winrm_exec_to_ssh(cmd, args=None):
    """Return the SSH exec string for a WinRM (cmd, args) pair."""
    formatted = format_windows_command(cmd, args)
    wrapped = wrap_native_command_for_ssh(formatted)
    return escape_trailing_backslash_for_ssh(wrapped)
