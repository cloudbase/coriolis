# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

import base64

from coriolis import exception, windows_minion
from coriolis.tests import test_base


class WindowsMinionTestCase(test_base.CoriolisBaseTestCase):
    def test_script_opens_ssh_on_every_profile(self):
        script = windows_minion.build_windows_openssh_script("ssh-rsa AAAAB3 key")

        self.assertNotIn("#ps1_sysnative", script)
        self.assertNotIn("net.exe user", script)
        self.assertIn("Set-NetFirewallRule", script)
        self.assertIn("-Profile Any", script)
        self.assertIn("-EdgeTraversalPolicy Allow", script)
        self.assertIn("ssh-rsa AAAAB3 key", script)
        self.assertIn("$sshPort = 22", script)
        self.assertNotIn("__CORIOLIS_PUBLIC_KEY__", script)
        self.assertNotIn("__CORIOLIS_SSH_PORT__", script)
        self.assertNotIn("__CORIOLIS_ACCOUNT_LINE__", script)

    def test_cloudbase_init_script_sets_password(self):
        script = windows_minion.build_windows_openssh_script(
            "ssh-rsa AAAAB3 key",
            username="Administrator",
            password="generated-pass",
            cloudbase_init=True,
        )

        self.assertTrue(script.startswith("#ps1_sysnative\n"))
        self.assertIn("net.exe user 'Administrator' 'generated-pass'", script)
        self.assertIn("ssh-rsa AAAAB3 key", script)

    def test_account_line_quotes_password_literally(self):
        script = windows_minion.build_windows_openssh_script(
            "ssh-rsa AAAAB3 key",
            username="O'Hara",
            password="p@ss'$env:USERNAME",
        )

        self.assertIn("net.exe user 'O''Hara' 'p@ss''$env:USERNAME'", script)
        self.assertIn("administrators_authorized_keys", script)

    def test_custom_port_is_written_into_the_script(self):
        script = windows_minion.build_windows_openssh_script(
            "ssh-rsa AAAAB3 key", port=2222
        )

        self.assertIn("$sshPort = 2222", script)
        self.assertIn("Set-NetFirewallPortFilter -LocalPort $sshPort", script)
        self.assertNotIn("$sshPort = 22\n", script)

    def test_script_rejects_an_invalid_port(self):
        self.assertRaises(
            exception.InvalidInput,
            windows_minion.build_windows_openssh_script,
            "ssh-rsa AAAAB3 key",
            port=0,
        )
        self.assertRaises(
            exception.InvalidInput,
            windows_minion.build_windows_openssh_script,
            "ssh-rsa AAAAB3 key",
            port=65536,
        )

    def test_partial_account_skips_the_password_line(self):
        username_only = windows_minion.build_windows_openssh_script(
            "ssh-rsa AAAAB3 key",
            username="ops",
        )
        password_only = windows_minion.build_windows_openssh_script(
            "ssh-rsa AAAAB3 key",
            password="generated-pass",
        )

        self.assertNotIn("net.exe user", username_only)
        self.assertNotIn("net.exe user", password_only)
        self.assertIn("administrators_authorized_keys", username_only)

    def test_agent_command_decodes_to_script(self):
        command = windows_minion.windows_openssh_agent_command("ssh-rsa AAAAB3 key")

        prefix = "powershell.exe -NoProfile -NonInteractive -EncodedCommand "
        self.assertTrue(command.startswith(prefix))
        encoded = command[len(prefix) :]
        script = base64.b64decode(encoded).decode("utf-16le")
        self.assertEqual(
            windows_minion.build_windows_openssh_script("ssh-rsa AAAAB3 key"),
            script,
        )
