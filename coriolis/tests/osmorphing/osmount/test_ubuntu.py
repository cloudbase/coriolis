# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.osmorphing.osmount import ubuntu
from coriolis.tests import test_base


class UbuntuOSMountToolsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the UbuntuOSMountTools class."""

    @mock.patch.object(ubuntu.base.BaseSSHOSMountTools, '_connect')
    def setUp(self, mock_connect):
        super(UbuntuOSMountToolsTestCase, self).setUp()
        self.ssh = mock.MagicMock()

        self.tools = ubuntu.UbuntuOSMountTools(
            self.ssh,
            mock.sentinel.event_manager,
            mock.sentinel.ignore_devices,
            mock.sentinel.operation_timeout,
        )

        mock_connect.assert_called_once_with()

        self.tools._ssh = self.ssh

    @mock.patch.object(ubuntu.utils, 'get_linux_os_info')
    def test_check_os(self, mock_get_linux_os_info):
        mock_get_linux_os_info.return_value = ['Ubuntu']

        result = self.tools.check_os()
        self.assertTrue(result)

    @mock.patch.object(ubuntu.base.BaseSSHOSMountTools, '_exec_sudo_env_cmd')
    @mock.patch.object(ubuntu.base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(ubuntu.base.BaseSSHOSMountTools, 'setup')
    def test_setup(self, mock_setup, mock_exec_cmd, mock_exec_sudo_env_cmd):
        result = self.tools.setup()
        self.assertIsNone(result)

        mock_setup.assert_called_once_with()
        # NOTE: the apt-get calls must go through '_exec_sudo_env_cmd' so
        # that any configured proxy reaches apt without relying on 'sudo -E',
        # which sudo-rs (the default on Ubuntu 26.04) does not support.
        mock_exec_sudo_env_cmd.assert_has_calls(
            [
                mock.call("apt-get update -y"),
                mock.call(
                    "apt-get -o DPkg::Lock::Timeout=600 "
                    "install lvm2 psmisc cryptsetup -y"
                ),
            ]
        )
        # NOTE: cryptsetup pulls in keyboard-configuration, whose postinst
        # would otherwise prompt for a keyboard layout and hang the install.
        self.assertEqual('noninteractive', self.tools._environment['DEBIAN_FRONTEND'])
        mock_exec_cmd.assert_has_calls(
            [mock.call("sudo modprobe dm-mod"), mock.call("sudo modprobe dm-crypt")]
        )

    @mock.patch.object(ubuntu.utils, 'exec_ssh_cmd')
    @mock.patch.object(ubuntu.base.BaseSSHOSMountTools, 'setup')
    def test_setup_propagates_proxy_to_apt(self, mock_setup, mock_exec_ssh):
        """End-to-end check of proxy propagation on an Ubuntu worker."""
        proxy = "http://10.0.0.1:3128"
        self.tools.set_proxy({'url': proxy})

        self.tools.setup()

        apt_cmds = [
            call[0][1]
            for call in mock_exec_ssh.call_args_list
            if "apt-get" in call[0][1]
        ]
        self.assertEqual(2, len(apt_cmds))
        for cmd in apt_cmds:
            self.assertTrue(
                cmd.startswith("sudo env "),
                "apt-get command is missing its env prefix: %s" % cmd,
            )
            for var in (
                'http_proxy',
                'HTTP_PROXY',
                'https_proxy',
                'HTTPS_PROXY',
                'ftp_proxy',
                'FTP_PROXY',
            ):
                self.assertIn("%s=%s" % (var, proxy), cmd)

    @mock.patch.object(ubuntu.base.BaseSSHOSMountTools, '_exec_cmd')
    @mock.patch.object(ubuntu.utils, 'restart_service')
    def test__allow_ssh_env_vars(self, mock_restart_service, mock_exec_cmd):
        result = self.tools._allow_ssh_env_vars()
        self.assertTrue(result)

        mock_exec_cmd.assert_called_once_with(
            'sudo sed -i -e "\$aAcceptEnv *" /etc/ssh/sshd_config'
        )
        mock_restart_service.assert_called_once_with(self.ssh, "sshd")
