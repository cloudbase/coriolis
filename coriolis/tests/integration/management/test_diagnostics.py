# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Integration tests for the diagnostics API.

Exercises diagnostics.get() via the Coriolis REST API.
"""

import socket

import netifaces

from coriolis import utils
from coriolis.tests.integration import base


class DiagnosticsTest(base.CoriolisIntegrationTestBase):
    def test_get_diagnostics(self):
        diag_list = self._client.diagnostics.get()

        # Returns a list of Diagnostics resources, one per service.
        self.assertIsInstance(diag_list, list)
        self.assertTrue(len(diag_list) > 0, "Expected at least one diagnostics entry")

        diag = diag_list[0]
        diag_ip_addr = diag.ip_addresses[0]
        ifname = list(diag_ip_addr.keys())[0]
        ip = netifaces.ifaddresses(ifname)[netifaces.AF_INET][0]["addr"]

        self.assertEqual(diag_ip_addr[ifname]["ipv4"][0]["addr"], ip)
        self.assertEqual(diag.os_info, utils._get_host_os_info())
        self.assertEqual(diag.hostname, socket.gethostname())

        self.assertEqual(diag.to_dict(), utils.get_diagnostics_info())
