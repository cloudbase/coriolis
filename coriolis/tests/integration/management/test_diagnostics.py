# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Integration tests for the diagnostics API.

Exercises diagnostics.get() via the Coriolis REST API.
"""

import netifaces
import socket

from coriolis.tests.integration import base
from coriolis import utils


class DiagnosticsTest(base.CoriolisIntegrationTestBase):

    def test_get_diagnostics(self):
        diag_list = self._client.diagnostics.get()

        # Returns a list of Diagnostics resources, one per service.
        self.assertIsInstance(diag_list, list)
        self.assertTrue(
            len(diag_list) > 0, "Expected at least one diagnostics entry")

        diag = diag_list[0]

        diag_ip_addr = None
        ifname = None
        for entry in diag.ip_addresses:
            name = list(entry.keys())[0]
            if entry[name]["ipv4"]:
                diag_ip_addr = entry
                ifname = name
                break

        self.assertIsNotNone(
            diag_ip_addr, "Expected at least one interface with an IPv4 "
            "address")
        ip = netifaces.ifaddresses(ifname)[netifaces.AF_INET][0]["addr"]

        self.assertEqual(diag_ip_addr[ifname]["ipv4"][0]["addr"], ip)
        self.assertEqual(diag.os_info, utils._get_host_os_info())
        self.assertEqual(diag.hostname, socket.gethostname())

        self.assertEqual(diag.to_dict(), utils.get_diagnostics_info())
