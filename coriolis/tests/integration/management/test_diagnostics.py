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

        diag_ip_addr = None
        ifname = None
        for entry in diag.ip_addresses:
            name = list(entry.keys())[0]
            if entry[name]["ipv4"]:
                diag_ip_addr = entry
                ifname = name
                break

        self.assertIsNotNone(
            diag_ip_addr, "Expected at least one interface with an IPv4 address"
        )
        ip = netifaces.ifaddresses(ifname)[netifaces.AF_INET][0]["addr"]

        self.assertEqual(diag_ip_addr[ifname]["ipv4"][0]["addr"], ip)
        self.assertEqual(diag.os_info, utils._get_host_os_info())
        self.assertEqual(diag.hostname, socket.gethostname())

        actual = diag.to_dict()
        expected = utils.get_diagnostics_info()
        # Disk, memory, and CPU are sampled when each diagnostics payload is
        # built, so two reads are not identical.
        for key in ("filesystems", "memory", "cpu_usage"):
            self.assertIn(key, actual)
            actual.pop(key)
            expected.pop(key)

        self.assertEqual(actual, expected)
        self._assert_filesystems(diag.filesystems)
        self._assert_memory(diag.memory)
        self._assert_cpu_usage(diag.cpu_usage)

    def _assert_filesystems(self, filesystems):
        self.assertIsInstance(filesystems, list)
        self.assertTrue(filesystems, "Expected at least one filesystem")
        mounts = []
        for entry in filesystems:
            self.assertEqual(
                set(entry.keys()),
                {"filesystem", "size", "used", "available", "capacity", "mounted_on"},
            )
            self.assertIsInstance(entry["filesystem"], str)
            self.assertIsInstance(entry["mounted_on"], str)
            for field in ("size", "used", "available", "capacity"):
                self.assertIsInstance(entry[field], int)
            mounts.append(entry["mounted_on"])
        self.assertIn("/", mounts)

    def _assert_memory(self, memory):
        self.assertEqual(
            set(memory.keys()),
            {"total", "used", "free", "shared", "buff_cache", "available", "swap"},
        )
        for field in ("total", "used", "free", "shared", "buff_cache", "available"):
            self.assertIsInstance(memory[field], int)
        self.assertGreater(memory["total"], 0)
        self.assertEqual(set(memory["swap"].keys()), {"total", "used", "free"})
        for field in ("total", "used", "free"):
            self.assertIsInstance(memory["swap"][field], int)

    def _assert_cpu_usage(self, cpu_usage):
        self.assertIsInstance(cpu_usage, list)
        self.assertTrue(cpu_usage, "Expected at least one CPU core")
        cores = []
        for entry in cpu_usage:
            self.assertEqual(set(entry.keys()), {"core", "percent"})
            self.assertIsInstance(entry["core"], int)
            self.assertIsInstance(entry["percent"], (int, float))
            self.assertGreaterEqual(entry["percent"], 0)
            cores.append(entry["core"])
        self.assertEqual(cores, list(range(len(cpu_usage))))
