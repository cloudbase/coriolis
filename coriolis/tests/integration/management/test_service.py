# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Integration tests for the services APIs.

Exercises service CRUD operations via the Coriolis REST API.
"""

import socket

from keystoneauth1.exceptions import http as http_exc

from coriolis.tests.integration import base


class ServiceTests(base.CoriolisIntegrationTestBase):
    def _create_service(self, host, binary, topic):
        svc = self._client.services.create(
            host=host, binary=binary, topic=topic, regions=[]
        )

        self.addCleanup(self._ignoreExc(self._client.services.delete), svc.id)

        return svc

    def test_service_crud(self):
        # The harness starts an in-process worker which registers itself with
        # the conductor; at least one service record should exist.
        services = self._client.services.list()

        self.assertTrue(len(services) > 0, "Expected at least one registered service")

        # Create.
        hostname = socket.gethostname()
        svc = self._create_service(hostname, "foo-binary", "lish-topic")

        # Get.
        fetched = self._client.services.get(svc.id)
        self.assertEqual(svc.id, fetched.id)
        self.assertEqual(hostname, fetched.host)
        self.assertEqual("foo-binary", fetched.binary)

        # List.
        services = self._client.services.list()
        ids = [s.id for s in services]
        self.assertIn(svc.id, ids)

        # Update.
        updated = self._client.services.update(svc.id, {"enabled": False})
        self.assertFalse(updated.enabled)

        # Delete.
        self._client.services.delete(svc.id)

        services = self._client.services.list()
        ids = [s.id for s in services]
        self.assertNotIn(svc.id, ids)
        self.assertRaises(http_exc.NotFound, self._client.services.get, svc.id)

    def test_service_registration_conflict(self):
        # ConductorServerEndpoint.register_service raises Conflict when a
        # service with the same host / binary / topic is already registered.
        hostname = socket.gethostname()
        svc = self._create_service(hostname, "conflict-binary", "conflict-topic")

        self.assertRaises(
            http_exc.Conflict,
            self._client.services.create,
            host=hostname,
            binary="conflict-binary",
            topic="conflict-topic",
            regions=[],
        )

        self._client.services.delete(svc.id)

    def test_service_registration_with_region(self):
        # Creates a service with a mapped region.
        region = self._client.regions.create("service-test-region")
        self.addCleanup(self._ignoreExc(self._client.regions.delete), region.id)

        hostname = socket.gethostname()
        svc = self._client.services.create(
            host=hostname,
            binary="region-binary",
            topic="region-topic",
            regions=[region.id],
        )
        self.addCleanup(self._ignoreExc(self._client.services.delete), svc.id)

        fetched = self._client.services.get(svc.id)
        self.assertIn(region.id, fetched.mapped_regions)
