# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Smoke tests for the integration harness itself.

Verifies that the in-process service stack (API, conductor, scheduler, worker)
initialises correctly and that basic resource CRUD works end-to-end, without
any cloud provider, block device, or scsi_debug involvement.
"""

from coriolis.tests.integration import base


class HarnessSmokeTest(base.CoriolisIntegrationTestBase):
    """Smoke tests: API accessible, conductor alive, DB reachable."""

    def test_api_is_accessible(self):
        """REST API starts and returns a list for an empty project."""
        endpoints = self._client.endpoints.list()
        self.assertIsInstance(endpoints, list)

    def test_endpoint_crud(self):
        """Endpoint create / get exercises."""
        endpoint = self._create_endpoint(
            name="smoke-test-endpoint",
            description="harness smoke test",
            connection_info={"key": "value"},
        )

        found = self._client.endpoints.get(endpoint.id)
        self.assertEqual(endpoint.id, found.id)
        self.assertEqual("smoke-test-endpoint", found.name)

    def test_transfer_create(self):
        """Transfer creation persists correctly without starting execution."""

        src = self._create_endpoint(
            name="smoke-src",
            connection_info={"foo": "lish"},
        )

        dst = self._create_endpoint(
            name="smoke-dst",
            connection_info={"bar": "tender"},
        )

        instances = ["smoke-instance"]
        transfer = self._create_transfer(src.id, dst.id, instances=instances)

        self.assertEqual(src.id, transfer.origin_endpoint_id)
        self.assertEqual(dst.id, transfer.destination_endpoint_id)
        self.assertEqual(["smoke-instance"], transfer.instances)
