# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Integration tests for the regions APIs.

Exercises region CRUD operations via the Coriolis REST API.
"""

from coriolis.tests.integration import base


class RegionTests(base.CoriolisIntegrationTestBase):

    def _create_region(self, name, **kwargs):
        region = self._client.regions.create(name, **kwargs)
        self.addCleanup(
            self._ignoreExc(self._client.regions.delete), region.id)

        return region

    def test_region_crud(self):
        # Create.
        region = self._create_region(
            "test-region", description="integration test region")

        # Get.
        fetched = self._client.regions.get(region.id)
        self.assertEqual(region.id, fetched.id)
        self.assertEqual("test-region", fetched.name)

        # List.
        regions = self._client.regions.list()
        ids = [r.id for r in regions]
        self.assertIn(region.id, ids)

        # Update.
        updated = self._client.regions.update(
            region.id, {"description": "updated"})
        self.assertEqual("updated", updated.description)

        # Delete.
        self._client.regions.delete(region.id)

        regions = self._client.regions.list()
        ids = [r.id for r in regions]
        self.assertNotIn(region.id, ids)
