# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Integration tests for the regions APIs.

Exercises region CRUD operations via the Coriolis REST API, as well as
scheduler behavior when workers and endpoints are mapped to regions.
"""

from keystoneauth1.exceptions import http as http_exc

from coriolis import constants
from coriolis.tests.integration import base
from coriolis import utils as coriolis_utils


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


class RegionSchedulingTests(base.ReplicaIntegrationTestBase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._worker_service = (
            cls._client.services.find_service_by_host_and_topic(
                coriolis_utils.get_hostname(),
                constants.WORKER_MAIN_MESSAGING_TOPIC))
        cls.addClassCleanup(
            cls._client.services.update,
            cls._worker_service.id, {"mapped_regions": []})

    def _create_region(self, name, **kwargs):
        region = self._client.regions.create(name, **kwargs)
        self.addCleanup(
            self._ignoreExc(self._client.regions.delete), region.id)

        return region

    def test_region_scheduling(self):
        """Covers the scheduler's RegionsFilter.

        A matched-region transfer schedules and completes normally, while
        a worker / endpoint region mismatch causes the transfer creation
        itself to fail.
        The scheduler needs a region-matching worker to service the endpoint
        validation calls for the created transfer. A region mismatch surfaces
        as an immediate BadRequest at creation time.
        """
        # Matched region: reuse the src / dst endpoints and transfer created in
        # ReplicaIntegrationTestBase.setUp.
        matched_region = self._create_region("region-matched")
        self._client.services.update(
            self._worker_service.id, {"mapped_regions": [matched_region.id]})
        self._client.endpoints.update(
            self._src_endpoint.id, {"mapped_regions": [matched_region.id]})
        self._client.endpoints.update(
            self._dst_endpoint.id, {"mapped_regions": [matched_region.id]})

        self._execute_and_wait(self._transfer.id)

        # Mismatched region: point the worker and a fresh pair of
        # endpoints at different regions, and assert transfer creation
        # itself is rejected.
        worker_region = self._create_region("region-worker")
        endpoint_region = self._create_region("region-endpoint")

        self._client.services.update(
            self._worker_service.id, {"mapped_regions": [worker_region.id]})

        src_endpoint = self._create_endpoint(
            name="region-mismatch-src",
            endpoint_type=self._exp_platform,
            connection_info=self._exp_conn_info,
            regions=[endpoint_region.id],
        )
        dst_endpoint = self._create_endpoint(
            name="region-mismatch-dst",
            endpoint_type=self._imp_platform,
            connection_info=self._imp_conn_info,
            regions=[endpoint_region.id],
        )

        self.assertRaises(
            http_exc.BadRequest,
            self._create_transfer,
            src_endpoint.id, dst_endpoint.id,
            ["region-mismatch-instance"],
        )
