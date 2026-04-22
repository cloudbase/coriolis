# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Integration tests for endpoint capability APIs.

Exercises endpoint-related operations via the Coriolis REST API:
- validate_connection (success and failure)
- endpoint update
- get_networks
- get_storage (list and default)
- get_source_environment_options
- get_target_environment_options
- endpoint_instances.list and endpoint_instances.get
"""

from coriolis.tests.integration import base


class EndpointCapabilitiesTest(base.CoriolisIntegrationTestBase):

    def setUp(self):
        super().setUp()
        # /dev/null satisfies os.path.exists checks in validate_connection.
        self._src_endpoint = self._create_endpoint(
            name="cap-src",
            endpoint_type="test-src",
            connection_info={
                "block_device_path": "/dev/null",
                "pkey_path": base._TEST_SSH_KEY_PATH,
            },
        )
        # Empty devices list passes the destination validate_connection loop.
        self._dst_endpoint = self._create_endpoint(
            name="cap-dest",
            endpoint_type="test-dest",
            connection_info={
                "devices": [],
                "pkey_path": base._TEST_SSH_KEY_PATH,
            },
        )

    def test_validate_connection(self):
        valid, message = self._client.endpoints.validate_connection(
            self._src_endpoint.id)
        self.assertTrue(valid, f"source: {message}")

        valid, message = self._client.endpoints.validate_connection(
            self._dst_endpoint.id)
        self.assertTrue(valid, f"destination: {message}")

    def test_validate_connection_failure(self):
        bad_endpoint = self._create_endpoint(
            name="cap-bad",
            endpoint_type="test-src",
            connection_info={
                "block_device_path": "/dev/coriolis-no-such-device",
                "pkey_path": base._TEST_SSH_KEY_PATH,
            },
        )
        valid, message = self._client.endpoints.validate_connection(
            bad_endpoint.id)
        self.assertFalse(valid)
        self.assertIsNotNone(message)

    def test_endpoint_update(self):
        updated = self._client.endpoints.update(
            self._src_endpoint.id,
            {"description": "updated description"},
        )
        self.assertEqual("updated description", updated.description)

    def test_list_networks(self):
        networks = self._client.endpoint_networks.list(self._dst_endpoint.id)
        self.assertTrue(len(networks) > 0, "Expected at least one network")
        first = networks[0]
        self.assertIn("id", first._info)
        self.assertIn("name", first._info)

    def test_list_storage(self):
        storage = self._client.endpoint_storage.list(self._dst_endpoint.id)
        self.assertTrue(
            len(storage) > 0, "Expected at least one storage backend")
        first = storage[0]
        self.assertIn("id", first._info)

        # The test provider's get_storage() does not set a config_default so
        # the value is expected to be None.
        default = self._client.endpoint_storage.get_default(
            self._dst_endpoint.id)
        self.assertIsNone(default)

    def test_list_source_options(self):
        options = self._client.endpoint_source_options.list(
            self._src_endpoint.id)
        self.assertIsInstance(options, list)
        self.assertTrue(
            len(options) > 0, "Expected at least one source option")

    def test_list_destination_options(self):
        options = self._client.endpoint_destination_options.list(
            self._dst_endpoint.id)
        self.assertIsInstance(options, list)
        self.assertTrue(
            len(options) > 0, "Expected at least one destination option")

    def test_list_instances(self):
        instances = self._client.endpoint_instances.list(
            self._src_endpoint.id, env={})

        self.assertIsInstance(instances, list)
        self.assertTrue(len(instances) > 0, "Expected at least one instance")
        first = instances[0]
        self.assertIn("id", first._info)
        self.assertIn("name", first._info)

        instances = self._client.endpoint_instances.list(
            self._src_endpoint.id, env={}, name="null")
        self.assertIsInstance(instances, list)

    def test_get_instance(self):
        instances = self._client.endpoint_instances.list(
            self._src_endpoint.id, env={})

        self.assertTrue(len(instances) > 0)

        instance = self._client.endpoint_instances.get(
            self._src_endpoint.id, instances[0].name, env={})

        self.assertEqual(instances[0].name, instance.name)
