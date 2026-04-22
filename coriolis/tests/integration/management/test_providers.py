# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""Integration tests for the providers APIs.

Exercises providers.list() and providers.schemas_list() via the Coriolis
REST API.
"""

from coriolis import constants
from coriolis.tests.integration import base


class ProvidersTest(base.CoriolisIntegrationTestBase):

    def test_list_providers(self):
        providers = self._client.providers.list()

        provider_names = [p["name"] for p in providers.providers_list]
        self.assertIn(self._imp_platform, provider_names)
        self.assertIn(self._exp_platform, provider_names)

    def test_schemas_list(self):
        # PROVIDER_TYPE_ENDPOINT returns a "connection_info_schema" key.
        schemas = self._client.providers.schemas_list(
            self._imp_platform, constants.PROVIDER_TYPE_ENDPOINT)

        schema_types = [s["type"] for s in schemas.provider_schemas]
        self.assertIn("connection_info_schema", schema_types)

        # PROVIDER_TYPE_TRANSFER_IMPORT returns a
        # "destination_environment_schema" key.
        schemas = self._client.providers.schemas_list(
            self._imp_platform, constants.PROVIDER_TYPE_TRANSFER_IMPORT)

        schema_types = [s["type"] for s in schemas.provider_schemas]
        self.assertIn("destination_environment_schema", schema_types)

        # PROVIDER_TYPE_TRANSFER_EXPORT returns a "source_environment_schema"
        # key.
        schemas = self._client.providers.schemas_list(
            self._exp_platform, constants.PROVIDER_TYPE_TRANSFER_EXPORT)

        schema_types = [s["type"] for s in schemas.provider_schemas]
        self.assertIn("source_environment_schema", schema_types)
