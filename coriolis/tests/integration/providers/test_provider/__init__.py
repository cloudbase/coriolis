# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
TestProvider registered in CONF.providers for integration tests.

The Coriolis provider factory resolves a single dotted class path and queries
it for every provider type it implements. A single entry in CONF.providers
satisfies both the source and destination side of a replica execution.
"""


from coriolis.providers import base


class TestProvider(
    base.BaseEndpointProvider,
    base.BaseReplicaExportProvider,
    base.BaseReplicaImportProvider,
):
    platform = "foo"

    def __new__(cls, *args, **kwargs):
        cls.__abstractmethods__ = set()
        return super().__new__(cls)

    def get_source_environment_schema(self):
        return {}

    def get_target_environment_schema(self):
        return {}
