# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Abstract base classes for test import / export providers.

Based on the Base* provider convention from coriolis/providers/base.py.

BaseTestImportProvider and BaseTestExportProvider contain provider-specific
logic not currently defined in the import / export providers, meant to be
used for testing-only purposes:
    - detect leaked resources
    - delete deployed replicas
"""

import abc
import unittest

from oslo_log import log as logging

from coriolis import context as coriolis_context

LOG = logging.getLogger(__name__)


class BaseTestExportProvider(abc.ABC):
    """Base test provider to be used by export providers.

    The base implementation relies on a pre-existing VM referenced by
    ``source.instance_name`` in providers.yaml, which will then be used in the
    integration tests.

    Providers that need to track / clean up leaked resources around this default
    behavior (e.g.: export-side snapshots) should override ``initialize()`` /
    ``teardown()``.

    Must be mixed in ahead of the concrete ExportProvider class, and its
    own ``__init__`` called explicitly, e.g.:

        class FooTestExportProvider(BaseTestExportProvider, FooExportProvider):
            def __init__(self, event_handler=None):
                FooExportProvider.__init__(self, event_handler)
                BaseTestExportProvider.__init__(self)
    """

    def __init__(self):
        self._ctxt = coriolis_context.get_admin_context()
        self._connection_info = None
        self._instance_name = None

    def initialize(self, connection_info: dict, source_config: dict):
        """One-time initialization, before any tests run.

        Can be used to list the current resources on the source provider,
        which can then be used to check if any test resources leaked and
        clean them.
        """
        self._connection_info = connection_info
        self._instance_name = source_config.get("instance_name")

    def teardown(self, connection_info: dict):
        """One-time teardown called at atexit.

        Can be used to check and clean any leaked test resources.
        """

    def check_prerequisites(self):
        """Raise ``unittest.SkipTest`` if required infrastructure is absent."""
        provider_name = type(self).__name__

        if not self._connection_info:
            raise unittest.SkipTest(
                "%s requires 'source.connection_info' in providers.yaml" % provider_name
            )

        if not self._instance_name:
            raise unittest.SkipTest(
                "%s requires 'source.instance_name' in providers.yaml" % provider_name
            )

        try:
            self.validate_connection(self._ctxt, self._connection_info)
        except Exception as ex:
            raise unittest.SkipTest() from ex

    def get_test_instance(self, size_mb: int):
        """Return ``(instance_name, source_environment)`` to use for one test.

        *size_mb* is a hint for providers that create disposable block devices;
        providers backed by a pre-existing VM ignore it.

        The returned ``source_environment`` is merged into the transfer's
        source_environment; it is only non-empty for providers that need to
        advertise something about the instance they just created (e.g.: the
        core test provider's ``instance_block_devices``).
        """
        return self._instance_name, {}

    def delete_test_instance(self, instance_name: str):
        """Release resources allocated by ``get_test_instance()``, if any."""

    def get_test_instance_device(self, instance_name: str):
        """Return the local block-device path backing *instance_name*.

        Only meaningful for providers that back test instances with a host-readable
        device (the core test provider); other providers return ``None``.
        """
        return None


class BaseTestImportProvider(abc.ABC):
    def initialize(self, connection_info: dict, source_config: dict):
        """One-time initialization, before any tests run.

        Can be used to list the current resources on the target provider,
        which can then be used to check if any test resources leaked and
        clean them.
        """

    def teardown(self, connection_info: dict):
        """One-time teardown called at atexit.

        Can be used to check and clean any leaked test resources.
        """

    def check_prerequisites(self):
        """Raise ``unittest.SkipTest`` if required infrastructure is absent."""

    def delete_deployed_instance(
        self,
        connection_info: dict,
        instance_name: str,
    ):
        """Destroy the VM created at the destination by a completed deployment.

        Called during integration test cleanup after each deployment test, so
        that finalized VMs do not accumulate across runs and cause failures in
        later tests (e.g. name collisions, resource exhaustion).
        """
