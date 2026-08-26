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

from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class BaseTestExportProvider(abc.ABC):
    def initialize(self, connection_info: dict):
        """One-time initialization, before any tests run.

        Can be used to list the current resources on the source provider,
        which can then be used to check if any test resources leaked and
        clean them.
        """

    def teardown(self, connection_info: dict):
        """One-time teardown called at atexit.

        Can be used to check and clean any leaked test resources.
        """

    def check_prerequisites(self):
        """Raise ``unittest.SkipTest`` if required infrastructure is absent."""

    def get_test_instance(self, size_mb: int):
        """Return ``(instance_name, source_environment)`` to use for one test.

        *size_mb* is a hint for providers that create disposable block devices;
        providers backed by a pre-existing VM ignore it.

        The returned ``source_environment`` is merged into the transfer's
        source_environment; it is only non-empty for providers that need to
        advertise something about the instance they just created (e.g.: the
        core test provider's ``instance_block_devices``).
        """
        raise NotImplementedError

    def delete_test_instance(self, instance_name: str):
        """Release resources allocated by ``get_test_instance()``, if any."""

    def get_test_instance_device(self, instance_name: str):
        """Return the local block-device path backing *instance_name*.

        Only meaningful for providers that back test instances with a host-readable
        device (the core test provider); other providers return ``None``.
        """
        return None


class BaseTestImportProvider(abc.ABC):
    def initialize(self, connection_info: dict):
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
