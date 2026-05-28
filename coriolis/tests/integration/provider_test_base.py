# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Abstract base class for test import providers.

Based on the Base* provider convention from coriolis/providers/base.py.

The BaseTestImportProvider contains provider-specific logic not currently
defined in the import providers, meant to be used for testing-only purposes:
    - detect leaked resources
    - delete deployed replicas
"""

import abc

from oslo_log import log as logging

LOG = logging.getLogger(__name__)


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
        self, connection_info: dict, instance_name: str,
    ):
        """Destroy the VM created at the destination by a completed deployment.

        Called during integration test cleanup after each deployment test, so
        that finalized VMs do not accumulate across runs and cause failures in
        later tests (e.g. name collisions, resource exhaustion).
        """
