# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Base test classes for the Coriolis integration tests.

Ensures the shared ``_IntegrationHarness`` is running (started once per
process) and exposes its port and paths as class attributes. The heavy
lifting - CONF overrides, DB sync, service startup - happens inside the
harness and is not repeated for each subclass.

Subclasses must be run as root.
"""

import os
import unittest

from coriolisclient import client as coriolis_client
from keystoneauth1 import session as ks_session
from keystoneauth1 import token_endpoint
from oslo_config import cfg
from oslo_log import log as logging

from coriolis import constants
from coriolis.tests.integration import harness
from coriolis.tests import test_base

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class CoriolisIntegrationTestBase(test_base.CoriolisBaseTestCase):
    """Base class for integration tests."""

    @classmethod
    def setUpClass(cls):
        if os.geteuid() != 0:
            raise unittest.SkipTest("Integration tests must run as root")

        super().setUpClass()
        cls._harness = harness._IntegrationHarness.get()
        cls._workdir = cls._harness.workdir
        cls._db_path = cls._harness.db_path
        cls._lock_path = cls._harness.lock_path
        cls._api_port = cls._harness.api_port
        cls._client = cls.get_client()

    # Helpers for subclasses
    @classmethod
    def get_client(cls):
        """Return a coriolisclient.Client pointed at the in-process API."""
        auth = token_endpoint.Token(
            endpoint='http://127.0.0.1:%d' % cls._api_port,
            token='integration-dummy-token',
        )
        return coriolis_client.Client(
            session=ks_session.Session(auth=auth),
            project_id=harness._TEST_PROJECT_ID,
        )

    def _create_endpoint(self, **kwargs):
        endpoint_kwargs = {
            "description": "",
            "regions": [],
        }
        endpoint_kwargs.update(kwargs)
        endpoint = self._client.endpoints.create(**endpoint_kwargs)
        self.addCleanup(self._client.endpoints.delete, endpoint.id)

        return endpoint

    def _create_transfer(self, src_id, dst_id, instances):
        """Create a Replica transfer object and return its ID."""
        transfer = self._client.transfers.create(
            origin_endpoint_id=src_id,
            destination_endpoint_id=dst_id,
            source_environment={},
            destination_environment={},
            instances=instances,
            transfer_scenario=constants.TRANSFER_SCENARIO_REPLICA,
            network_map={},
            storage_mappings={},
            notes="integration test replica",
            skip_os_morphing=True,
        )
        self.addCleanup(self._client.transfers.delete, transfer.id)

        return transfer
