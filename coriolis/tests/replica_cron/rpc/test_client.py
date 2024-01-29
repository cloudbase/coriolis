# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.replica_cron.rpc import client as rpc_client
from coriolis.tests import test_base


class ReplicaCronClientTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the ReplicaCronClient class."""

    def setUp(self):
        super(ReplicaCronClientTestCase, self).setUp()
        self.client = rpc_client.ReplicaCronClient()
        self.ctxt = mock.MagicMock()

    def test_register(self):
        self.client._call = mock.Mock()
        self.client.register(self.ctxt, mock.sentinel.schedule)

        self.client._call.assert_called_once_with(
            self.ctxt, 'register', schedule=mock.sentinel.schedule)

    def test_unregister(self):
        self.client._call = mock.Mock()
        self.client.unregister(self.ctxt, mock.sentinel.schedule)

        self.client._call.assert_called_once_with(
            self.ctxt, 'unregister', schedule=mock.sentinel.schedule)

    def test_get_diagnostics(self):
        self.client._call = mock.Mock()
        result = self.client.get_diagnostics(self.ctxt)

        self.client._call.assert_called_once_with(
            self.ctxt, 'get_diagnostics')
        self.assertEqual(result, self.client._call.return_value)
