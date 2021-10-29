# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import ddt
from unittest import mock

from coriolis.conductor.rpc import server
from coriolis import exception
from coriolis.tests import test_base, testutils


@ddt.ddt
class ConductorServerEndpointTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Conductor RPC server."""

    def setUp(self):
        super(ConductorServerEndpointTestCase, self).setUp()
        self.server = server.ConductorServerEndpoint()

    @mock.patch.object(server.ConductorServerEndpoint, '_create_task')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_check_replica_running_executions')
    @mock.patch.object(server.ConductorServerEndpoint, '_get_replica')
    def test_delete_replica_disks_invalid_state(self, mock_get_replica,
                                                mock_check_replica_running,
                                                mock_create_task):
        mock_replica = mock_get_replica.return_value
        mock_replica.instances = [mock.sentinel.instance]
        mock_replica.info = {}
        delete_replica_disks = testutils.get_wrapped_function(
            self.server.delete_replica_disks)

        self.assertRaises(exception.InvalidReplicaState,
                          delete_replica_disks,
                          self.server, mock.sentinel.context, mock.sentinel.replica_id)

        mock_get_replica.assert_called_once_with(mock.sentinel.context,
                                                 mock.sentinel.replica_id)
        mock_check_replica_running.assert_called_once_with(
            mock.sentinel.context, mock_replica)
        mock_create_task.assert_not_called()
