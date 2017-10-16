# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import ddt
import mock

from coriolis.conductor.rpc import server
from coriolis import exception
from coriolis.tests import test_base


@ddt.ddt
class ConductorServerEndpointTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Conductor RPC server."""

    def setUp(self):
        super(ConductorServerEndpointTestCase, self).setUp()
        self.server = server.ConductorServerEndpoint()

    @ddt.data({}, {mock.sentinel.instance: {}})
    @mock.patch.object(server.ConductorServerEndpoint, '_create_task')
    @mock.patch.object(server.ConductorServerEndpoint,
                       '_check_replica_running_executions')
    @mock.patch.object(server.ConductorServerEndpoint, '_get_replica')
    def test_update_endpoint_not_found(self, replica_info, mock_get_replica,
                                       mock_check_replica_running,
                                       mock_create_task):
        mock_replica = mock_get_replica.return_value
        mock_replica.instances = [mock.sentinel.instance]
        mock_replica.info = {}

        self.assertRaises(exception.InvalidReplicaState,
                          self.server.delete_replica_disks,
                          mock.sentinel.context, mock.sentinel.replica_id)

        mock_get_replica.assert_called_once_with(mock.sentinel.context,
                                                 mock.sentinel.replica_id)
        mock_check_replica_running.assert_called_once_with(
            mock.sentinel.context, mock_replica)
        mock_create_task.assert_not_called()
