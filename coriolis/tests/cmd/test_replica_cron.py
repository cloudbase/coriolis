# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import sys
from unittest import mock

from coriolis.cmd import replica_cron
from coriolis import constants
from coriolis.replica_cron.rpc import server as rpc_server
from coriolis import service
from coriolis.tests import test_base
from coriolis import utils


class ReplicaCronTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis replica_cron CMD"""

    @mock.patch.object(service, 'service')
    @mock.patch.object(service, 'MessagingService')
    @mock.patch.object(rpc_server, 'ReplicaCronServerEndpoint')
    @mock.patch.object(utils, 'setup_logging')
    @mock.patch('coriolis.cmd.replica_cron.CONF')
    @mock.patch.object(sys, 'argv')
    def test_main(
        self,
        mock_argv,
        mock_conf,
        mock_setup_logging,
        mock_ReplicaCronServerEndpoint,
        mock_MessagingService,
        mock_service
    ):
        replica_cron.main()

        mock_conf.assert_called_once_with(
            mock_argv[1:], project='coriolis', version="1.0.0")
        mock_setup_logging.assert_called_once()
        mock_ReplicaCronServerEndpoint.assert_called_once()
        mock_MessagingService.assert_called_once_with(
            constants.REPLICA_CRON_MAIN_MESSAGING_TOPIC,
            [mock_ReplicaCronServerEndpoint.return_value],
            rpc_server.VERSION,
            worker_count=1)
        mock_service.launch.assert_called_once_with(
            mock_conf, mock_MessagingService.return_value,
            workers=mock_MessagingService.return_value.
            get_workers_count.return_value)
        mock_service.launch.return_value.wait.assert_called_once()
