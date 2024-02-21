# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import sys
from unittest import mock

from coriolis.cmd import worker
from coriolis import constants
from coriolis import service
from coriolis.tests import test_base
from coriolis import utils
from coriolis.worker.rpc import server as rpc_server


class WorkerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis worker CMD"""

    @mock.patch.object(service, 'service')
    @mock.patch.object(rpc_server, 'WorkerServerEndpoint')
    @mock.patch.object(service, 'MessagingService')
    @mock.patch.object(utils, 'setup_logging')
    @mock.patch('coriolis.cmd.worker.CONF')
    @mock.patch.object(service, 'get_worker_count_from_args')
    @mock.patch.object(sys, 'argv')
    def test_main(
        self,
        mock_argv,
        mock_get_worker_count_from_args,
        mock_conf,
        mock_setup_logging,
        mock_MessagingService,
        mock_WorkerServerEndpoint,
        mock_service
    ):
        worker_count = mock.sentinel.worker_count
        args = ['mock_arg_1', 'mock_arg_2']
        mock_get_worker_count_from_args.return_value = (worker_count, args)

        worker.main()

        mock_get_worker_count_from_args.assert_called_once_with(mock_argv)
        mock_conf.assert_called_once_with(
            ['mock_arg_2'], project='coriolis', version="1.0.0")
        mock_setup_logging.assert_called_once()
        mock_MessagingService.assert_called_once_with(
            constants.WORKER_MAIN_MESSAGING_TOPIC,
            [mock_WorkerServerEndpoint.return_value],
            rpc_server.VERSION, worker_count=worker_count, init_rpc=False)
        mock_service.launch.assert_called_once_with(
            mock_conf, mock_MessagingService.return_value,
            workers=mock_MessagingService.return_value.
            get_workers_count.return_value)
        mock_service.launch.return_value.wait.assert_called_once()

    @mock.patch.object(service, 'service')
    @mock.patch.object(rpc_server, 'WorkerServerEndpoint')
    @mock.patch.object(service, 'MessagingService')
    @mock.patch.object(utils, 'setup_logging')
    @mock.patch('coriolis.cmd.worker.CONF')
    @mock.patch.object(service, 'get_worker_count_from_args')
    @mock.patch.object(sys, 'argv')
    def test_main_no_worker_count(
        self,
        mock_argv,
        mock_get_worker_count_from_args,
        mock_conf,
        mock_setup_logging,
        mock_MessagingService,
        mock_WorkerServerEndpoint,
        mock_service
    ):
        worker_count = None
        args = ['mock_arg_1', 'mock_arg_2']
        mock_get_worker_count_from_args.return_value = (worker_count, args)

        worker.main()

        mock_get_worker_count_from_args.assert_called_once_with(mock_argv)
        mock_conf.assert_called_once_with(
            ['mock_arg_2'], project='coriolis', version="1.0.0")
        mock_setup_logging.assert_called_once()
        mock_MessagingService.assert_called_once_with(
            constants.WORKER_MAIN_MESSAGING_TOPIC,
            [mock_WorkerServerEndpoint.return_value],
            rpc_server.VERSION, worker_count=mock_conf.worker.worker_count,
            init_rpc=False)
        mock_service.launch.assert_called_once_with(
            mock_conf, mock_MessagingService.return_value,
            workers=mock_MessagingService.return_value.
            get_workers_count.return_value)
        mock_service.launch.return_value.wait.assert_called_once()
