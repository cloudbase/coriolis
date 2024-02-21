# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import sys
from unittest import mock

from coriolis.cmd import api
from coriolis import service
from coriolis.tests import test_base
from coriolis import utils


class ApiTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis api CMD"""

    @mock.patch.object(service, 'service')
    @mock.patch.object(service, 'WSGIService')
    @mock.patch.object(utils, 'setup_logging')
    @mock.patch('coriolis.cmd.api.CONF')
    @mock.patch.object(service, 'get_worker_count_from_args')
    @mock.patch.object(sys, 'argv')
    def test_main(
        self,
        mock_argv,
        mock_get_worker_count_from_args,
        mock_conf,
        mock_setup_logging,
        mock_WSGIService,
        mock_service
    ):
        worker_count = mock.sentinel.worker_count
        args = ['mock_arg_1', 'mock_arg_2']
        mock_get_worker_count_from_args.return_value = (worker_count, args)

        api.main()

        mock_get_worker_count_from_args.assert_called_once_with(mock_argv)
        mock_conf.assert_called_once_with(
            ['mock_arg_2'], project='coriolis', version="1.0.0")
        mock_setup_logging.assert_called_once()
        mock_WSGIService.assert_called_once_with(
            'coriolis-api', worker_count=worker_count)
        mock_service.launch.assert_called_once_with(
            mock_conf, mock_WSGIService.return_value,
            workers=mock_WSGIService.return_value.
            get_workers_count.return_value)
        mock_service.launch.return_value.wait.assert_called_once()

    @mock.patch.object(service, 'service')
    @mock.patch.object(service, 'WSGIService')
    @mock.patch.object(utils, 'setup_logging')
    @mock.patch('coriolis.cmd.api.CONF')
    @mock.patch.object(service, 'get_worker_count_from_args')
    @mock.patch.object(sys, 'argv')
    def test_main_no_worker_count(
        self,
        mock_argv,
        mock_get_worker_count_from_args,
        mock_conf,
        mock_setup_logging,
        mock_WSGIService,
        mock_service
    ):
        worker_count = None
        args = ['mock_arg_1', 'mock_arg_2']
        mock_get_worker_count_from_args.return_value = (worker_count, args)

        api.main()

        mock_get_worker_count_from_args.assert_called_once_with(mock_argv)
        mock_conf.assert_called_once_with(
            ['mock_arg_2'], project='coriolis', version="1.0.0")
        mock_setup_logging.assert_called_once()
        mock_WSGIService.assert_called_once_with(
            'coriolis-api', worker_count=mock_conf.api.worker_count)
        mock_service.launch.assert_called_once_with(
            mock_conf, mock_WSGIService.return_value,
            workers=mock_WSGIService.return_value.
            get_workers_count.return_value)
        mock_service.launch.return_value.wait.assert_called_once()
