# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import time

from unittest import mock

from coriolis.conductor.rpc import utils as rpc_utils
from coriolis.tests import test_base
from coriolis import utils


class CoriolisTestException(Exception):
    pass


class ConductorUtilsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Conductor RPC utils."""

    @mock.patch.object(utils, 'get_exception_details')
    @mock.patch.object(time, 'sleep')
    def test_check_create_registration_for_service(
        self,
        mock_sleep,
        mock_get_exception_details
    ):
        conductor_rpc = mock.Mock()
        conductor_rpc.check_service_registered.return_value = {
            'id': mock.sentinel.id}
        result = rpc_utils.check_create_registration_for_service(
            conductor_rpc,
            mock.sentinel.request_context,
            mock.sentinel.host,
            mock.sentinel.binary,
            mock.sentinel.topic,
            enabled=False,
            mapped_regions=None,
            providers=mock.sentinel.providers,
            specs=mock.sentinel.specs,
            retry_period=30
        )

        self.assertEqual(
            conductor_rpc.update_service.return_value,
            result
        )
        conductor_rpc.check_service_registered.assert_called_once_with(
            mock.sentinel.request_context,
            mock.sentinel.host,
            mock.sentinel.binary,
            mock.sentinel.topic
        )
        conductor_rpc.update_service.assert_called_once_with(
            mock.sentinel.request_context,
            mock.sentinel.id,
            updated_values={
                "providers": mock.sentinel.providers,
                "specs": mock.sentinel.specs
            }
        )
        conductor_rpc.register_service.assert_not_called()
        mock_sleep.assert_not_called()
        mock_get_exception_details.assert_not_called()

    @mock.patch.object(utils, 'get_exception_details')
    @mock.patch.object(time, 'sleep')
    def test_check_create_registration_for_service_register_service(
        self,
        mock_sleep,
        mock_get_exception_details
    ):
        conductor_rpc = mock.Mock()
        conductor_rpc.check_service_registered.side_effect = \
            [CoriolisTestException(), None]
        period = 30
        with self.assertLogs('coriolis.conductor.rpc.utils', level='WARN'):
            result = rpc_utils.check_create_registration_for_service(
                conductor_rpc,
                mock.sentinel.request_context,
                mock.sentinel.host,
                mock.sentinel.binary,
                mock.sentinel.topic,
                enabled=False,
                mapped_regions=None,
                providers=mock.sentinel.providers,
                specs=mock.sentinel.specs,
                retry_period=30
            )

        self.assertEqual(
            conductor_rpc.register_service.return_value,
            result
        )
        conductor_rpc.check_service_registered.assert_has_calls(
            [mock.call(
                mock.sentinel.request_context,
                mock.sentinel.host,
                mock.sentinel.binary,
                mock.sentinel.topic
            )] * 2
        )
        conductor_rpc.register_service.assert_called_once_with(
            mock.sentinel.request_context,
            mock.sentinel.host,
            mock.sentinel.binary,
            mock.sentinel.topic,
            False,
            mapped_regions=None,
            providers=mock.sentinel.providers,
            specs=mock.sentinel.specs
        )
        conductor_rpc.update_service.assert_not_called()
        mock_sleep.assert_called_once_with(period)
        mock_get_exception_details.assert_called_once()
