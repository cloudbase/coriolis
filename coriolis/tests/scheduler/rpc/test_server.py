# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

import ddt

from coriolis import constants
from coriolis.db import api as db_api
from coriolis import exception
from coriolis.scheduler.filters import trivial_filters
from coriolis.scheduler.rpc import server
from coriolis.tests import test_base
from coriolis.tests import testutils
from coriolis import utils


@ddt.ddt
class SchedulerServerEndpointTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Scheduler Worker RPC server."""

    def setUp(self):
        super(SchedulerServerEndpointTestCase, self).setUp()
        self.server = server.SchedulerServerEndpoint()

    @mock.patch.object(utils, "get_diagnostics_info")
    def test_get_diagnostics(self, mock_get_diagnostics_info):
        result = self.server.get_diagnostics(mock.sentinel.context)

        mock_get_diagnostics_info.assert_called_once_with()
        self.assertEqual(result, mock_get_diagnostics_info.return_value)

    @mock.patch.object(trivial_filters, 'TopicFilter', autospec=True)
    @mock.patch.object(db_api, 'get_services')
    def test_get_all_worker_services(self, mock_get_services,
                                     mock_topic_filter_cls):
        mock_get_services.return_value = mock.sentinel.services

        mock_topic_filter_cls.return_value.filter_services.return_value = \
            mock.sentinel.filtered_services

        result = self.server._get_all_worker_services(mock.sentinel.context)

        mock_get_services.assert_called_once_with(mock.sentinel.context)
        mock_topic_filter_cls.assert_called_once_with(
            constants.WORKER_MAIN_MESSAGING_TOPIC)
        mock_topic_filter_cls.return_value.filter_services.\
            assert_called_once_with(mock.sentinel.services)

        self.assertEqual(result, mock.sentinel.filtered_services)

    @mock.patch.object(db_api, 'get_services')
    def test_get_all_worker_services_no_services(self, mock_get_services):
        mock_get_services.return_value = []

        self.assertRaises(exception.NoWorkerServiceError,
                          self.server._get_all_worker_services,
                          mock.sentinel.context)

        mock_get_services.assert_called_once_with(mock.sentinel.context)

    def test_get_weighted_filtered_services_no_filters(self):
        services = [mock.Mock(id=1), mock.Mock(id=2)]

        with self.assertLogs('coriolis.scheduler.rpc.server',
                             level=logging.WARN):
            result = self.server._get_weighted_filtered_services(services,
                                                                 None)
        expected_result = [(services[0], 100), (services[1], 100)]
        self.assertEqual(result, expected_result)

    def test_get_weighted_filtered_services_with_filters_reject(self):
        services = [mock.Mock(id=1), mock.Mock(id=2)]
        filters = [mock.Mock(), mock.Mock()]
        filters[0].rate_service.return_value = 50
        filters[1].rate_service.return_value = 0

        self.assertRaises(exception.NoSuitableWorkerServiceError,
                          self.server._get_weighted_filtered_services,
                          services, filters)

    def test_get_weighted_filtered_services_with_filters_accept(self):
        services = [mock.Mock(id=1), mock.Mock(id=2)]
        filters = [mock.Mock(), mock.Mock()]
        filters[0].rate_service.return_value = 50
        filters[1].rate_service.return_value = 100

        result = self.server._get_weighted_filtered_services(services,
                                                             filters)
        expected_result = [(services[0], 150), (services[1], 150)]
        self.assertEqual(result, expected_result)

    @mock.patch.object(db_api, 'get_regions')
    def test__filter_regions_check_all_exist_false(self, mock_get_regions):
        mock_get_regions.return_value = [
            mock.Mock(id='region1', enabled=True),
            mock.Mock(id='region2', enabled=True),
        ]
        region_ids = ['region1', 'region2']

        result = self.server._filter_regions(None, region_ids,
                                             check_all_exist=False)

        self.assertEqual(result, mock_get_regions.return_value)

    @mock.patch.object(db_api, 'get_regions')
    def test__filter_regions_all_disabled(self, mock_get_regions):
        mock_get_regions.return_value = [
            mock.Mock(id='region1', enabled=False),
            mock.Mock(id='region2', enabled=False),
        ]
        region_ids = ['region1', 'region2']

        result = self.server._filter_regions(None, region_ids, enabled=False)

        self.assertEqual(result, mock_get_regions.return_value)

    @mock.patch.object(db_api, 'get_regions')
    def test__filter_regions_some_enabled_some_disabled(self,
                                                        mock_get_regions):
        mock_get_regions.return_value = [
            mock.Mock(id='region1', enabled=True),
            mock.Mock(id='region2', enabled=False),
        ]
        region_ids = ['region1', 'region2']

        result = self.server._filter_regions(None, region_ids)

        self.assertEqual(result, [mock_get_regions.return_value[0]])

    @mock.patch.object(db_api, 'get_regions')
    def test__filter_regions_some_missing(self, mock_get_regions):
        mock_get_regions.return_value = [
            mock.Mock(id='region1', enabled=True),
            mock.Mock(id='region2', enabled=True),
        ]
        region_ids = ['region1', 'region2', 'region3']

        self.assertRaises(exception.RegionNotFound,
                          self.server._filter_regions, None, region_ids)

    @mock.patch.object(trivial_filters, 'ProviderTypesFilter', autospec=True)
    @mock.patch.object(trivial_filters, 'RegionsFilter', autospec=True)
    @mock.patch.object(trivial_filters, 'EnabledFilter', autospec=True)
    @mock.patch.object(
        server.SchedulerServerEndpoint, '_get_weighted_filtered_services'
    )
    @mock.patch.object(
        server.SchedulerServerEndpoint, '_filter_regions'
    )
    @mock.patch.object(
        server.SchedulerServerEndpoint, '_get_all_worker_services'
    )
    @ddt.file_data("data/get_workers_for_specs_config.yaml")
    @ddt.unpack
    def test_get_workers_for_specs(
            self,
            mock_get_all_worker_services,
            mock_filter_regions,
            mock_get_weighted_filtered_services,
            mock_enabled_filter_cls,
            mock_regions_filter_cls,
            mock_provider_types_filter_cls,
            config,
            expected_result,
            expected_exception,
    ):

        enabled = config.get("enabled", None)
        region_sets = config.get("region_sets", None)
        provider_requirements = config.get("provider_requirements", None)

        # Convert the config dict to an object, skipping the providers
        # as it's the only field used as dict in the code
        config_obj = testutils.DictToObject(config, skip_attrs=["providers"])
        mock_get_all_worker_services.return_value = (
            config_obj.services_db or []
        )
        mock_filter_regions.return_value = config_obj.regions_db or []
        mock_get_weighted_filtered_services.return_value = \
            [] if expected_result is None else [
                (mock.Mock(id=expected_id), 100)
                for expected_id in expected_result
            ]

        kwargs = {
            "enabled": enabled,
            "region_sets": region_sets,
            "provider_requirements": provider_requirements,
        }
        if expected_exception:
            exception_type = getattr(exception, expected_exception)
            self.assertRaises(
                exception_type,
                self.server.get_workers_for_specs,
                mock.sentinel.context,
                **kwargs
            )
            return

        result = self.server.get_workers_for_specs(
            mock.sentinel.context,
            **kwargs
        )

        mock_get_all_worker_services.assert_called_once_with(
            mock.sentinel.context)

        if region_sets:
            calls = [mock.call(
                mock.sentinel.context,
                region_set,
                enabled=True,
                check_all_exist=True)
                for region_set in region_sets]
            mock_filter_regions.assert_has_calls(calls, any_order=True)

        mock_get_weighted_filtered_services.assert_called_once_with(
            mock_get_all_worker_services.return_value, mock.ANY
        )

        id_array = [worker.id for worker in result]

        self.assertEqual(id_array, expected_result)

        # Assertions for the trivial filter classes
        if enabled is not None:
            mock_enabled_filter_cls.assert_called_once_with(enabled=enabled)
        if region_sets:
            calls = [mock.call(region_set, any_region=True)
                     for region_set in region_sets]
            mock_regions_filter_cls.assert_has_calls(calls, any_order=True)
        if provider_requirements:
            mock_provider_types_filter_cls.assert_called_once_with(
                provider_requirements)
