# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import ddt

from coriolis import exception
from coriolis.scheduler.filters import trivial_filters
from coriolis.scheduler.rpc import server
from coriolis.tests import test_base
from coriolis.tests import testutils


@ddt.ddt
class SchedulerServerEndpointTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Scheduler Worker RPC server."""

    def setUp(self):
        super(SchedulerServerEndpointTestCase, self).setUp()
        self.server = server.SchedulerServerEndpoint()

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
        # providers is the only field used as dict in the code
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
