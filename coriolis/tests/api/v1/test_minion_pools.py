# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import ddt
from webob import exc

from coriolis.api.v1 import minion_pools
from coriolis.api.v1.views import minion_pool_view
from coriolis import constants
from coriolis.endpoints import api as endpoints_api
from coriolis import exception
from coriolis.minion_pools import api
from coriolis.tests import test_base
from coriolis.tests import testutils


@ddt.ddt
class MinionPoolControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Minion Pool v1 API"""

    def setUp(self):
        super(MinionPoolControllerTestCase, self).setUp()
        self.minion_pools = minion_pools.MinionPoolController()

    @mock.patch.object(minion_pool_view, 'single')
    @mock.patch.object(api.API, 'get_minion_pool')
    def test_show(
        self,
        mock_get_minion_pool,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id

        result = self.minion_pools.show(mock_req, id)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with("migration:minion_pools:show")
        mock_get_minion_pool.assert_called_once_with(mock_context, id)
        mock_single.assert_called_once_with(
            mock_get_minion_pool.return_value)

    @mock.patch.object(minion_pool_view, 'single')
    @mock.patch.object(api.API, 'get_minion_pool')
    def test_show_not_found(
        self,
        mock_get_minion_pool,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_get_minion_pool.return_value = None

        self.assertRaises(
            exc.HTTPNotFound,
            self.minion_pools.show,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with("migration:minion_pools:show")
        mock_get_minion_pool.assert_called_once_with(mock_context, id)
        mock_single.assert_not_called()

    @mock.patch.object(minion_pool_view, 'collection')
    @mock.patch.object(api.API, 'get_minion_pools')
    def test_index(
        self,
        mock_get_minion_pools,
        mock_collection
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}

        result = self.minion_pools.index(mock_req)

        self.assertEqual(
            mock_collection.return_value,
            result
        )

        mock_context.can.assert_called_once_with("migration:minion_pools:list")
        mock_get_minion_pools.assert_called_once_with(mock_context)
        mock_collection.assert_called_once_with(
            mock_get_minion_pools.return_value)

    @ddt.file_data('data/minion_pools_retention_strategy.yml')
    def test__check_pool_retention_strategy(
        self,
        exception_raised,
        pool_retention_strategy,
    ):
        if exception_raised:
            self.assertRaisesRegex(
                Exception,
                "Invalid minion pool retention strategy '%s'"
                % pool_retention_strategy,
                self.minion_pools._check_pool_retention_strategy,
                pool_retention_strategy
            )
        else:
            strategy = getattr(constants, pool_retention_strategy)

            self.assertEqual(
                None,
                self.minion_pools._check_pool_retention_strategy(strategy)
            )

    @ddt.file_data('data/minion_pools_numeric_values.yml')
    def test__check_pool_numeric_values(
        self,
        config,
        exception_raised
    ):
        minimum_minions = config.get("minimum_minions", None)
        maximum_minions = config.get("maximum_minions", None)
        minion_max_idle_time = config.get("minion_max_idle_time", None)
        if exception_raised:
            self.assertRaisesRegex(
                Exception,
                exception_raised,
                self.minion_pools._check_pool_numeric_values,
                minimum_minions,
                maximum_minions,
                minion_max_idle_time
            )
        else:
            self.assertEqual(
                self.minion_pools._check_pool_numeric_values(
                    minimum_minions, maximum_minions, minion_max_idle_time),
                None)

    @mock.patch.object(minion_pools.MinionPoolController,
                       '_check_pool_retention_strategy')
    @mock.patch.object(minion_pools.MinionPoolController,
                       '_check_pool_numeric_values')
    @ddt.file_data('data/minion_pools_validate_create_body.yml')
    def test__validate_create_body(
        self,
        mock__check_pool_numeric_values,
        mock__check_pool_retention_strategy,
        config,
        exception_raised,
        expected_result
    ):
        expected_validation_api_method = config.get(
            "expected_validation_api_method", None)
        ctxt = {}
        body = config["body"]
        minion_pool = body["minion_pool"]
        minion_pool["os_type"] = getattr(
            constants, minion_pool["os_type"], None)
        minion_pool["platform"] = getattr(
            constants, minion_pool["platform"], None)
        if 'minion_retention_strategy' in minion_pool:
            minion_pool["minion_retention_strategy"] = \
                getattr(
                    constants, minion_pool["minion_retention_strategy"],
                    constants.MINION_POOL_MACHINE_RETENTION_STRATEGY_DELETE)
        minion_retention_strategy = minion_pool.get(
            'minion_retention_strategy',
            constants.MINION_POOL_MACHINE_RETENTION_STRATEGY_DELETE
        )
        minimum_minions = minion_pool.get('minimum_minions', 1)
        maximum_minions = minion_pool.get('maximum_minions', 1)
        minion_max_idle_time = minion_pool.get('minion_max_idle_time', 1)

        if exception_raised:
            self.assertRaisesRegex(
                Exception,
                exception_raised,
                testutils.get_wrapped_function(
                    self.minion_pools._validate_create_body),
                self.minion_pools,
                ctxt,
                body
            )
        else:
            with mock.patch.object(
                endpoints_api.API,
                expected_validation_api_method) as mock_validation_api_method:

                result = testutils.get_wrapped_function(
                    self.minion_pools._validate_create_body)(
                        self.minion_pools,
                        ctxt,
                        body,
                )

                self.assertEqual(
                    tuple(expected_result),
                    result
                )

                mock_validation_api_method.assert_called_once_with(
                    ctxt,
                    minion_pool["endpoint_id"],
                    minion_pool["environment_options"]
                )
                mock__check_pool_numeric_values.assert_called_once_with(
                    minimum_minions, maximum_minions, minion_max_idle_time)
                mock__check_pool_retention_strategy.assert_called_once_with(
                    minion_retention_strategy
                )

    @mock.patch.object(minion_pool_view, 'single')
    @mock.patch.object(api.API, 'create')
    @mock.patch.object(minion_pools.MinionPoolController,
                       '_validate_create_body')
    def test_create(
        self,
        mock_validate_create_body,
        mock_create,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        mock_body = {}
        mock_validate_create_body.return_value = (mock.sentinel.value,) * 11

        result = self.minion_pools.create(mock_req, mock_body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:minion_pools:create")
        mock_validate_create_body.assert_called_once_with(
            mock_context, mock_body)
        mock_create.assert_called_once()
        mock_single.assert_called_once_with(mock_create.return_value)

    @ddt.file_data('data/minion_pools_validate_enviroment_options.yml')
    def test__validate_updated_environment_options(
        self,
        platform,
        expected_api_method,
        exception_raised
    ):
        platform = getattr(constants, platform, None)
        endpoint_id = mock.sentinel.endpoint_id
        minion_pool = {
            'platform': platform,
            'endpoint_id': endpoint_id
        }
        enviroment_options = mock.sentinel.enviroment_options
        context = mock.sentinel.context

        if exception_raised:
            self.assertRaisesRegex(
                Exception,
                exception_raised,
                self.minion_pools._validate_updated_environment_options,
                context,
                minion_pool,
                enviroment_options
            )
        else:
            with mock.patch.object(endpoints_api.API,
                                   expected_api_method) as mock_api_method:
                self.minion_pools._validate_updated_environment_options(
                    context,
                    minion_pool,
                    enviroment_options
                )

                mock_api_method.assert_called_once_with(
                    context, endpoint_id, enviroment_options)

    @mock.patch.object(minion_pools.MinionPoolController,
                       '_validate_updated_environment_options')
    @mock.patch.object(api.API, 'get_minion_pool')
    @mock.patch.object(minion_pools.MinionPoolController,
                       '_check_pool_retention_strategy')
    @mock.patch.object(minion_pools.MinionPoolController,
                       '_check_pool_numeric_values')
    @ddt.file_data('data/minion_pools_validate_update_body.yml')
    def test__validate_update_body(
        self,
        mock_check_pool_numeric_values,
        mock_check_pool_retention_strategy,
        mock_get_minion_pool,
        mock_validate_updated_environment_options,
        config,
        exception_raised,
        expected_result
    ):
        body = config["body"]
        minion_pool = body["minion_pool"]
        environment_options = minion_pool.get('environment_options', {})
        minion_retention_strategy = minion_pool.get(
            'minion_retention_strategy', "")
        validate_get_minion_pool = config.get("validate_get_minion_pool", None)
        minimum_minions = minion_pool.get('minimum_minions', 1)
        maximum_minions = minion_pool.get('maximum_minions', 1)
        minion_max_idle_time = minion_pool.get('minion_max_idle_time', 1)
        mock_context = mock.Mock()
        id = mock.sentinel.id
        mock_get_minion_pool.return_value = {
            'minimum_minions': 1,
            'maximum_minions': 1
        }

        if exception_raised:
            self.assertRaisesRegex(
                Exception,
                exception_raised,
                testutils.get_wrapped_function(
                    self.minion_pools._validate_update_body),
                self.minion_pools,
                id,
                mock_context,
                body
            )
        else:
            result = testutils.get_wrapped_function(
                self.minion_pools._validate_update_body)(
                    self.minion_pools,
                    id,
                    mock_context,
                    body,
            )

            self.assertEqual(
                expected_result,
                result
            )

            if minion_retention_strategy:
                mock_check_pool_retention_strategy.assert_called_once_with(
                    minion_retention_strategy
                )
            if validate_get_minion_pool:
                mock_get_minion_pool.assert_called_once_with(
                    mock_context, id
                )
                mock_check_pool_numeric_values.assert_called_once_with(
                    minimum_minions, maximum_minions, minion_max_idle_time
                )
            if environment_options:
                (mock_validate_updated_environment_options.
                    assert_called_once_with)(
                    mock_context, mock_get_minion_pool.return_value,
                    environment_options
                )

    @mock.patch.object(minion_pool_view, 'single')
    @mock.patch.object(api.API, 'update')
    @mock.patch.object(minion_pools.MinionPoolController,
                       '_validate_update_body')
    def test_update(
        self,
        mock_validate_update_body,
        mock_update,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        body = mock.sentinel.body

        result = self.minion_pools.update(mock_req, id, body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:minion_pools:update")
        mock_validate_update_body.assert_called_once_with(
            id, mock_context, body)
        mock_update.assert_called_once_with(
            mock_context, id, mock_validate_update_body.return_value)

    @mock.patch.object(api.API, 'delete')
    def test_delete(
        self,
        mock_delete
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id

        self.assertRaises(
            exc.HTTPNoContent,
            self.minion_pools.delete,
            mock_req,
            id
        )

        mock_delete.assert_called_once_with(mock_context, id)

    @mock.patch.object(api.API, 'delete')
    def test_delete_not_found(
        self,
        mock_delete
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_delete.side_effect = exception.NotFound()

        self.assertRaises(
            exc.HTTPNotFound,
            self.minion_pools.delete,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with(
            "migration:minion_pools:delete")
        mock_delete.assert_called_once_with(mock_context, id)
