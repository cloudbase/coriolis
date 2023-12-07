# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import ddt
from webob import exc

from coriolis.api.v1 import minion_pool_actions as minion
from coriolis.api.v1.views import minion_pool_view
from coriolis import exception
from coriolis.minion_pools import api
from coriolis.tests import test_base
from coriolis.tests import testutils


@ddt.ddt
class MinionPoolActionsControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Minion Pool Actions v1 API"""

    def setUp(self):
        super(MinionPoolActionsControllerTestCase, self).setUp()
        self.minion = minion.MinionPoolActionsController()

    @mock.patch.object(minion_pool_view, 'single')
    @mock.patch.object(api.API, 'allocate_minion_pool')
    def test__allocate_pool(
        self,
        mock_allocate_minion_pool,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_body = {}

        result = testutils.get_wrapped_function(
            self.minion._allocate_pool)(
                mock_req,
                id,
                mock_body
        )

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:minion_pools:allocate")
        mock_allocate_minion_pool.assert_called_once_with(mock_context, id)
        mock_single.assert_called_once_with(
            mock_allocate_minion_pool.return_value)

    @mock.patch.object(minion_pool_view, 'single')
    @mock.patch.object(api.API, 'allocate_minion_pool')
    @ddt.file_data('data/minion_pool_exceptions.yml')
    def test__allocate_pool_raises(
        self,
        exception_raised,
        mock_allocate_minion_pool,
        mock_single,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_body = {}
        expected_exception = getattr(exception, exception_raised)
        mock_allocate_minion_pool.side_effect = expected_exception('err')

        self.assertRaises(
            exc.HTTPNotFound,
            testutils.get_wrapped_function(self.minion._allocate_pool),
            mock_req,
            id,
            mock_body
        )

        mock_context.can.assert_called_once_with(
            "migration:minion_pools:allocate")
        mock_allocate_minion_pool.assert_called_once_with(mock_context, id)
        mock_single.assert_not_called()

    @mock.patch.object(minion_pool_view, 'single')
    @mock.patch.object(api.API, 'refresh_minion_pool')
    def test__refresh_pool(
        self,
        mock_refresh_minion_pool,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_body = {}

        result = testutils.get_wrapped_function(
            self.minion._refresh_pool)(
                mock_req,
                id,
                mock_body
        )

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:minion_pools:refresh")
        mock_refresh_minion_pool.assert_called_once_with(mock_context, id)
        mock_single.assert_called_once_with(
            mock_refresh_minion_pool.return_value)

    @mock.patch.object(minion_pool_view, 'single')
    @mock.patch.object(api.API, 'refresh_minion_pool')
    @ddt.file_data('data/minion_pool_exceptions.yml')
    def test__refresh_pool_raises(
        self,
        exception_raised,
        mock_refresh_minion_pool,
        mock_single,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_body = {}
        expected_exception = getattr(exception, exception_raised)
        mock_refresh_minion_pool.side_effect = expected_exception('err')

        self.assertRaises(
            exc.HTTPNotFound,
            testutils.get_wrapped_function(self.minion._refresh_pool),
            mock_req,
            id,
            mock_body
        )

        mock_context.can.assert_called_once_with(
            "migration:minion_pools:refresh")
        mock_refresh_minion_pool.assert_called_once_with(mock_context, id)
        mock_single.assert_not_called()

    @mock.patch.object(minion_pool_view, 'single')
    @mock.patch.object(api.API, 'deallocate_minion_pool')
    def test__deallocate_pool(
        self,
        mock_deallocate_minion_pool,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_body = {'deallocate': {}}

        result = testutils.get_wrapped_function(
            self.minion._deallocate_pool)(
                mock_req,
                id,
                mock_body
        )

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:minion_pools:deallocate")
        mock_deallocate_minion_pool.assert_called_once_with(
            mock_context, id, force=False)
        mock_single.assert_called_once_with(
            mock_deallocate_minion_pool.return_value)

    @mock.patch.object(minion_pool_view, 'single')
    @mock.patch.object(api.API, 'deallocate_minion_pool')
    @ddt.file_data('data/minion_pool_exceptions.yml')
    def test__deallocate_pool_raises(
        self,
        exception_raised,
        mock_deallocate_minion_pool,
        mock_single,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_body = {'deallocate': {}}
        expected_exception = getattr(exception, exception_raised)
        mock_deallocate_minion_pool.side_effect = expected_exception('err')

        self.assertRaises(
            exc.HTTPNotFound,
            testutils.get_wrapped_function(self.minion._deallocate_pool),
            mock_req,
            id,
            mock_body
        )

        mock_context.can.assert_called_once_with(
            "migration:minion_pools:deallocate")
        mock_deallocate_minion_pool.assert_called_once_with(
            mock_context, id, force=False)
        mock_single.assert_not_called()
