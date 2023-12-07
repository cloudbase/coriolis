# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from webob import exc

from coriolis.api.v1 import regions
from coriolis.api.v1.views import region_view
from coriolis import exception
from coriolis.regions import api
from coriolis.tests import test_base
from coriolis.tests import testutils


class RegionControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Regions v1 API"""

    def setUp(self):
        super(RegionControllerTestCase, self).setUp()
        self.regions = regions.RegionController()

    @mock.patch.object(region_view, 'single')
    @mock.patch.object(api.API, 'get_region')
    def test_show(
        self,
        mock_get_region,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id

        result = self.regions.show(mock_req, id)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with("migration:regions:show")
        mock_get_region.assert_called_once_with(mock_context, id)
        mock_single.assert_called_once_with(mock_get_region.return_value)

    @mock.patch.object(api.API, 'get_region')
    def test_show_not_found(
        self,
        mock_get_region
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_get_region.return_value = None

        self.assertRaises(
            exc.HTTPNotFound,
            self.regions.show,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with("migration:regions:show")
        mock_get_region.assert_called_once_with(mock_context, id)

    @mock.patch.object(region_view, 'collection')
    @mock.patch.object(api.API, 'get_regions')
    def test_index(
        self,
        mock_get_regions,
        mock_collection
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}

        result = self.regions.index(mock_req)

        self.assertEqual(
            mock_collection.return_value,
            result
        )

        mock_context.can.assert_called_once_with("migration:regions:list")
        mock_get_regions.assert_called_once_with(mock_context)
        mock_collection.assert_called_once_with(mock_get_regions.return_value)

    def test_validate_create_body(
        self,
    ):
        name = mock.sentinel.name
        description = mock.sentinel.description
        enabled = False
        body = {
            "region": {
                "name": name,
                "description": description,
                "enabled": enabled
            }
        }

        result = testutils.get_wrapped_function(
            self.regions._validate_create_body)(self.regions, body=body)

        self.assertEqual(
            (name, description, enabled),
            result
        )

    @mock.patch.object(region_view, 'single')
    @mock.patch.object(api.API, 'create')
    @mock.patch.object(regions.RegionController, '_validate_create_body')
    def test_create(
        self,
        mock_validate_create_body,
        mock_create,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        name = mock.sentinel.name
        description = mock.sentinel.description
        enabled = True
        mock_validate_create_body.return_value = (name, description, enabled)
        region = {
            "name": name,
            "description": description,
            "enabled": enabled
        }
        body = {"region": region}

        result = self.regions.create(mock_req, body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with("migration:regions:create")
        mock_validate_create_body.assert_called_once_with(body)
        mock_create.assert_called_once_with(
            mock_context, region_name=name, description=description,
            enabled=enabled)
        mock_single.assert_called_once_with(mock_create.return_value)

    def test_validate_update_body(
        self,
    ):
        name = mock.sentinel.name
        description = mock.sentinel.description
        enabled = True
        region = {
            "name": name,
            "description": description,
            "enabled": enabled,
            "mock_key": "mock_value"
        }
        expected_result = {
            "name": name,
            "description": description,
            "enabled": enabled,
        }
        body = {"region": region}

        result = testutils.get_wrapped_function(
            self.regions._validate_update_body)(self.regions, body=body)

        self.assertEqual(
            expected_result,
            result
        )

    @mock.patch.object(region_view, 'single')
    @mock.patch.object(api.API, 'update')
    @mock.patch.object(regions.RegionController, '_validate_update_body')
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

        result = self.regions.update(mock_req, id, body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with("migration:regions:update")
        mock_validate_update_body.assert_called_once_with(body)
        mock_update.assert_called_once_with(
            mock_context, id,
            mock_validate_update_body.return_value)
        mock_single.assert_called_once_with(mock_update.return_value)

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
            self.regions.delete,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with("migration:regions:delete")
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
            self.regions.delete,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with("migration:regions:delete")
        mock_delete.assert_called_once_with(mock_context, id)
