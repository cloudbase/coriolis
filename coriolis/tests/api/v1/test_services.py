# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from webob import exc

from coriolis.api.v1 import services
from coriolis.api.v1.views import service_view
from coriolis import exception
from coriolis.services import api
from coriolis.tests import test_base
from coriolis.tests import testutils


class ServiceControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Service v1 API"""

    def setUp(self):
        super(ServiceControllerTestCase, self).setUp()
        self.services = services.ServiceController()

    @mock.patch.object(service_view, 'single')
    @mock.patch.object(api.API, 'get_service')
    def test_show(
        self,
        mock_get_service,
        mock_single,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id

        result = self.services.show(mock_req, id)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with("migration:services:show")
        mock_get_service.assert_called_once_with(
            mock_context, id)
        mock_single.assert_called_once_with(mock_get_service.return_value)

    @mock.patch.object(service_view, 'single')
    @mock.patch.object(api.API, 'get_service')
    def test_show_no_service(
        self,
        mock_get_service,
        mock_single,
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_get_service.return_value = None

        self.assertRaises(
            exc.HTTPNotFound,
            self.services.show,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with("migration:services:show")
        mock_get_service.assert_called_once_with(
            mock_context, id)
        mock_single.assert_not_called()

    @mock.patch.object(service_view, 'collection')
    @mock.patch.object(api.API, 'get_services')
    def test_index(
        self,
        mock_get_services,
        mock_collection
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}

        result = self.services.index(mock_req)

        self.assertEqual(
            mock_collection.return_value,
            result
        )

        mock_context.can.assert_called_once_with("migration:services:list")
        mock_get_services.assert_called_once_with(mock_context)
        mock_collection.assert_called_once_with(mock_get_services.return_value)

    def test_validate_create_body(
        self,
    ):
        host = mock.sentinel.host
        binary = mock.sentinel.binary
        topic = mock.sentinel.topic
        mapped_regions = ["region_1", "region_2"]
        enabled = False
        body = {
            "service": {
                "host": host,
                "binary": binary,
                "topic": topic,
                "mapped_regions": mapped_regions,
                "enabled": enabled
            }
        }

        result = testutils.get_wrapped_function(
            self.services._validate_create_body)(self.services, body=body)

        self.assertEqual(
            (host, binary, topic, mapped_regions, enabled),
            result
        )

    @mock.patch.object(service_view, 'single')
    @mock.patch.object(api.API, 'create')
    @mock.patch.object(services.ServiceController, '_validate_create_body')
    def test_create(
        self,
        mock_validate_create_body,
        mock_create,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        host = mock.sentinel.host
        binary = mock.sentinel.binary
        topic = mock.sentinel.topic
        mapped_regions = ["region_1", "region_2"]
        enabled = True
        mock_validate_create_body.return_value = (
            host, binary, topic, mapped_regions, enabled)
        service = {
            "host": host,
            "binary": binary,
            "topic": topic,
            "mapped_regions": mapped_regions,
            "enabled": enabled
        }
        body = {"service": service}

        result = self.services.create(mock_req, body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with("migration:services:create")
        mock_validate_create_body.assert_called_once_with(body)
        mock_create.assert_called_once_with(
            mock_context, host=host, binary=binary, topic=topic,
            mapped_regions=mapped_regions, enabled=enabled)
        mock_single.assert_called_once_with(mock_create.return_value)

    def test_validate_update_body(
        self
    ):
        host = mock.sentinel.host
        binary = mock.sentinel.binary
        topic = mock.sentinel.topic
        mapped_regions = ["region_1", "region_2"]
        enabled = True
        service = {
            "host": host,
            "binary": binary,
            "topic": topic,
            "mapped_regions": mapped_regions,
            "enabled": enabled
        }
        body = {"service": service}

        result = testutils.get_wrapped_function(
            self.services._validate_update_body)(self.services, body=body)

        self.assertEqual(
            {'enabled': enabled, 'mapped_regions': mapped_regions},
            result
        )

    def test_validate_update_body_no_keys(
        self
    ):
        service = {}
        body = {"service": service}

        result = testutils.get_wrapped_function(
            self.services._validate_update_body)(self.services, body=body)

        self.assertEqual(
            {},
            result
        )

    @mock.patch.object(service_view, 'single')
    @mock.patch.object(api.API, 'update')
    @mock.patch.object(services.ServiceController, '_validate_update_body')
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

        result = self.services.update(mock_req, id, body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with("migration:services:update")
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
            self.services.delete,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with("migration:services:delete")
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
            self.services.delete,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with("migration:services:delete")
        mock_delete.assert_called_once_with(mock_context, id)
