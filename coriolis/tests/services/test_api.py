# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.


from unittest import mock

from coriolis.services import api as api_service
from coriolis.tests import test_base


class APITestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis API service."""

    def setUp(self):
        super(APITestCase, self).setUp()
        self.api = api_service.API()
        self.rpc_client_mock = mock.Mock()
        self.api._rpc_client = self.rpc_client_mock

    def test_create(self):
        self.api.create('ctxt', 'host', 'binary', 'topic', 'mapped_regions',
                        True)
        self.rpc_client_mock.register_service.assert_called_once_with(
            'ctxt', 'host', 'binary', 'topic', True, 'mapped_regions')

    def test_update(self):
        self.api.update('ctxt', 'service_id', 'updated_values')
        self.rpc_client_mock.update_service.assert_called_once_with(
            'ctxt', 'service_id', 'updated_values')

    def test_delete(self):
        self.api.delete('ctxt', 'region_id')
        self.rpc_client_mock.delete_service.assert_called_once_with(
            'ctxt', 'region_id')

    def test_get_services(self):
        self.api.get_services('ctxt')
        self.rpc_client_mock.get_services.assert_called_once_with('ctxt')

    def test_get_service(self):
        self.api.get_service('ctxt', 'service_id')
        self.rpc_client_mock.get_service.assert_called_once_with(
            'ctxt', 'service_id')
