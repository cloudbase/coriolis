# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.regions import api as regions_module
from coriolis.tests import test_base


class APITestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis API class."""

    def setUp(self):
        super(APITestCase, self).setUp()
        self.api = regions_module.API()
        self.rpc_client = mock.MagicMock()
        self.api._rpc_client = self.rpc_client
        self.ctxt = mock.sentinel.ctxt
        self.region_id = mock.sentinel.region_id
        self.region_name = mock.sentinel.region_name

    def test_create(self):
        description = mock.sentinel.description

        result = self.api.create(self.ctxt, self.region_name, description,
                                 enabled=True)

        self.rpc_client.create_region.assert_called_once_with(
            self.ctxt, self.region_name, description=description,
            enabled=True)
        self.assertEqual(result, self.rpc_client.create_region.return_value)

    def test_update(self):
        updated_values = mock.sentinel.updated_values

        result = self.api.update(self.ctxt, self.region_id, updated_values)

        self.rpc_client.update_region.assert_called_once_with(
            self.ctxt, self.region_id, updated_values=updated_values)
        self.assertEqual(result, self.rpc_client.update_region.return_value)

    def test_delete(self):
        self.api.delete(self.ctxt, self.region_id)

        self.rpc_client.delete_region.assert_called_once_with(
            self.ctxt, self.region_id)

    def test_get_regions(self):
        result = self.api.get_regions(self.ctxt)

        self.rpc_client.get_regions.assert_called_once_with(self.ctxt)
        self.assertEqual(result, self.rpc_client.get_regions.return_value)

    def test_get_region(self):
        result = self.api.get_region(self.ctxt, self.region_id)

        self.rpc_client.get_region.assert_called_once_with(
            self.ctxt, self.region_id)
        self.assertEqual(result, self.rpc_client.get_region.return_value)
