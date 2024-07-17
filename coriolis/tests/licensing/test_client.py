# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
import os
from unittest import mock

from coriolis import exception
from coriolis.licensing import client as licensing_module
from coriolis.tests import test_base
from coriolis.tests import testutils


class LicensingClientTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the LicensingClient class."""

    def setUp(self):
        super(LicensingClientTestCase, self).setUp()
        self.client = licensing_module.LicensingClient(
            "https://10.7.2.3:37667/v1")
        self.resource = "licences"
        self.body = {'test_key': 'test_value'}
        self.reservation_id = "reservation_id"
        self.client._appliance_id = "appliance_id"
        licensing_module.CONF = mock.Mock()

    # Helper function to setup a mock response
    def setup_mock_response(self, mock_request, ok=True,
                            status_code=200, json_return_value=None,
                            side_effect=None):
        mock_resp = mock.MagicMock()
        mock_resp.ok = ok
        mock_resp.status_code = status_code
        if side_effect:
            mock_resp.json.side_effect = side_effect
        else:
            mock_resp.json.return_value = json_return_value
        mock_request.return_value = mock_resp

        return mock_resp

    @mock.patch.object(licensing_module.LicensingClient, 'get_appliances')
    @mock.patch.object(licensing_module.LicensingClient, 'create_appliance')
    @mock.patch.object(licensing_module.LicensingClient, 'get_licence_status')
    def test_from_env(self, mock_get_licence_status, mock_create_appliance,
                      mock_get_appliances):
        os.environ["LICENSING_SERVER_BASE_URL"] = "https://10.7.2.3:37667/v1"
        mock_get_appliances.return_value = [{"id": "appliance_id1"}]
        mock_create_appliance.return_value = {"id": "appliance_id2"}

        result = licensing_module.LicensingClient.from_env()

        mock_get_appliances.assert_called_once()
        mock_create_appliance.assert_not_called()
        mock_get_licence_status.assert_called_once()
        self.assertEqual(result._appliance_id, "appliance_id1")

    def test_from_env_no_base_url(self):
        os.environ["LICENSING_SERVER_BASE_URL"] = ""
        with self.assertLogs('coriolis.licensing.client',
                             level=logging.WARN):
            result = licensing_module.LicensingClient.from_env()
            self.assertIsNone(result)

    @mock.patch.object(licensing_module.LicensingClient, 'get_appliances')
    def test_from_env_multiple_appliances(self, mock_get_appliances):
        os.environ["LICENSING_SERVER_BASE_URL"] = "https://10.7.2.3:37667/v1"
        mock_get_appliances.return_value = [
            {"id": "appliance_id1"}, {"id": "appliance_id2"}
        ]

        self.assertRaises(exception.CoriolisException,
                          licensing_module.LicensingClient.from_env)

    @mock.patch.object(licensing_module.LicensingClient, 'get_appliances')
    @mock.patch.object(licensing_module.LicensingClient, 'create_appliance')
    @mock.patch.object(licensing_module.LicensingClient, 'get_licence_status')
    def test_from_env_no_appliances(self, mock_get_licence_status,
                                    mock_create_appliance,
                                    mock_get_appliances):
        os.environ["LICENSING_SERVER_BASE_URL"] = "https://10.7.2.3:37667/v1"
        mock_get_appliances.return_value = []
        mock_create_appliance.return_value = {"id": "new_appliance_id"}

        result = licensing_module.LicensingClient.from_env()

        mock_get_appliances.assert_called_once()
        mock_create_appliance.assert_called_once()
        mock_get_licence_status.assert_called_once()
        self.assertEqual(
            result._appliance_id, mock_create_appliance.return_value["id"]
        )

    def test__get_url_for_resource(self):
        result = self.client._get_url_for_resource(self.resource)
        self.assertEqual(result, "https://10.7.2.3:37667/v1/licences")

    def test__get_url_for_appliance_resource(self):
        result = self.client._get_url_for_appliance_resource(self.resource)
        self.assertEqual(
            result,
            "https://10.7.2.3:37667/v1/appliances/appliance_id/licences"
        )

    def test_raise_response_error(self):
        mock_response = mock.Mock()
        mock_response.json.return_value = {'test_key': 'test_value'}

        self.client._raise_response_error(mock_response)

        mock_response.raise_for_status.assert_called_once()

    def test_raise_response_error_conflict(self):
        mock_response = mock.Mock()
        mock_response.json.return_value = {
            'error': {'code': 409, 'message': 'test_message'}}

        self.assertRaises(
            exception.Conflict,
            self.client._raise_response_error,
            mock_response
        )

        mock_response.raise_for_status.assert_not_called()

    def test_raise_response_error_json_exception(self):
        mock_response = mock.Mock()
        mock_response.json.side_effect = KeyboardInterrupt()

        with self.assertLogs('coriolis.licensing.client', level=logging.DEBUG):
            self.client._raise_response_error(mock_response)

        mock_response.raise_for_status.assert_called_once()

        mock_response.reset_mock()
        mock_response.json.side_effect = Exception()

        with self.assertLogs('coriolis.licensing.client', level=logging.DEBUG):
            self.client._raise_response_error(mock_response)

        mock_response.raise_for_status.assert_called_once()

    @mock.patch.object(licensing_module.LicensingClient,
                       '_raise_response_error')
    @mock.patch('requests.post')
    def test__do_req(
        self,
        mock_post,
        mock_raise_response_error
    ):
        mock_response = self.setup_mock_response(
            mock_post, json_return_value={'test_key': 'test_value'})

        original_do_req = testutils.get_wrapped_function(self.client._do_req)

        with self.assertLogs('coriolis.licensing.client', level=logging.DEBUG):
            result = original_do_req(
                self.client, 'POST', self.resource,
                body=self.body,
                response_key='test_key'
            )
        self.assertEqual(result, 'test_value')

        mock_post.assert_called_once_with(
            self.client._get_url_for_appliance_resource(self.resource),
            verify=True,
            timeout=licensing_module.CONF.default_requests_timeout,
            data=licensing_module.json.dumps(self.body)
        )
        mock_response.json.assert_called_once()
        mock_raise_response_error.assert_not_called()

    def test__do_req_invalid_method(self):
        original_do_req = testutils.get_wrapped_function(self.client._do_req)

        self.assertRaises(
            ValueError, original_do_req, self.client, 'INVALID_METHOD',
            self.resource
        )

    @mock.patch.object(licensing_module.LicensingClient,
                       '_raise_response_error')
    @mock.patch('requests.get')
    def test__do_req_raw_response(
        self,
        mock_get,
        mock_raise_response_error
    ):
        mock_response = self.setup_mock_response(mock_get)

        original_do_req = testutils.get_wrapped_function(self.client._do_req)

        result = original_do_req(
            self.client, 'GET', self.resource, raw_response=True
        )
        self.assertEqual(result, mock_get.return_value)

        mock_get.assert_called_once_with(
            self.client._get_url_for_appliance_resource(self.resource),
            verify=True,
            timeout=licensing_module.CONF.default_requests_timeout
        )
        mock_response.json.assert_not_called()
        mock_raise_response_error.assert_not_called()

    @mock.patch.object(licensing_module.LicensingClient,
                       '_raise_response_error')
    @mock.patch('requests.get')
    def test__do_req_response_not_ok(
        self,
        mock_get,
        mock_raise_response_error
    ):
        mock_response = self.setup_mock_response(
            mock_get, ok=False, status_code=409,
            json_return_value={
                'error': {'code': 409, 'message': 'test_message'}}
        )

        original_do_req = testutils.get_wrapped_function(self.client._do_req)

        result = original_do_req(self.client, 'GET', self.resource)

        self.assertEqual(
            result,
            {'error': {'code': 409, 'message': 'test_message'}}
        )
        mock_raise_response_error.assert_called_once_with(mock_response)

    @mock.patch.object(licensing_module.LicensingClient,
                       '_raise_response_error')
    @mock.patch('requests.get')
    def test__do_req_no_response_key(
        self,
        mock_get,
        mock_raise_response_error
    ):
        mock_response = self.setup_mock_response(
            mock_get, json_return_value={'test_key': 'test_value'})

        original_do_req = testutils.get_wrapped_function(self.client._do_req)

        self.assertRaises(
            ValueError, original_do_req, self.client, 'GET', self.resource,
            response_key='nonexistent_key'
        )

        mock_response.json.assert_called_once()
        mock_raise_response_error.assert_not_called()

    @mock.patch.object(licensing_module.LicensingClient, '_do_req')
    def test__get(self, mock_do_req):
        result = self.client._get(self.resource)
        self.assertEqual(result, mock_do_req.return_value)

        mock_do_req.assert_called_once_with(
            'GET', self.resource, response_key=None, appliance_scoped=True
        )

    @mock.patch.object(licensing_module.LicensingClient, '_do_req')
    def test__post(self, mock_do_req):
        result = self.client._post(self.resource, body=self.body)
        self.assertEqual(result, mock_do_req.return_value)

        mock_do_req.assert_called_once_with(
            'POST', self.resource, body=self.body,
            response_key=None, appliance_scoped=True
        )

    @mock.patch.object(licensing_module.LicensingClient, '_do_req')
    def test__put(self, mock_do_req):
        result = self.client._put(self.resource, body=self.body)
        self.assertEqual(result, mock_do_req.return_value)

        mock_do_req.assert_called_once_with(
            'PUT', self.resource, body=self.body,
            response_key=None, appliance_scoped=True
        )

    @mock.patch.object(licensing_module.LicensingClient, '_do_req')
    def test__delete(self, mock_do_req):
        result = self.client._delete(self.resource, body=self.body)
        self.assertEqual(result, mock_do_req.return_value)

        mock_do_req.assert_called_once_with(
            'DELETE', self.resource, body=self.body,
            response_key=None, appliance_scoped=True
        )

    @mock.patch.object(licensing_module.LicensingClient, '_get')
    def test_get_appliances(self, mock_get):
        result = self.client.get_appliances()
        self.assertEqual(result, mock_get.return_value)

        mock_get.assert_called_once_with(
            '/appliances', response_key='appliances', appliance_scoped=False
        )

    @mock.patch.object(licensing_module.LicensingClient, '_get')
    def test_get_appliance(self, mock_get):
        result = self.client.get_appliance()
        self.assertEqual(result, mock_get.return_value)

        mock_get.assert_called_once_with(
            f'/appliances/{self.client._appliance_id}',
            response_key='appliance',
            appliance_scoped=False
        )

    @mock.patch.object(licensing_module.LicensingClient, '_post')
    def test_create_appliance(self, mock_post):
        result = self.client.create_appliance()
        self.assertEqual(result, mock_post.return_value)

        mock_post.assert_called_once_with(
            '/appliances', body=None, response_key='appliance',
            appliance_scoped=False
        )

    @mock.patch.object(licensing_module.LicensingClient, '_get')
    def test_get_licence_status(self, mock_get):
        result = self.client.get_licence_status()
        self.assertEqual(result, mock_get.return_value)

        mock_get.assert_called_once_with(
            '/status', 'appliance_licence_status'
        )

    @mock.patch.object(licensing_module.LicensingClient, '_get')
    def test_get_licences(self, mock_get):
        result = self.client.get_licences()
        self.assertEqual(result, mock_get.return_value)

        mock_get.assert_called_once_with(
            '/licences', response_key='licences'
        )

    @mock.patch.object(licensing_module.LicensingClient, '_post')
    def test_add_licence(self, mock_post):
        licence_data = (
            "-----BEGIN CERTIFICATE-----\n\
            MIIDXTCCAkWgAwIBAgIJAJC1HiE2u90vMA0GCSqGSIb3DQEBCwUAMEUxCzAJBgNV\n\
            -----END CERTIFICATE-----"
        )
        result = self.client.add_licence(licence_data)
        self.assertEqual(result, mock_post.return_value)

        mock_post.assert_called_once_with(
            '/licences', licence_data
        )

    @mock.patch.object(licensing_module.LicensingClient, '_post')
    def test_add_reservation(self, mock_post):
        result = self.client.add_reservation(
            licensing_module.RESERVATION_TYPE_REPLICA, 2)
        self.assertEqual(result, mock_post.return_value)

        mock_post.assert_called_once_with(
            '/reservations',
            {
                'type': licensing_module.RESERVATION_TYPE_REPLICA,
                'count': 2
            },
            response_key='reservation'
        )

    def test_add_reservation_invalid_type(self):
        self.assertRaises(
            ValueError, self.client.add_reservation,
            'invalid_reservation_type', mock.sentinel.num_vms
        )

    @mock.patch.object(licensing_module.LicensingClient, 'add_reservation')
    def test_add_migrations_reservation(self, mock_add_reservation):
        result = self.client.add_migrations_reservation(2)
        self.assertEqual(result, mock_add_reservation.return_value)

        mock_add_reservation.assert_called_once_with(
            licensing_module.RESERVATION_TYPE_MIGRATION, 2
        )

    @mock.patch.object(licensing_module.LicensingClient, 'add_reservation')
    def test_add_replicas_reservation(self, mock_add_reservation):
        result = self.client.add_replicas_reservation(2)
        self.assertEqual(result, mock_add_reservation.return_value)

        mock_add_reservation.assert_called_once_with(
            licensing_module.RESERVATION_TYPE_REPLICA, 2
        )

    @mock.patch.object(licensing_module.LicensingClient, '_get')
    def test_get_reservations(self, mock_get):
        result = self.client.get_reservations()
        self.assertEqual(result, mock_get.return_value)

        mock_get.assert_called_once_with(
            '/reservations', response_key='reservations'
        )

    @mock.patch.object(licensing_module.LicensingClient, '_get')
    def test_get_reservation(self, mock_get):
        result = self.client.get_reservation(self.reservation_id)
        self.assertEqual(result, mock_get.return_value)

        mock_get.assert_called_once_with(
            f'/reservations/{self.reservation_id}', response_key='reservation'
        )

    @mock.patch.object(licensing_module.LicensingClient, '_post')
    def test_check_refresh_reservation(self, mock_post):
        result = self.client.check_refresh_reservation(self.reservation_id)
        self.assertEqual(result, mock_post.return_value)

        mock_post.assert_called_once_with(
            f'/reservations/{self.reservation_id}/refresh',
            None, response_key='reservation'
        )

    @mock.patch.object(licensing_module.LicensingClient,
                       '_raise_response_error')
    @mock.patch.object(licensing_module.LicensingClient, '_do_req')
    def test_delete_reservation_404_status_code(
        self,
        mock_do_req,
        mock_raise_response_error
    ):
        self.setup_mock_response(mock_do_req, ok=False, status_code=404)

        with self.assertLogs('coriolis.licensing.client', level=logging.WARN):
            self.client.delete_reservation(
                self.reservation_id, raise_on_404=False)

        mock_do_req.assert_called_once_with(
            'delete', '/reservations/%s' % self.reservation_id,
            raw_response=True
        )
        mock_raise_response_error.assert_not_called()

    @mock.patch.object(licensing_module.LicensingClient,
                       '_raise_response_error')
    @mock.patch.object(licensing_module.LicensingClient, '_do_req')
    def test_delete_reservation_404_status_code_raise(
        self,
        mock_do_req,
        mock_raise_response_error
    ):
        mock_response = self.setup_mock_response(
            mock_do_req, ok=False, status_code=404)

        with self.assertLogs('coriolis.licensing.client', level=logging.WARN):
            self.client.delete_reservation(
                self.reservation_id, raise_on_404=True)

        mock_do_req.assert_called_once_with(
            'delete', '/reservations/%s' % self.reservation_id,
            raw_response=True
        )
        mock_raise_response_error.assert_called_once_with(mock_response)

    @mock.patch.object(licensing_module.LicensingClient,
                       '_raise_response_error')
    @mock.patch.object(licensing_module.LicensingClient, '_do_req')
    def test_delete_reservation_status_code_raise(
        self,
        mock_do_req,
        mock_raise_response_error
    ):
        mock_response = self.setup_mock_response(
            mock_do_req, ok=False, status_code=400)

        self.client.delete_reservation(
            self.reservation_id, raise_on_404=True)

        mock_do_req.assert_called_once_with(
            'delete', '/reservations/%s' % self.reservation_id,
            raw_response=True
        )
        mock_raise_response_error.assert_called_once_with(mock_response)

    @mock.patch.object(licensing_module.LicensingClient,
                       '_raise_response_error')
    @mock.patch.object(licensing_module.LicensingClient, '_do_req')
    def test_delete_reservation(
        self,
        mock_do_req,
        mock_raise_response_error
    ):
        mock_response = self.setup_mock_response(
            mock_do_req, ok=False, status_code=200)

        self.client.delete_reservation(self.reservation_id, raise_on_404=True)

        mock_do_req.assert_called_once_with(
            'delete', '/reservations/%s' % self.reservation_id,
            raw_response=True
        )
        mock_raise_response_error.assert_called_once_with(mock_response)
