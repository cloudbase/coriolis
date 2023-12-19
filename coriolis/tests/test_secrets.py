# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

import json
from unittest import mock

from barbicanclient import client as barbican_client
import keystoneauth1

from coriolis import keystone
from coriolis import secrets
from coriolis.tests import test_base


class SecretsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis secrets module."""

    @mock.patch.object(keystone, 'create_keystone_session')
    @mock.patch.object(barbican_client, 'Client')
    def test_get_barbican_secret_payload(self, mock_client, mock_session):
        mock_session.return_value = mock.sentinel.session
        mock_secret = mock.MagicMock()
        mock_secret.payload = mock.sentinel.payload
        mock_barbican = mock.MagicMock()
        mock_barbican.secrets.get.return_value = mock_secret
        mock_client.return_value = mock_barbican

        result = secrets._get_barbican_secret_payload(
            mock.sentinel.ctxt, mock.sentinel.secret_ref)

        self.assertEqual(mock.sentinel.payload, result)
        mock_session.assert_called_once_with(mock.sentinel.ctxt)
        mock_client.assert_called_once_with(session=mock.sentinel.session)
        mock_barbican.secrets.get.assert_called_once_with(
            mock.sentinel.secret_ref)

    @mock.patch.object(secrets, '_get_barbican_secret_payload')
    def test_get_secret_success(self, mock_get_payload):
        mock_get_payload.return_value = json.dumps({'key': 'value'})

        result = secrets.get_secret(mock.sentinel.ctxt,
                                    mock.sentinel.secret_ref)

        self.assertEqual({'key': 'value'}, result)
        mock_get_payload.assert_called_once_with(
            mock.sentinel.ctxt, mock.sentinel.secret_ref)

    @mock.patch.object(secrets, '_get_barbican_secret_payload')
    def test_get_secret_unauthorized(self, mock_get_payload):
        mock_ctxt = mock.MagicMock()
        mock_get_payload.side_effect = [
            keystoneauth1.exceptions.http.Unauthorized,
            json.dumps({'key': 'value'})]

        with mock.patch('copy.deepcopy', side_effect=lambda func: func):
            result = secrets.get_secret(mock_ctxt, mock.sentinel.secret_ref)

        self.assertEqual({'key': 'value'}, result)
        self.assertEqual(2, mock_get_payload.call_count)
        self.assertEqual(mock_ctxt.trust_id, None)
        mock_get_payload.assert_called_with(
            mock_ctxt, mock.sentinel.secret_ref)

    @mock.patch.object(secrets, '_get_barbican_secret_payload')
    def test_get_secret_raises_value_error(self, mock_get_payload):
        mock_get_payload.side_effect = ValueError("Test exception")

        self.assertRaises(ValueError, secrets.get_secret, mock.sentinel.ctxt,
                          mock.sentinel.secret_ref)

        mock_get_payload.assert_called_once_with(
            mock.sentinel.ctxt, mock.sentinel.secret_ref)
