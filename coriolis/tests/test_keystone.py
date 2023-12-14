# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

from keystoneauth1 import exceptions as ks_exceptions

from coriolis import exception
from coriolis import keystone
from coriolis.tests import test_base


class KeystoneTestCase(test_base.CoriolisBaseTestCase):
    """Collection of tests for the Coriolis Keystone module."""

    def setUp(self):
        super(KeystoneTestCase, self).setUp()
        self.ctxt = mock.Mock()
        self.ctxt.trust_id = None
        self.ctxt.auth_token = 'test_token'
        self.ctxt.project_name = 'test_project'
        self.ctxt.project_domain_name = 'test_domain'
        self.ctxt.user = 'test_user'
        self.ctxt.project_id = 'test_project_id'
        self.ctxt.roles = ['test_role']
        self.connection_info = {
            'auth_url': 'test_auth_url',
            'username': 'test_username',
            'password': 'test_password',
            'project_name': 'test_project_name'}

        self.trusts_auth_plugin = mock.Mock()
        self.trusts_auth_plugin.auth_url = 'test_auth_url'
        self.trusts_auth_plugin.get_user_id.return_value = 'test_trust_user_id'

        self.auth = mock.Mock()
        self.session = mock.Mock()
        self.client = mock.Mock()
        self.trust = mock.Mock()

        self.trust.id = 'test_trust_id'
        self.client.trusts.create.return_value = self.trust

    @mock.patch.object(keystone.loading, 'load_auth_from_conf_options')
    def test_get_trusts_auth_plugin(self, mock_load_auth_from_conf_options):
        mock_load_auth_from_conf_options.return_value = self.trusts_auth_plugin

        result = keystone._get_trusts_auth_plugin()

        mock_load_auth_from_conf_options.assert_called_once_with(
            keystone.CONF, keystone.TRUSTEE_CONF_GROUP, trust_id=None)
        self.assertEqual(result, self.trusts_auth_plugin)

    @mock.patch('coriolis.keystone._get_trusts_auth_plugin')
    @mock.patch.object(keystone.loading, 'get_plugin_loader')
    @mock.patch.object(keystone.ks_session, 'Session')
    @mock.patch.object(keystone.kc_v3, 'Client')
    def test_create_trust(self, mock_client, mock_session,
                          mock_get_plugin_loader, mock_get_trusts_auth_plugin):
        mock_get_trusts_auth_plugin.return_value = self.trusts_auth_plugin
        mock_get_plugin_loader.return_value.\
            load_from_options.return_value = self.auth
        mock_session.return_value = self.session
        mock_client.return_value = self.client

        keystone.create_trust(self.ctxt)

        mock_get_trusts_auth_plugin.assert_called_once_with()
        mock_get_plugin_loader.assert_called_once_with("v3token")
        mock_get_plugin_loader.return_value.\
            load_from_options.assert_called_once_with(
                auth_url=self.trusts_auth_plugin.auth_url,
                token=self.ctxt.auth_token,
                project_name=self.ctxt.project_name,
                project_domain_name=self.ctxt.project_domain_name)
        mock_session.assert_called_once_with(
            auth=self.auth, verify=not keystone.CONF.keystone.allow_untrusted)
        mock_client.assert_called_once_with(session=self.session)
        self.client.trusts.create.assert_called_once_with(
            trustor_user=self.ctxt.user,
            trustee_user=self.trusts_auth_plugin.get_user_id.return_value,
            project=self.ctxt.project_id,
            impersonation=True,
            role_names=self.ctxt.roles)
        self.assertEqual(self.ctxt.trust_id, self.trust.id)

    def test_create_trust_with_existing_trust_id(self):
        self.ctxt.trust_id = 'test_trust_id'
        keystone.create_trust(self.ctxt)
        self.assertEqual(self.ctxt.trust_id, 'test_trust_id')

    @mock.patch('coriolis.keystone._get_trusts_auth_plugin')
    @mock.patch.object(keystone.kc_v3, 'Client')
    @mock.patch.object(keystone.loading, 'get_plugin_loader')
    @mock.patch.object(keystone.ks_session, 'Session')
    def test_create_trust_unauthorized_exception(self,
                                                 mock_session,
                                                 mock_get_plugin_loader,
                                                 mock_client,
                                                 mock_get_trusts_auth_plugin):
        mock_get_trusts_auth_plugin.return_value = self.trusts_auth_plugin
        mock_get_plugin_loader.return_value.\
            load_from_options.return_value = self.auth
        mock_session.return_value = self.session
        mock_client.return_value = self.client
        self.trusts_auth_plugin.get_user_id.side_effect = \
            ks_exceptions.Unauthorized

        self.assertRaises(exception.NotAuthorized, keystone.create_trust,
                          self.ctxt)

    @mock.patch('coriolis.keystone._get_trusts_auth_plugin')
    @mock.patch.object(keystone.kc_v3, 'Client')
    @mock.patch.object(keystone.ks_session, 'Session')
    def test_delete_trust(self, mock_session, mock_client,
                          mock_get_trusts_auth_plugin):
        mock_get_trusts_auth_plugin.return_value = self.trusts_auth_plugin
        self.ctxt.trust_id = 'test_trust_id'
        mock_session.return_value = self.session
        mock_client.return_value = self.client

        keystone.delete_trust(self.ctxt)

        mock_get_trusts_auth_plugin.assert_called_once_with('test_trust_id')
        mock_session.assert_called_once_with(
            auth=self.trusts_auth_plugin, verify=True)
        mock_client.assert_called_once_with(session=self.session)
        self.client.trusts.delete.assert_called_once_with('test_trust_id')
        self.assertEqual(self.ctxt.trust_id, None)

    @mock.patch('coriolis.keystone._get_trusts_auth_plugin')
    @mock.patch.object(keystone.kc_v3, 'Client')
    @mock.patch.object(keystone.ks_session, 'Session')
    def test_delete_trust_with_not_found_exception(
            self, mock_session, mock_client, mock_get_trusts_auth_plugin):
        mock_get_trusts_auth_plugin.return_value = self.trusts_auth_plugin
        self.ctxt.trust_id = 'test_trust_id'
        mock_session.return_value = self.session
        mock_client.return_value = self.client
        self.client.trusts.delete.side_effect = ks_exceptions.NotFound

        with self.assertLogs('coriolis.keystone', level=logging.WARN):
            keystone.delete_trust(self.ctxt)

        mock_get_trusts_auth_plugin.assert_called_once_with('test_trust_id')
        mock_session.assert_called_once_with(
            auth=self.trusts_auth_plugin, verify=True)
        mock_client.assert_called_once_with(session=self.session)
        self.client.trusts.delete.assert_called_once_with('test_trust_id')
        self.assertEqual(self.ctxt.trust_id, None)

    @mock.patch.object(keystone.ks_session, 'Session')
    @mock.patch.object(keystone.loading, 'get_plugin_loader')
    def test_create_keystone_session(self, mock_get_plugin_loader,
                                     mock_session):
        mock_get_plugin_loader.return_value.\
            load_from_options.return_value = self.auth
        mock_session.return_value = self.session

        result = keystone.create_keystone_session(self.ctxt,
                                                  self.connection_info)

        mock_get_plugin_loader.assert_called_once_with("password")
        mock_get_plugin_loader.return_value.\
            load_from_options.assert_called_once_with(
                auth_url=self.connection_info['auth_url'],
                username=self.connection_info['username'],
                password=self.connection_info['password'],
                project_name=self.connection_info['project_name'])

        mock_session.assert_called_once_with(
            auth=self.auth, verify=not keystone.CONF.keystone.allow_untrusted)
        self.assertEqual(result, self.session)

    @mock.patch('coriolis.keystone._get_trusts_auth_plugin')
    @mock.patch.object(keystone.ks_session, 'Session')
    @mock.patch.object(keystone.loading, 'get_plugin_loader')
    def test_create_keystone_session_with_trust_id_no_username(
            self, mock_get_plugin_loader, mock_session,
            mock_get_trusts_auth_plugin):
        mock_get_plugin_loader.return_value.\
            load_from_options.return_value = self.auth
        mock_session.return_value = self.session
        mock_get_trusts_auth_plugin.return_value = self.trusts_auth_plugin

        self.ctxt.trust_id = 'test_trust_id'
        self.ctxt.username = None
        self.connection_info = {}

        result = keystone.create_keystone_session(self.ctxt,
                                                  self.connection_info)

        mock_get_trusts_auth_plugin.assert_called_once_with('test_trust_id')
        mock_session.assert_called_once_with(
            auth=self.trusts_auth_plugin, verify=True)
        self.assertEqual(result, mock_session.return_value)

    @mock.patch.object(keystone.ks_session, 'Session')
    @mock.patch.object(keystone.loading, 'get_plugin_loader')
    def test_create_keystone_session_with_connection_info_no_username(
            self, mock_get_plugin_loader, mock_session):
        mock_get_plugin_loader.return_value.\
            load_from_options.return_value = self.auth
        mock_session.return_value = self.session

        connection_info = self.connection_info.copy()
        connection_info.pop('username')

        result = keystone.create_keystone_session(self.ctxt,
                                                  connection_info)

        mock_get_plugin_loader.assert_called_once_with("token")
        mock_get_plugin_loader.return_value.\
            load_from_options.assert_called_once_with(
                auth_url=self.connection_info['auth_url'],
                token=self.ctxt.auth_token,
                project_name=self.connection_info['project_name'])

        mock_session.assert_called_once_with(
            auth=self.auth, verify=not keystone.CONF.keystone.allow_untrusted)
        self.assertEqual(result, self.session)

    @mock.patch.object(keystone.ks_session, 'Session')
    @mock.patch.object(keystone.loading, 'get_plugin_loader')
    def test_create_keystone_session_with_connection_info_no_auth_url(
            self, mock_get_plugin_loader, mock_session):
        keystone.CONF.keystone.auth_url = None
        mock_get_plugin_loader.return_value.\
            load_from_options.return_value = self.auth
        mock_session.return_value = self.session

        connection_info = self.connection_info.copy()
        connection_info.pop('auth_url')

        self.assertRaises(exception.CoriolisException,
                          keystone.create_keystone_session,
                          self.ctxt, connection_info)

    @mock.patch.object(keystone.ks_session, 'Session')
    @mock.patch.object(keystone.loading, 'get_plugin_loader')
    def test_create_keystone_session_version_3(self, mock_get_plugin_loader,
                                               mock_session):
        mock_get_plugin_loader.return_value.\
            load_from_options.return_value = self.auth
        mock_session.return_value = self.session
        self.connection_info['identity_api_version'] = 3

        result = keystone.create_keystone_session(self.ctxt,
                                                  self.connection_info)

        mock_get_plugin_loader.assert_called_once_with("v3password")
        mock_get_plugin_loader.return_value.\
            load_from_options.assert_called_once_with(
                auth_url=self.connection_info['auth_url'],
                username=self.connection_info['username'],
                password=self.connection_info['password'],
                project_name=self.connection_info['project_name'],
                project_domain_name=self.ctxt.project_domain_name,
                project_domain_id=self.ctxt.project_domain_id,
                user_domain_name=self.ctxt.user_domain_name,
                user_domain_id=self.ctxt.user_domain_id)
        mock_session.assert_called_once_with(
            auth=self.auth, verify=not keystone.CONF.keystone.allow_untrusted)
        self.assertEqual(result, self.session)

    @mock.patch.object(keystone.ks_session, 'Session')
    @mock.patch.object(keystone.loading, 'get_plugin_loader')
    def test_create_keystone_session_no_project_domain_name_and_id(
            self, mock_get_plugin_loader, mock_session):
        mock_get_plugin_loader.return_value.\
            load_from_options.return_value = self.auth
        mock_session.return_value = self.session

        self.connection_info['identity_api_version'] = 3
        self.ctxt.project_domain_name = None
        self.ctxt.project_domain_id = None

        self.assertRaises(exception.CoriolisException,
                          keystone.create_keystone_session,
                          self.ctxt, self.connection_info)

    @mock.patch.object(keystone.ks_session, 'Session')
    @mock.patch.object(keystone.loading, 'get_plugin_loader')
    def test_create_keystone_session_no_user_domain_name_and_id(
            self, mock_get_plugin_loader, mock_session):
        mock_get_plugin_loader.return_value.\
            load_from_options.return_value = self.auth
        mock_session.return_value = self.session

        self.connection_info['identity_api_version'] = 3
        self.ctxt.user_domain_name = None
        self.ctxt.user_domain_id = None

        self.assertRaises(exception.CoriolisException,
                          keystone.create_keystone_session,
                          self.ctxt, self.connection_info)
