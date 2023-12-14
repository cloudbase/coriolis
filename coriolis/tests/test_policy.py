# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import policy
from coriolis.tests import test_base


class PolicyTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis policy module."""

    @mock.patch.object(policy, '_ENFORCER')
    def test_reset(self, mock_enforcer):
        result = policy.reset()

        mock_enforcer.clear.assert_called_once_with()
        self.assertEqual(result, None)
        self.assertEqual(policy._ENFORCER, None)

    @mock.patch('oslo_policy.policy.Enforcer')
    @mock.patch.object(policy, 'register_rules')
    @mock.patch.object(policy, 'CONF')
    def test_init(self, mock_conf, mock_register_rules, mock_policy_enforcer):
        mock_enforcer_instance = mock.Mock()
        mock_policy_enforcer.return_value = mock_enforcer_instance

        with mock.patch('coriolis.policy._ENFORCER', None):
            result = policy.init()

        mock_policy_enforcer.assert_called_once_with(mock_conf)
        mock_register_rules.assert_called_once_with(mock_enforcer_instance)
        mock_enforcer_instance.load_rules.assert_called_once_with()
        self.assertEqual(result, None)

    @mock.patch.object(policy, '_ENFORCER')
    def test_init_already_initialized(self, mock_enforcer):
        mock_enforcer_instance = mock.Mock()
        mock_enforcer.return_value = mock_enforcer_instance

        result = policy.init()
        self.assertEqual(result, None)

    @mock.patch.object(policy, 'itertools')
    def test_register_rules(self, mock_itertools):
        mock_enforcer = mock.Mock()
        mock_enforcer.register_defaults.return_value = ['rule1', 'rule2']
        mock_itertools.chain.return_value = ['rule1', 'rule2', 'rule3']

        result = policy.register_rules(mock_enforcer)

        mock_enforcer.register_defaults.assert_called_once_with(
            ['rule1', 'rule2', 'rule3'])
        self.assertEqual(result, None)

    @mock.patch.object(policy, 'init')
    def test_get_enforcer(self, mock_init):
        mock_enforcer = mock.Mock()
        policy._ENFORCER = mock_enforcer

        result = policy.get_enforcer()

        mock_init.assert_called_once_with()
        self.assertEqual(result, mock_enforcer)

    @mock.patch.object(policy, 'init')
    def test_get_enforcer_not_initialized(self, mock_init):
        policy._ENFORCER = None

        result = policy.get_enforcer()

        self.assertEqual(result, None)
        mock_init.assert_called_once_with()

    @mock.patch.object(policy, '_ENFORCER')
    def test_check_policy_for_context(self, mock_enforcer):
        mock_enforcer.authorize.return_value = True

        mock_context = mock.Mock()
        mock_context.to_policy_values.return_value = 'test-credentials'

        result = policy.check_policy_for_context(
            mock_context, 'rule', 'target')

        self.assertEqual(result, True)
        mock_enforcer.authorize.assert_called_once_with(
            'rule', 'target', 'test-credentials', do_raise=True, exc=mock.ANY,
            action='rule')

    @mock.patch.object(policy, '_ENFORCER')
    def test_check_policy_for_context_do_not_raise(self, mock_enforcer):
        mock_enforcer.authorize.return_value = False

        mock_context = mock.Mock()
        mock_context.to_policy_values.return_value = 'test-credentials'

        result = policy.check_policy_for_context(
            mock_context, 'rule', 'target', do_raise=False)

        mock_enforcer.authorize.assert_called_once_with(
            'rule', 'target', 'test-credentials', do_raise=False, exc=mock.ANY,
            action='rule')
        self.assertEqual(result, False)

    @mock.patch.object(policy, '_ENFORCER')
    def test_check_policy_for_context_not_authorized(self, mock_enforcer):
        mock_enforcer.authorize.side_effect = (
            policy.exception.PolicyNotAuthorized)

        mock_context = mock.Mock()
        mock_context.to_policy_values.return_value = 'test-credentials'

        self.assertRaises(
            policy.exception.PolicyNotAuthorized,
            policy.check_policy_for_context, mock_context, 'rule', 'target')

        mock_enforcer.authorize.assert_called_once_with(
            'rule', 'target', 'test-credentials', do_raise=True, exc=mock.ANY,
            action='rule')

    @mock.patch.object(policy, '_ENFORCER')
    def test_check_policy_for_context_custom_exception(self, mock_enforcer):
        mock_enforcer.authorize.side_effect = ValueError

        mock_context = mock.Mock()
        mock_context.to_policy_values.return_value = 'test-credentials'

        self.assertRaises(
            ValueError, policy.check_policy_for_context, mock_context,
            'rule', 'target', exc=ValueError)
