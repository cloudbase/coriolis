# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis import context
from coriolis import exception
from coriolis.tests import test_base
from coriolis.tests import testutils


class RequestContextTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis RequestContext class."""

    def setUp(self):
        super(RequestContextTestCase, self).setUp()
        self.req_context = context.RequestContext(
            user="user",
            project_id='test_project_id',
        )

    @mock.patch.object(context.timeutils, 'utcnow')
    def test__init__(self, mock_utcnow):
        context_req = testutils.get_wrapped_function(context.RequestContext)(
            user="user",
            project_id='test_project_id',
            roles=['role1', 'role2'],
        )

        mock_utcnow.assert_called_once_with()
        self.assertEqual(context_req.timestamp, mock_utcnow.return_value)
        self.assertEqual(context_req.roles, ['role1', 'role2'])

    @mock.patch.object(context.timeutils, 'parse_isotime')
    def test__init__with_string_timestamp(self, mock_parse_isotime):
        new_context = testutils.get_wrapped_function(context.RequestContext)(
            user="user",
            project_id='test_project_id',
            timestamp="2023-11-09T13:31:08Z",
        )

        mock_parse_isotime.assert_called_once_with("2023-11-09T13:31:08Z")
        self.assertEqual(new_context.timestamp,
                         mock_parse_isotime.return_value)
        self.assertEqual(new_context.roles, [])

    def test_to_dict(self):
        result = self.req_context.to_dict()

        with mock.patch.object(context, 'RequestContext') as mock_req_context:
            mock_req_context(**result)
            mock_req_context.assert_called_once_with(**result)
        self.assertIsInstance(result, dict)

    @mock.patch.object(context.policy, 'check_policy_for_context')
    def test_can(self, mock_check_policy):
        result = self.req_context.can('test_action')

        mock_check_policy.assert_called_once_with(
            self.req_context, 'test_action', {'project_id': 'test_project_id',
                                              'user_id': 'user'})

        self.assertEqual(result, mock_check_policy.return_value)

    @mock.patch.object(context.policy, 'check_policy_for_context')
    def test_can_policy_not_authorized(self, mock_check_policy):
        mock_check_policy.side_effect = exception.PolicyNotAuthorized(
            action='test_action')

        self.assertRaises(exception.PolicyNotAuthorized,
                          self.req_context.can, 'test_action')

        mock_check_policy.assert_called_once_with(
            self.req_context, 'test_action', {'project_id': 'test_project_id',
                                              'user_id': 'user'})

    @mock.patch.object(context.policy, 'check_policy_for_context')
    def test_can_policy_with_custom_target(self, mock_check_policy):
        custom_target = {'custom_key': 'custom_value'}
        result = self.req_context.can('test_action', custom_target,
                                      fatal=False)

        expected_target = {'project_id': 'test_project_id',
                           'user_id': 'user',
                           'custom_key': 'custom_value'}

        mock_check_policy.assert_called_once_with(
            self.req_context, 'test_action', expected_target)
        self.assertEqual(result, mock_check_policy.return_value)


class GetAdminContextTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis get_admin_context function."""

    @mock.patch.object(context, 'RequestContext')
    def test_get_admin_context(self, mock_request_context):
        result = context.get_admin_context()

        mock_request_context.assert_called_once_with(
            user=None, project_id=None, is_admin=True, trust_id=None)
        self.assertEqual(result, mock_request_context.return_value)
