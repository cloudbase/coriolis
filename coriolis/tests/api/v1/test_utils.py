# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import ddt
from webob import exc

from coriolis.api.v1 import utils
from coriolis import exception
from coriolis import schemas
from coriolis.tests import test_base


@ddt.ddt
class UtilsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Utils v1 API"""

    def test_get_bool_url_arg_(
        self
    ):
        mock_req = mock.Mock()
        mock_req.GET.get.return_value = "true"
        arg_name = "show_deleted"

        result = utils.get_bool_url_arg(mock_req, arg_name, default=False)

        self.assertEqual(
            True,
            result
        )
        mock_req.GET.get.assert_called_once_with(arg_name, False)

    def test_get_bool_url_arg_false(
        self
    ):
        mock_req = mock.Mock()
        mock_req.GET.get.return_value = False
        arg_name = "show_deleted"

        result = utils.get_bool_url_arg(mock_req, arg_name, False)

        self.assertEqual(
            False,
            result
        )
        mock_req.GET.get.assert_called_once_with(arg_name, False)

    def test_get_bool_url_arg_invalid_json(
        self
    ):
        mock_req = mock.MagicMock()
        mock_req.GET.get.return_value = "}invalid{"
        arg_name = "include_task_info"

        with self.assertLogs('coriolis.api.v1.utils', level='WARN'):
            result = utils.get_bool_url_arg(mock_req, arg_name, default=False)

        self.assertEqual(
            False,
            result
        )
        mock_req.GET.get.assert_called_once_with(arg_name, False)

    @mock.patch.object(schemas, 'validate_value')
    def test_validate_network_map(
        self,
        mock_validate_value
    ):
        network_map = "mock_network_map"

        utils.validate_network_map(network_map)

        mock_validate_value.assert_called_once_with(
            network_map, schemas.CORIOLIS_NETWORK_MAP_SCHEMA)

    @mock.patch.object(schemas, 'validate_value')
    def test_validate_network_map_schema_validation_exception(
        self,
        mock_validate_value
    ):
        network_map = "mock_network_map"
        mock_validate_value.side_effect = exception.SchemaValidationException()

        self.assertRaises(
            exc.HTTPBadRequest,
            utils.validate_network_map,
            network_map
        )

        mock_validate_value.assert_called_once_with(
            network_map, schemas.CORIOLIS_NETWORK_MAP_SCHEMA)

    @mock.patch.object(schemas, 'validate_value')
    def test_validate_storage_mappings(
        self,
        mock_validate_value
    ):
        storage_mappings = "mock_storage_mappings"

        utils.validate_storage_mappings(storage_mappings)

        mock_validate_value.assert_called_once_with(
            storage_mappings, schemas.CORIOLIS_STORAGE_MAPPINGS_SCHEMA)

    @mock.patch.object(schemas, 'validate_value')
    def test_validate_storage_mappings_schema_validation_exception(
        self,
        mock_validate_value
    ):
        storage_mappings = "mock_storage_mappings"
        mock_validate_value.side_effect = exception.SchemaValidationException()

        self.assertRaises(
            exc.HTTPBadRequest,
            utils.validate_storage_mappings,
            storage_mappings
        )

        mock_validate_value.assert_called_once_with(
            storage_mappings, schemas.CORIOLIS_STORAGE_MAPPINGS_SCHEMA)

    @mock.patch.object(utils, '_build_keyerror_message')
    def test_format_keyerror_message(self, mock_build_keyerror_message):
        @utils.format_keyerror_message(resource='endpoint', method='create')
        def mock_fun(body):
            return body['endpoint']

        result = mock_fun({'endpoint': "expected_result"})

        self.assertEqual(
            "expected_result",
            result
        )

        with self.assertLogs('coriolis.api.v1.utils', level='ERROR'):
            self.assertRaises(
                exception.InvalidInput,
                mock_fun,
                ''
            )
        mock_build_keyerror_message.assert_not_called()

        with self.assertLogs('coriolis.api.v1.utils', level='ERROR'):
            self.assertRaises(
                exception.InvalidInput,
                mock_fun,
                {}
            )
            mock_build_keyerror_message.assert_called_once_with(
                'endpoint', 'create', 'endpoint'
            )

    @mock.patch.object(utils, '_build_keyerror_message')
    def test_format_keyerror_message_empty_keyerror(
            self, mock_build_keyerror_message):

        @utils.format_keyerror_message(resource='endpoint', method='create')
        def mock_fun(body):
            raise KeyError()

        with self.assertLogs('coriolis.api.v1.utils', level='ERROR'):
            self.assertRaises(exception.InvalidInput, mock_fun, {})
        mock_build_keyerror_message.assert_not_called()

    def test_build_keyerror_message_resource_equals_key(
        self,
    ):
        resource = "mock_resource"
        method = "create"
        key = "mock_resource"

        result = utils._build_keyerror_message(resource, method, key)

        self.assertEqual(
            'The mock_resource creation body needs to be encased inside the'
            ' "mock_resource" key',
            result
        )

    def test_build_keyerror_message_resource_not_equals_key(
        self,
    ):
        resource = "mock_resource"
        method = "update"
        key = "mock_key"

        result = utils._build_keyerror_message(resource, method, key)

        self.assertEqual(
            'The mock_resource update body lacks a required attribute: '
            '"mock_key"',
            result
        )

    def test_validate_user_scripts(
        self,
    ):
        user_scripts = {
            'global': {
                'linux': 'mock_scripts_linux',
                'windows': 'mock_scripts_windows'
            },
            'instances': {}
        }

        result = utils.validate_user_scripts(user_scripts)

        self.assertEqual(
            user_scripts,
            result
        )

    def test_validate_user_scripts_none(
        self,
    ):
        user_scripts = None

        result = utils.validate_user_scripts(user_scripts)

        self.assertEqual(
            {},
            result
        )

    @ddt.file_data('data/utils_validate_user_scripts_raises.yml')
    def test_validate_user_scripts_invalid_input(
        self,
        user_scripts
    ):
        self.assertRaises(
            exception.InvalidInput,
            utils.validate_user_scripts,
            user_scripts
        )

    @ddt.file_data('data/utils_validate_instances_list_for_transfer.yml')
    def test_validate_instances_list_for_transfer(
        self,
        instances,
        expected_result,
        exception_raised
    ):
        if exception_raised:
            self.assertRaises(
                exception.InvalidInput,
                utils.validate_instances_list_for_transfer,
                instances
            )
        else:
            result = utils.validate_instances_list_for_transfer(instances)

            self.assertEqual(
                expected_result,
                result
            )
