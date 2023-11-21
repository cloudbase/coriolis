# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import ddt
import paramiko

from coriolis import constants
from coriolis import exception
from coriolis.tasks import base
from coriolis.tests import test_base


@ddt.ddt
class BaseTasksTestCase(test_base.CoriolisBaseTestCase):

    @mock.patch.object(base.TaskRunner, '__abstractmethods__', set())
    def setUp(self):
        super(BaseTasksTestCase, self).setUp()
        self.task_runner = base.TaskRunner()

    @mock.patch.object(base.TaskRunner, 'get_required_platform')
    @mock.patch('coriolis.tasks.base.get_shared_lib_dirs_for_provider')
    @ddt.file_data('data/get_shared_libs_for_providers.yml')
    @ddt.unpack
    def test_get_shared_libs_for_providers(
            self, mock_get_shared_lib_dirs,
            mock_get_required_platform, config, expected_result):
        # shared_libs = {}
        platform = config.get('platform', '')
        mock_get_required_platform.return_value = platform
        source_lib_dirs = config.get('source_lib_dirs', [])
        dest_lib_dirs = config.get('destination_lib_dirs', [])
        return_source_dirs = config.get('return_source_dirs', False)
        return_dest_dirs = config.get('return_dest_dirs', False)
        shared_lib_dirs_side_effect = []
        expected_calls = []
        if return_source_dirs:
            shared_lib_dirs_side_effect.append(source_lib_dirs)
            expected_calls.append(
                mock.call.mock_get_shared_lib_dirs(
                    mock.sentinel.ctxt, mock.sentinel.origin,
                    mock.sentinel.event_handler))
        if return_dest_dirs:
            shared_lib_dirs_side_effect.append(dest_lib_dirs)
            expected_calls.append(
                mock.call.mock_get_shared_lib_dirs(
                    mock.sentinel.ctxt, mock.sentinel.destination,
                    mock.sentinel.event_handler))
        mock_get_shared_lib_dirs.side_effect = shared_lib_dirs_side_effect

        result = self.task_runner.get_shared_libs_for_providers(
            mock.sentinel.ctxt, mock.sentinel.origin,
            mock.sentinel.destination, mock.sentinel.event_handler)
        mock_get_required_platform.assert_called_once_with()
        self.assertEqual(result, expected_result)
        mock_get_shared_lib_dirs.assert_has_calls(expected_calls)

    @mock.patch.object(base.TaskRunner, 'get_required_task_info_properties')
    @mock.patch.object(base.TaskRunner, '_run')
    @mock.patch.object(base.TaskRunner, 'get_returned_task_info_properties')
    @mock.patch('coriolis.utils.sanitize_task_info')
    @ddt.file_data('data/run.yml')
    @ddt.unpack
    def test_run(
            self, mock_sanitize_task_info, mock_get_returned_task_info_props,
            mock_run, mock_get_required_task_info_props, config,
            exception_expected):
        required_properties = config.get('required_properties', [])
        returned_properties = config.get('returned_properties', [])
        task_info = config.get('task_info', {})
        run_result = config.get('run_result', {})
        sanitize_check = config.get('sanitize_check', False)
        mock_get_required_task_info_props.return_value = required_properties
        mock_get_returned_task_info_props.return_value = returned_properties
        mock_run.return_value = run_result

        if exception_expected:
            self.assertRaises(
                exception.CoriolisException, self.task_runner.run,
                mock.sentinel.ctxt, mock.sentinel.instance,
                mock.sentinel.origin, mock.sentinel.destination,
                task_info, mock.sentinel.event_handler)
            if sanitize_check:
                mock_sanitize_task_info.assert_called_once_with(
                    mock_run.return_value)
            return

        result = self.task_runner.run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            mock.sentinel.destination, task_info, mock.sentinel.event_handler)
        mock_get_required_task_info_props.assert_called_once_with()
        mock_get_returned_task_info_props.assert_called_with()
        mock_run.assert_called_once_with(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            mock.sentinel.destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, mock_run.return_value)

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    def test_get_shared_lib_dirs_for_provider(self, mock_get_conn_info,
                                              mock_get_provider):
        endpoint = mock.MagicMock()
        mock_get_provider.return_value = mock.MagicMock()
        mock_get_shared_lib_dirs = (
            mock_get_provider.return_value.get_shared_library_directories)

        result = base.get_shared_lib_dirs_for_provider(
            mock.sentinel.ctxt, endpoint,
            mock.sentinel.event_handler)

        mock_get_provider.assert_called_once_with(
            endpoint['type'], constants.PROVIDER_TYPE_SETUP_LIBS,
            mock.sentinel.event_handler, raise_if_not_found=False)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, endpoint)
        mock_get_shared_lib_dirs.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value)
        self.assertEqual(result, mock_get_shared_lib_dirs.return_value)

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    def test_get_shared_lib_dirs_for_providers_no_provider(
            self, mock_get_conn_info, mock_get_provider):
        endpoint = mock.MagicMock()
        mock_get_provider.return_value = None

        result = base.get_shared_lib_dirs_for_provider(
            mock.sentinel.ctxt, endpoint,
            mock.sentinel.event_handler)
        mock_get_provider.assert_called_once_with(
            endpoint['type'], constants.PROVIDER_TYPE_SETUP_LIBS,
            mock.sentinel.event_handler, raise_if_not_found=False)
        mock_get_conn_info.assert_not_called()
        self.assertEqual(result, [])

    @mock.patch('coriolis.utils.get_secret_connection_info')
    @ddt.data(
        ({"connection_info": None}, {}),
        ({"connection_info": {}}, {}),
        ({"connection_info": {"secret": "password"}}, {"secret": "password"}),
        ({"connection_info": "str_value"}, "str_value"),
        ({}, {}),
    )
    def test_get_connection_info(self, data, mock_get_secret_conn_info):
        expected_conn_info = data[1]

        result = base.get_connection_info(
            mock.sentinel.ctxt, data[0])

        mock_get_secret_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, expected_conn_info)
        self.assertEqual(result, mock_get_secret_conn_info.return_value)

    def test_marshal_migr_conn_info_none(self):
        result = base.marshal_migr_conn_info(None)
        self.assertEqual(result, None)

    def test_marshal_migr_conn_info_no_key(self):
        migr_conn_info = {"no_pkey": "value"}
        result = base.marshal_migr_conn_info(migr_conn_info)
        self.assertEqual(result, migr_conn_info)

    def test_marshal_migr_conn_info_serialized(self):
        migr_conn_info = {"pkey": "serialized_key"}

        result = base.marshal_migr_conn_info(migr_conn_info)
        self.assertEqual(result, migr_conn_info)

    @mock.patch('coriolis.utils.serialize_key')
    @mock.patch('coriolis.tasks.base.CONF')
    def test_marshal_migr_conn_info(self, mock_conf, mock_serialize_key):
        pkey = mock.MagicMock()
        custom_pkey_field_name = "custom_key"
        migr_conn_info = {custom_pkey_field_name: pkey}
        expected_conn_info = {
            custom_pkey_field_name: mock_serialize_key.return_value}

        result = base.marshal_migr_conn_info(
            migr_conn_info, private_key_field_name=custom_pkey_field_name)
        mock_serialize_key.assert_called_once_with(
            pkey, mock_conf.serialization.temp_keypair_password)
        self.assertEqual(result, expected_conn_info)

    def test_unmarshal_migr_conn_info_none(self):
        result = base.unmarshal_migr_conn_info(None)
        self.assertEqual(result, None)

    def test_unmarshal_migr_conn_info_no_key(self):
        migr_conn_info = {"no_pkey": "value"}
        result = base.unmarshal_migr_conn_info(migr_conn_info)
        self.assertEqual(result, migr_conn_info)

    def test_unmarshal_migr_conn_info_serialized(self):
        pkey = paramiko.RSAKey.generate(bits=2048)
        migr_conn_info = {"pkey": pkey}

        result = base.unmarshal_migr_conn_info(migr_conn_info)
        self.assertEqual(result, migr_conn_info)

    @mock.patch('coriolis.utils.deserialize_key')
    @mock.patch('coriolis.tasks.base.CONF')
    def test_unmarshal_migr_conn_info(self, mock_conf, mock_deserialize_key):
        custom_pkey_field_name = "custom_key"
        migr_conn_info = {custom_pkey_field_name: "pkey"}
        expected_conn_info = {
            custom_pkey_field_name: mock_deserialize_key.return_value}

        result = base.unmarshal_migr_conn_info(
            migr_conn_info, private_key_field_name=custom_pkey_field_name)
        mock_deserialize_key.assert_called_once_with(
            "pkey", mock_conf.serialization.temp_keypair_password)
        self.assertEqual(result, expected_conn_info)
