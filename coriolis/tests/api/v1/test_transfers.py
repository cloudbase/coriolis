# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import ddt
from webob import exc

from coriolis.api.v1 import transfers
from coriolis.api.v1 import utils as api_utils
from coriolis.api.v1.views import transfer_tasks_execution_view
from coriolis.api.v1.views import transfer_view
from coriolis.endpoints import api as endpoints_api
from coriolis import exception
from coriolis.tests import test_base
from coriolis.tests import testutils
from coriolis.transfers import api


@ddt.ddt
class TransferControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Transfer Controller v1 API"""

    def setUp(self):
        super(TransferControllerTestCase, self).setUp()
        self.transfers = transfers.TransferController()

    @mock.patch('coriolis.api.v1.transfers.CONF')
    @mock.patch.object(transfer_view, 'single')
    @mock.patch.object(api.API, 'get_transfer')
    def test_show(
        self,
        mock_get_transfer,
        mock_single,
        mock_conf
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_conf.api.include_task_info_in_transfers_api = True

        result = self.transfers.show(mock_req, id)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with("migration:transfers:show")
        mock_get_transfer.assert_called_once_with(
            mock_context, id, include_task_info=True)
        mock_single.assert_called_once_with(mock_get_transfer.return_value)

    @mock.patch('coriolis.api.v1.transfers.CONF')
    @mock.patch.object(transfer_view, 'single')
    @mock.patch.object(api.API, 'get_transfer')
    def test_show_no_transfer(
        self,
        mock_get_transfer,
        mock_single,
        mock_conf
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_conf.api.include_task_info_in_transfers_api = True
        mock_get_transfer.return_value = None

        self.assertRaises(
            exc.HTTPNotFound,
            self.transfers.show,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with("migration:transfers:show")
        mock_get_transfer.assert_called_once_with(
            mock_context, id, include_task_info=True)
        mock_single.assert_not_called()

    @mock.patch('coriolis.api.v1.transfers.CONF')
    @mock.patch.object(transfer_view, 'collection')
    @mock.patch.object(api.API, 'get_transfers')
    @mock.patch.object(api_utils, '_get_show_deleted')
    def test_list(
        self,
        mock_get_show_deleted,
        mock_get_transfers,
        mock_collection,
        mock_conf
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}

        result = self.transfers._list(mock_req)

        self.assertEqual(
            mock_collection.return_value,
            result
        )

        mock_get_show_deleted.assert_called_once_with(
            mock_req.GET.get.return_value)
        mock_context.can.assert_called_once_with("migration:transfers:list")
        mock_get_transfers.assert_called_once_with(
            mock_context,
            include_tasks_executions=
            mock_conf.api.include_task_info_in_transfers_api,
            include_task_info=mock_conf.api.include_task_info_in_transfers_api
        )
        mock_collection.assert_called_once_with(
            mock_get_transfers.return_value)

    @mock.patch.object(api_utils, 'validate_instances_list_for_transfer')
    @mock.patch.object(endpoints_api.API, 'validate_source_environment')
    @mock.patch.object(api_utils, 'validate_network_map')
    @mock.patch.object(endpoints_api.API, 'validate_target_environment')
    @mock.patch.object(api_utils, 'validate_user_scripts')
    @mock.patch.object(api_utils, 'validate_storage_mappings')
    @ddt.file_data('data/transfers_validate_create_body.yml')
    def test_validate_create_body(
        self,
        mock_validate_storage_mappings,
        mock_validate_user_scripts,
        mock_validate_target_environment,
        mock_validate_network_map,
        mock_validate_source_environment,
        mock_validate_instances_list_for_transfer,
        config,
        exception_raised,
        expected_result
    ):
        ctxt = {}
        body = config["body"]
        transfer = body["transfer"]
        origin_endpoint_id = transfer.get('origin_endpoint_id')
        source_environment = transfer.get('source_environment')
        network_map = transfer.get('network_map')
        destination_endpoint_id = transfer.get('destination_endpoint_id')
        destination_environment = transfer.get('destination_environment')
        user_scripts = transfer.get('user_scripts')
        instances = transfer.get('instances')
        storage_mappings = transfer.get('storage_mappings')
        mock_validate_instances_list_for_transfer.return_value = instances

        if exception_raised:
            self.assertRaisesRegex(
                Exception,
                exception_raised,
                testutils.get_wrapped_function(
                    self.transfers._validate_create_body),
                self.transfers,
                ctxt,
                body
            )

            mock_validate_network_map.assert_not_called()
        else:
            result = testutils.get_wrapped_function(
                self.transfers._validate_create_body)(
                    self.transfers,
                    ctxt,
                    body,
            )

            self.assertEqual(
                tuple(expected_result),
                result
            )

            mock_validate_network_map.assert_called_once_with(network_map)
            mock_validate_target_environment.assert_called_once_with(
                ctxt, destination_endpoint_id, destination_environment)
            mock_validate_user_scripts.assert_called_once_with(user_scripts)
            mock_validate_storage_mappings.assert_called_once_with(
                storage_mappings)

        mock_validate_source_environment.assert_called_once_with(
            ctxt, origin_endpoint_id, source_environment)
        mock_validate_instances_list_for_transfer.assert_called_once_with(
            instances)

    @mock.patch.object(transfer_view, 'single')
    @mock.patch.object(api.API, 'create')
    @mock.patch.object(transfers.TransferController, '_validate_create_body')
    def test_create(
        self,
        mock_validate_create_body,
        mock_create,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        mock_body = {}
        mock_validate_create_body.return_value = (mock.sentinel.value,) * 15

        result = self.transfers.create(mock_req, mock_body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:transfers:create")
        mock_validate_create_body.assert_called_once_with(
            mock_context, mock_body)
        mock_create.assert_called_once()
        mock_single.assert_called_once_with(mock_create.return_value)

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
            self.transfers.delete,
            mock_req,
            id
        )

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
            self.transfers.delete,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with("migration:transfers:delete")
        mock_delete.assert_called_once_with(mock_context, id)

    @ddt.file_data('data/transfers_update_storage_mappings.yml')
    def test_update_storage_mappings(
        self,
        config,
        expected_result,
        logs_expected
    ):
        original_storage_mappings = config['original_storage_mappings']
        new_storage_mappings = config['new_storage_mappings']

        if logs_expected:
            with self.assertLogs('coriolis.api.v1.transfers', level='INFO'):
                result = self.transfers._update_storage_mappings(
                    original_storage_mappings, new_storage_mappings)
        else:
            result = self.transfers._update_storage_mappings(
                original_storage_mappings, new_storage_mappings)

        self.assertEqual(
            expected_result,
            result
        )

    def test_get_updated_user_scripts(
        self,
    ):
        original_user_scripts = {
            'global': {"mock_global_scripts_1": "mock_value",
                       "mock_global_scripts_2": "mock_value"},
            'instances': {"mock_instance_scripts": "mock_value"}
        }
        new_user_scripts = {
            'global': {"mock_global_scripts_1": "mock_new_value"},
            'instances': {"mock_instance_scripts": "mock_new_value"}
        }
        expected_result = {
            'global': {"mock_global_scripts_1": "mock_new_value",
                       "mock_global_scripts_2": "mock_value"},
            'instances': {"mock_instance_scripts": "mock_new_value"}
        }
        result = self.transfers._get_updated_user_scripts(
            original_user_scripts, new_user_scripts)

        self.assertEqual(
            expected_result,
            result
        )

    def test_get_updated_user_scripts_new_user_scripts_empty(
        self,
    ):
        original_user_scripts = {
            'global': {"mock_global_scripts_1": "mock_value",
                       "mock_global_scripts_2": "mock_value"},
            'instances': {"mock_instance_scripts": "mock_value"}
        }
        new_user_scripts = {}

        result = self.transfers._get_updated_user_scripts(
            original_user_scripts, new_user_scripts)

        self.assertEqual(
            original_user_scripts,
            result
        )

    @mock.patch.object(transfers.TransferController,
                       '_get_updated_user_scripts')
    @mock.patch.object(api_utils, 'validate_user_scripts')
    @mock.patch.object(transfers.TransferController,
                       '_update_storage_mappings')
    @ddt.file_data('data/transfers_get_merged_transfer_values.yml')
    def test_get_merged_transfer_values(
        self,
        mock_update_storage_mappings,
        mock_validate_user_scripts,
        mock_get_updated_user_scripts,
        config,
        expected_result
    ):
        transfer = config['transfer']
        updated_values = config['updated_values']
        original_storage_mapping = transfer.get('storage_mappings', {})
        transfer_user_scripts = transfer.get('user_scripts', {})
        updated_user_scripts = updated_values.get('user_scripts', {})
        new_storage_mappings = updated_values.get('storage_mappings', {})
        expected_result['storage_mappings'] = \
            mock_update_storage_mappings.return_value
        expected_result['destination_environment'][
            'storage_mappings'] = mock_update_storage_mappings.return_value
        expected_result['user_scripts'] = \
            mock_get_updated_user_scripts.return_value
        mock_validate_user_scripts.side_effect = ["mock_scripts",
                                                  "mock_new_scripts"]

        result = self.transfers._get_merged_transfer_values(
            transfer, updated_values)

        self.assertEqual(
            expected_result,
            result
        )

        mock_update_storage_mappings.assert_called_once_with(
            original_storage_mapping, new_storage_mappings)
        mock_validate_user_scripts.assert_has_calls(
            [mock.call(transfer_user_scripts),
             mock.call(updated_user_scripts)])
        mock_get_updated_user_scripts.assert_called_once_with(
            "mock_scripts", "mock_new_scripts")

    @mock.patch.object(api_utils, 'validate_user_scripts')
    @mock.patch.object(api_utils, 'validate_storage_mappings')
    @mock.patch.object(api_utils, 'validate_network_map')
    @mock.patch.object(endpoints_api.API, 'validate_target_environment')
    @mock.patch.object(endpoints_api.API, 'validate_source_environment')
    @mock.patch.object(transfers.TransferController,
                       '_get_merged_transfer_values')
    @mock.patch.object(api.API, 'get_transfer')
    @ddt.file_data('data/transfers_validate_update_body.yml')
    def test_validate_update_body(
        self,
        mock_get_transfer,
        mock_get_merged_transfer_values,
        mock_validate_source_environment,
        mock_validate_target_environment,
        mock_validate_network_map,
        mock_validate_storage_mappings,
        mock_validate_user_scripts,
        config,
        expected_result
    ):
        body = config['body']
        transfer = config['transfer']
        transfer_body = body['transfer']
        context = mock.sentinel.context
        id = mock.sentinel.id
        mock_get_transfer.return_value = transfer
        mock_get_merged_transfer_values.return_value = transfer_body

        result = testutils.get_wrapped_function(
            self.transfers._validate_update_body)(
            self.transfers,
            id,
            context,
            body
        )

        self.assertEqual(
            expected_result,
            result
        )

        mock_get_transfer.assert_called_once_with(context, id)
        mock_get_merged_transfer_values.assert_called_once_with(
            transfer, transfer_body)
        mock_validate_source_environment.assert_called_once_with(
            context, transfer['origin_endpoint_id'],
            transfer_body['source_environment'])
        mock_validate_target_environment.assert_called_once_with(
            context, transfer['destination_endpoint_id'],
            transfer_body['destination_environment'])
        mock_validate_network_map.assert_called_once_with(
            transfer_body['network_map'])
        mock_validate_storage_mappings.assert_called_once_with(
            transfer_body['storage_mappings'])
        mock_validate_user_scripts.assert_called_once_with(
            transfer_body['user_scripts'])

    @mock.patch.object(api.API, 'get_transfer')
    @ddt.file_data('data/transfers_validate_update_body_raises.yml')
    def test_validate_update_body_raises(
        self,
        mock_get_transfer,
        body,
    ):
        context = mock.sentinel.context
        id = mock.sentinel.id

        self.assertRaises(
            exc.HTTPBadRequest,
            testutils.get_wrapped_function(
                self.transfers._validate_update_body),
            self.transfers,
            id,
            context,
            body
        )

        mock_get_transfer.assert_called_once_with(context, id)

    @mock.patch.object(transfer_tasks_execution_view, 'single')
    @mock.patch.object(api.API, 'update')
    @mock.patch.object(transfers.TransferController, '_validate_update_body')
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

        result = self.transfers.update(mock_req, id, body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:transfers:update")
        mock_validate_update_body.assert_called_once_with(
            id, mock_context, body)
        mock_update.assert_called_once_with(
            mock_context, id,
            mock_validate_update_body.return_value)
        mock_single.assert_called_once_with(mock_update.return_value)

    @mock.patch.object(api.API, 'update')
    @mock.patch.object(transfers.TransferController, '_validate_update_body')
    def test_update_not_found(
        self,
        mock_validate_update_body,
        mock_update
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        body = mock.sentinel.body
        mock_update.side_effect = exception.NotFound()

        self.assertRaises(
            exc.HTTPNotFound,
            self.transfers.update,
            mock_req,
            id,
            body
        )

        mock_context.can.assert_called_once_with(
            "migration:transfers:update")
        mock_validate_update_body.assert_called_once_with(
            id, mock_context, body)
        mock_update.assert_called_once_with(
            mock_context, id,
            mock_validate_update_body.return_value)

    @mock.patch.object(api.API, 'update')
    @mock.patch.object(transfers.TransferController, '_validate_update_body')
    def test_update_not_invalid_parameter_value(
        self,
        mock_validate_update_body,
        mock_update
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        body = mock.sentinel.body
        mock_update.side_effect = exception.InvalidParameterValue('err')

        self.assertRaises(
            exc.HTTPNotFound,
            self.transfers.update,
            mock_req,
            id,
            body
        )

        mock_context.can.assert_called_once_with(
            "migration:transfers:update")
        mock_validate_update_body.assert_called_once_with(
            id, mock_context, body)
        mock_update.assert_called_once_with(
            mock_context, id,
            mock_validate_update_body.return_value)
