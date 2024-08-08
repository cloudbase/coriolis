# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import ddt
from webob import exc

from coriolis.api.v1 import replicas
from coriolis.api.v1 import utils as api_utils
from coriolis.api.v1.views import replica_tasks_execution_view
from coriolis.api.v1.views import replica_view
from coriolis.endpoints import api as endpoints_api
from coriolis import exception
from coriolis.replicas import api
from coriolis.tests import test_base
from coriolis.tests import testutils


@ddt.ddt
class ReplicaControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Replica Controller v1 API"""

    def setUp(self):
        super(ReplicaControllerTestCase, self).setUp()
        self.replicas = replicas.ReplicaController()

    @mock.patch('coriolis.api.v1.replicas.CONF')
    @mock.patch.object(replica_view, 'single')
    @mock.patch.object(api.API, 'get_replica')
    def test_show(
        self,
        mock_get_replica,
        mock_single,
        mock_conf
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_conf.api.include_task_info_in_replicas_api = True

        result = self.replicas.show(mock_req, id)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with("migration:replicas:show")
        mock_get_replica.assert_called_once_with(
            mock_context, id, include_task_info=True)
        mock_single.assert_called_once_with(mock_get_replica.return_value)

    @mock.patch('coriolis.api.v1.replicas.CONF')
    @mock.patch.object(replica_view, 'single')
    @mock.patch.object(api.API, 'get_replica')
    def test_show_no_replica(
        self,
        mock_get_replica,
        mock_single,
        mock_conf
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_conf.api.include_task_info_in_replicas_api = True
        mock_get_replica.return_value = None

        self.assertRaises(
            exc.HTTPNotFound,
            self.replicas.show,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with("migration:replicas:show")
        mock_get_replica.assert_called_once_with(
            mock_context, id, include_task_info=True)
        mock_single.assert_not_called()

    @mock.patch('coriolis.api.v1.replicas.CONF')
    @mock.patch.object(replica_view, 'collection')
    @mock.patch.object(api.API, 'get_replicas')
    @mock.patch.object(api_utils, '_get_show_deleted')
    def test_list(
        self,
        mock_get_show_deleted,
        mock_get_replicas,
        mock_collection,
        mock_conf
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}

        result = self.replicas._list(mock_req)

        self.assertEqual(
            mock_collection.return_value,
            result
        )

        mock_get_show_deleted.assert_called_once_with(
            mock_req.GET.get.return_value)
        mock_context.can.assert_called_once_with("migration:replicas:list")
        mock_get_replicas.assert_called_once_with(
            mock_context,
            include_tasks_executions=
            mock_conf.api.include_task_info_in_replicas_api,
            include_task_info=mock_conf.api.include_task_info_in_replicas_api
        )
        mock_collection.assert_called_once_with(mock_get_replicas.return_value)

    @mock.patch.object(api_utils, 'validate_instances_list_for_transfer')
    @mock.patch.object(endpoints_api.API, 'validate_source_environment')
    @mock.patch.object(api_utils, 'validate_network_map')
    @mock.patch.object(endpoints_api.API, 'validate_target_environment')
    @mock.patch.object(api_utils, 'validate_user_scripts')
    @mock.patch.object(api_utils, 'normalize_user_scripts')
    @mock.patch.object(api_utils, 'validate_storage_mappings')
    @ddt.file_data('data/replicas_validate_create_body.yml')
    def test_validate_create_body(
        self,
        mock_validate_storage_mappings,
        mock_normalize_user_scripts,
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
        replica = body["replica"]
        origin_endpoint_id = replica.get('origin_endpoint_id')
        source_environment = replica.get('source_environment')
        network_map = replica.get('network_map')
        destination_endpoint_id = replica.get('destination_endpoint_id')
        destination_environment = replica.get('destination_environment')
        user_scripts = replica.get('user_scripts')
        instances = replica.get('instances')
        storage_mappings = replica.get('storage_mappings')
        mock_validate_instances_list_for_transfer.return_value = instances
        mock_normalize_user_scripts.return_value = user_scripts

        if exception_raised:
            self.assertRaisesRegex(
                Exception,
                exception_raised,
                testutils.get_wrapped_function(
                    self.replicas._validate_create_body),
                self.replicas,
                ctxt,
                body
            )

            mock_validate_network_map.assert_not_called()
        else:
            result = testutils.get_wrapped_function(
                self.replicas._validate_create_body)(
                    self.replicas,
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
            mock_normalize_user_scripts.assert_called_once_with(
                user_scripts, instances)
            mock_validate_storage_mappings.assert_called_once_with(
                storage_mappings)

        mock_validate_source_environment.assert_called_once_with(
            ctxt, origin_endpoint_id, source_environment)
        mock_validate_instances_list_for_transfer.assert_called_once_with(
            instances)

    @mock.patch.object(replica_view, 'single')
    @mock.patch.object(api.API, 'create')
    @mock.patch.object(replicas.ReplicaController, '_validate_create_body')
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
        mock_validate_create_body.return_value = (mock.sentinel.value,) * 13

        result = self.replicas.create(mock_req, mock_body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:replicas:create")
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
            self.replicas.delete,
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
            self.replicas.delete,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with("migration:replicas:delete")
        mock_delete.assert_called_once_with(mock_context, id)

    @ddt.file_data('data/replicas_update_storage_mappings.yml')
    def test_update_storage_mappings(
        self,
        config,
        expected_result,
        logs_expected
    ):
        original_storage_mappings = config['original_storage_mappings']
        new_storage_mappings = config['new_storage_mappings']

        if logs_expected:
            with self.assertLogs('coriolis.api.v1.replicas', level='INFO'):
                result = self.replicas._update_storage_mappings(
                    original_storage_mappings, new_storage_mappings)
        else:
            result = self.replicas._update_storage_mappings(
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
        result = self.replicas._get_updated_user_scripts(
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

        result = self.replicas._get_updated_user_scripts(
            original_user_scripts, new_user_scripts)

        self.assertEqual(
            original_user_scripts,
            result
        )

    @mock.patch.object(replicas.ReplicaController, '_get_updated_user_scripts')
    @mock.patch.object(api_utils, 'validate_user_scripts')
    @mock.patch.object(replicas.ReplicaController, '_update_storage_mappings')
    @ddt.file_data('data/replicas_get_merged_replica_values.yml')
    def test_get_merged_replica_values(
        self,
        mock_update_storage_mappings,
        mock_validate_user_scripts,
        mock_get_updated_user_scripts,
        config,
        expected_result
    ):
        replica = config['replica']
        updated_values = config['updated_values']
        original_storage_mapping = replica.get('storage_mappings', {})
        replica_user_scripts = replica.get('user_scripts', {})
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

        result = self.replicas._get_merged_replica_values(
            replica, updated_values)

        self.assertEqual(
            expected_result,
            result
        )

        mock_update_storage_mappings.assert_called_once_with(
            original_storage_mapping, new_storage_mappings)
        mock_validate_user_scripts.assert_has_calls(
            [mock.call(replica_user_scripts), mock.call(updated_user_scripts)])
        mock_get_updated_user_scripts.assert_called_once_with(
            "mock_scripts", "mock_new_scripts")

    @mock.patch.object(api_utils, 'normalize_user_scripts')
    @mock.patch.object(api_utils, 'validate_user_scripts')
    @mock.patch.object(api_utils, 'validate_storage_mappings')
    @mock.patch.object(api_utils, 'validate_network_map')
    @mock.patch.object(endpoints_api.API, 'validate_target_environment')
    @mock.patch.object(endpoints_api.API, 'validate_source_environment')
    @mock.patch.object(replicas.ReplicaController,
                       '_get_merged_replica_values')
    @mock.patch.object(api.API, 'get_replica')
    @ddt.file_data('data/replicas_validate_update_body.yml')
    def test_validate_update_body(
        self,
        mock_get_replica,
        mock_get_merged_replica_values,
        mock_validate_source_environment,
        mock_validate_target_environment,
        mock_validate_network_map,
        mock_validate_storage_mappings,
        mock_validate_user_scripts,
        mock_normalize_user_scripts,
        config,
        expected_result
    ):
        body = config['body']
        replica = config['replica']
        replica_body = body['replica']
        context = mock.sentinel.context
        id = mock.sentinel.id
        mock_get_replica.return_value = replica
        mock_get_merged_replica_values.return_value = replica_body
        mock_normalize_user_scripts.return_value = replica_body['user_scripts']

        result = testutils.get_wrapped_function(
            self.replicas._validate_update_body)(
            self.replicas,
            id,
            context,
            body
        )

        self.assertEqual(
            expected_result,
            result
        )

        mock_get_replica.assert_called_once_with(context, id)
        mock_get_merged_replica_values.assert_called_once_with(
            replica, replica_body)
        mock_validate_source_environment.assert_called_once_with(
            context, replica['origin_endpoint_id'],
            replica_body['source_environment'])
        mock_validate_target_environment.assert_called_once_with(
            context, replica['destination_endpoint_id'],
            replica_body['destination_environment'])
        mock_validate_network_map.assert_called_once_with(
            replica_body['network_map'])
        mock_validate_storage_mappings.assert_called_once_with(
            replica_body['storage_mappings'])
        mock_validate_user_scripts.assert_called_once_with(
            replica_body['user_scripts'])
        mock_normalize_user_scripts.assert_called_once_with(
            replica_body['user_scripts'], replica['instances'])

    @mock.patch.object(api.API, 'get_replica')
    @ddt.file_data('data/replicas_validate_update_body_raises.yml')
    def test_validate_update_body_raises(
        self,
        mock_get_replica,
        body,
    ):
        context = mock.sentinel.context
        id = mock.sentinel.id

        self.assertRaises(
            exc.HTTPBadRequest,
            testutils.get_wrapped_function(
                self.replicas._validate_update_body),
            self.replicas,
            id,
            context,
            body
        )

        mock_get_replica.assert_called_once_with(context, id)

    @mock.patch.object(replica_tasks_execution_view, 'single')
    @mock.patch.object(api.API, 'update')
    @mock.patch.object(replicas.ReplicaController, '_validate_update_body')
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

        result = self.replicas.update(mock_req, id, body)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with(
            "migration:replicas:update")
        mock_validate_update_body.assert_called_once_with(
            id, mock_context, body)
        mock_update.assert_called_once_with(
            mock_context, id,
            mock_validate_update_body.return_value)
        mock_single.assert_called_once_with(mock_update.return_value)

    @mock.patch.object(api.API, 'update')
    @mock.patch.object(replicas.ReplicaController, '_validate_update_body')
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
            self.replicas.update,
            mock_req,
            id,
            body
        )

        mock_context.can.assert_called_once_with(
            "migration:replicas:update")
        mock_validate_update_body.assert_called_once_with(
            id, mock_context, body)
        mock_update.assert_called_once_with(
            mock_context, id,
            mock_validate_update_body.return_value)

    @mock.patch.object(api.API, 'update')
    @mock.patch.object(replicas.ReplicaController, '_validate_update_body')
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
            self.replicas.update,
            mock_req,
            id,
            body
        )

        mock_context.can.assert_called_once_with(
            "migration:replicas:update")
        mock_validate_update_body.assert_called_once_with(
            id, mock_context, body)
        mock_update.assert_called_once_with(
            mock_context, id,
            mock_validate_update_body.return_value)
