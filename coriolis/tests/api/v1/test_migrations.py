# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock
from webob import exc

import ddt

from coriolis.api.v1 import migrations
from coriolis.api.v1 import utils as api_utils
from coriolis.api.v1.views import migration_view
from coriolis.endpoints import api as endpoints_api
from coriolis import exception
from coriolis.migrations import api
from coriolis.tests import test_base
from coriolis.tests import testutils


@ddt.ddt
class MigrationControllerTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Migrations v1 API"""

    def setUp(self):
        super(MigrationControllerTestCase, self).setUp()
        self.migrations = migrations.MigrationController()

    @mock.patch.object(migration_view, 'single')
    @mock.patch.object(api.API, 'get_migration')
    @mock.patch('coriolis.api.v1.migrations.CONF')
    def test_show(
        self,
        mock_conf,
        mock_get_migration,
        mock_single
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_conf.api.include_task_info_in_migrations_api = False

        result = self.migrations.show(mock_req, id)

        self.assertEqual(
            mock_single.return_value,
            result
        )

        mock_context.can.assert_called_once_with("migration:migrations:show")
        mock_get_migration.assert_called_once_with(
            mock_context, id, include_task_info=False
        )
        mock_single.assert_called_once_with(mock_get_migration.return_value)

    @mock.patch.object(api.API, 'get_migration')
    @mock.patch('coriolis.api.v1.migrations.CONF')
    def test_show_no_migration(
        self,
        mock_conf,
        mock_get_migration
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        id = mock.sentinel.id
        mock_conf.api.include_task_info_in_migrations_api = False
        mock_get_migration.return_value = None

        self.assertRaises(
            exc.HTTPNotFound,
            self.migrations.show,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with("migration:migrations:show")
        mock_get_migration.assert_called_once_with(
            mock_context, id, include_task_info=False
        )

    @mock.patch.object(migration_view, 'collection')
    @mock.patch.object(api.API, 'get_migrations')
    @mock.patch.object(api_utils, '_get_show_deleted')
    @mock.patch('coriolis.api.v1.migrations.CONF')
    def test__list(
        self,
        mock_conf,
        mock__get_show_deleted,
        mock_get_migrations,
        mock_collection
    ):
        mock_req = mock.Mock()
        mock_context = mock.Mock()
        mock_req.environ = {'coriolis.context': mock_context}
        mock_conf.api.include_task_info_in_migrations_api = False

        result = self.migrations._list(mock_req)

        self.assertEqual(
            mock_collection.return_value,
            result
        )
        self.assertEqual(
            mock_context.show_deleted,
            mock__get_show_deleted.return_value
        )

        mock__get_show_deleted.assert_called_once_with(
            mock_req.GET.get.return_value)
        mock_context.can.assert_called_once_with("migration:migrations:list")
        mock_get_migrations.assert_called_once_with(
            mock_context,
            include_tasks=False,
            include_task_info=False
        )

    @mock.patch.object(api_utils, 'validate_storage_mappings')
    @mock.patch.object(endpoints_api.API, 'validate_target_environment')
    @mock.patch.object(api_utils, 'validate_network_map')
    @mock.patch.object(endpoints_api.API, 'validate_source_environment')
    @mock.patch.object(api_utils, 'validate_instances_list_for_transfer')
    @ddt.file_data('data/migration_validate_input.yml')
    @ddt.unpack
    def test__validate_migration_input(
            self,
            mock_validate_instances_list_for_transfer,
            mock_validate_source_environment,
            mock_validate_network_map,
            mock_validate_target_environment,
            mock_validate_storage_mappings,
            config,
            raises_value_error,
    ):
        mock_context = mock.Mock()
        mock_validate_instances_list_for_transfer.return_value = \
            config['migration']['instances']

        if raises_value_error:
            self.assertRaises(
                ValueError,
                testutils.get_wrapped_function(
                    self.migrations._validate_migration_input),
                self.migrations,
                context=mock_context,
                body=config
            )
            mock_validate_instances_list_for_transfer.assert_called_once()
        else:
            testutils.get_wrapped_function(
                self.migrations._validate_migration_input)(
                    self.migrations,
                    context=mock_context,  # type: ignore
                    body=config,  # type: ignore
            )
            mock_validate_source_environment.assert_called_once_with(
                mock_context,
                config['migration']['origin_endpoint_id'],
                config['migration']['source_environment']
            )
            mock_validate_network_map.assert_called_once_with(
                config['migration']['network_map']
            )
            mock_validate_target_environment.assert_called_once_with(
                mock_context,
                config['migration']['destination_endpoint_id'],
                config['migration']['destination_environment']
            )
            mock_validate_storage_mappings.assert_called_once_with(
                config['migration']['storage_mappings']
            )
            mock_validate_instances_list_for_transfer.assert_called_once_with(
                config['migration']['instances'],
            )

    @mock.patch.object(migration_view, 'single')
    @mock.patch.object(migrations.MigrationController,
                       '_validate_migration_input')
    @mock.patch.object(api_utils, 'normalize_user_scripts')
    @mock.patch.object(api_utils, 'validate_user_scripts')
    @ddt.file_data('data/migration_create.yml')
    @ddt.unpack
    def test_create(
        self,
        mock_validate_user_scripts,
        mock_normalize_user_scripts,
        mock__validate_migration_input,
        mock_single,
        config,
        expected_api_method,
        validation_expected,
    ):
        with mock.patch.object(api.API,
                               expected_api_method) as mock_api_method:
            mock_req = mock.Mock()
            mock_context = mock.Mock()
            mock_req.environ = {'coriolis.context': mock_context}
            mock__validate_migration_input.return_value = \
                (mock.sentinel.value,) * 14

            result = self.migrations.create(mock_req, config)

            self.assertEqual(
                mock_single.return_value,
                result
            )

            mock_context.can.assert_called_once_with(
                "migration:migrations:create")
            mock_validate_user_scripts.assert_called_once_with(
                config['migration']['user_scripts'])
            mock_normalize_user_scripts.assert_called_once_with(
                config['migration']['user_scripts'],
                config['migration']['instances']
            )
            if validation_expected:
                mock__validate_migration_input.assert_called_once_with(
                    mock_context, config)
            mock_api_method.assert_called_once()
            mock_single.assert_called_once_with(mock_api_method.return_value)

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
            self.migrations.delete,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with("migration:migrations:delete")
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
            self.migrations.delete,
            mock_req,
            id
        )

        mock_context.can.assert_called_once_with("migration:migrations:delete")
        mock_delete.assert_called_once_with(mock_context, id)
