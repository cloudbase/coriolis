# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import ddt

from coriolis import constants
from coriolis.tasks import migration_tasks
from coriolis.tests import test_base


@ddt.ddt
class GetOptimalFlavorTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(GetOptimalFlavorTaskTestCase, self).setUp()
        self.task_runner = migration_tasks.GetOptimalFlavorTask()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.events.EventManager')
    @ddt.data(
        None,
        {"selected_flavor": "flavor1"}
    )
    def test__run(self, instance_deployment_info, mock_event_manager,
                  mock_get_conn_info, mock_get_provider):
        destination = mock.MagicMock()
        provider = mock_get_provider.return_value
        task_info = {
            "target_environment": mock.sentinel.target_environment,
            "export_info": mock.sentinel.export_info,
            "instance_deployment_info": instance_deployment_info,
        }
        expected_result = {
            "instance_deployment_info": {
                "selected_flavor": provider.get_optimal_flavor.return_value}}

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        mock_get_provider.assert_called_once_with(
            destination['type'], constants.PROVIDER_TYPE_INSTANCE_FLAVOR,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        provider.get_optimal_flavor.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            mock.sentinel.target_environment, mock.sentinel.export_info)
        mock_event_manager.assert_called_once_with(mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)


class ValidateMigrationDestinationInputsTaskTestCase(
        test_base.CoriolisBaseTestCase):
    def setUp(self):
        super(ValidateMigrationDestinationInputsTaskTestCase, self).setUp()
        self.task_runner = (
            migration_tasks.ValidateMigrationDestinationInputsTask())

    def test__validate_provider_replica_import_input(self):
        provider = mock.MagicMock()
        args = [
            mock.sentinel.ctxt, mock.sentinel.conn_info,
            mock.sentinel.target_environment, mock.sentinel.export_info]
        self.task_runner._validate_provider_replica_import_input(
            provider, *args)
        provider.validate_replica_import_input.assert_called_once_with(
            *args,
            check_os_morphing_resources=True,
            check_final_vm_params=True)
