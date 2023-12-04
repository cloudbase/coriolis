# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import ddt

from coriolis import constants
from coriolis import exception
from coriolis import schemas
from coriolis.tasks import osmorphing_tasks
from coriolis.tests import test_base


@ddt.ddt
class OSMorphingTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(OSMorphingTaskTestCase, self).setUp()
        self.task_runner = osmorphing_tasks.OSMorphingTask()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.unmarshal_migr_conn_info')
    @mock.patch('coriolis.osmorphing.manager.morph_image')
    @ddt.file_data('data/osmorphing_task_run.yml')
    @ddt.unpack
    def test__run(self, mock_morph_image, mock_unmarshal, mock_get_provider,
                  config, expected_instance_script):
        mock_get_provider.side_effect = [mock.sentinel.origin_provider,
                                         mock.sentinel.destination_provider]
        instance = config['instance']
        task_info = config['task_info']
        osmorphing_info = task_info.get('osmorphing_info', {})
        origin = mock.MagicMock()
        destination = mock.MagicMock()
        expected_calls = [
            mock.call.mock_get_provider(
                origin['type'], constants.PROVIDER_TYPE_REPLICA_EXPORT,
                mock.sentinel.event_handler),
            mock.call.mock_get_provider(
                destination['type'], constants.PROVIDER_TYPE_REPLICA_IMPORT,
                mock.sentinel.event_handler),
        ]

        result = self.task_runner._run(
            mock.sentinel.ctxt, instance, origin, destination, task_info,
            mock.sentinel.event_handler)
        mock_get_provider.assert_has_calls(expected_calls)
        mock_unmarshal.assert_called_once_with(
            task_info['osmorphing_connection_info'])
        mock_morph_image.assert_called_once_with(
            mock.sentinel.origin_provider, mock.sentinel.destination_provider,
            mock_unmarshal.return_value, osmorphing_info,
            expected_instance_script, mock.sentinel.event_handler)
        self.assertEqual(result, {})


@ddt.ddt
class DeployOSMorphingResourcesTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(DeployOSMorphingResourcesTaskTestCase, self).setUp()
        self.task_runner = osmorphing_tasks.DeployOSMorphingResourcesTask()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.schemas.validate_value')
    @mock.patch('coriolis.tasks.base.marshal_migr_conn_info')
    @ddt.file_data('data/deploy_osmorphing_task_run.yml')
    @ddt.unpack
    def test__run(self, mock_marshal_conn_info, mock_validate_value,
                  mock_get_conn_info, mock_get_provider, import_info,
                  raise_expected, log_expected):
        prov_fun = mock_get_provider.return_value.deploy_os_morphing_resources
        prov_fun.return_value = import_info
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        method_args = [
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler]

        def _get_result(*args):
            if log_expected:
                with self.assertLogs('coriolis.tasks.osmorphing_tasks',
                                     level=osmorphing_tasks.logging.WARN):
                    return self.task_runner._run(*args)
            return self.task_runner._run(*args)

        if raise_expected:
            self.assertRaises(
                exception.InvalidTaskResult,
                self.task_runner._run, *method_args)
            mock_marshal_conn_info.assert_not_called()
        else:
            expected_result = {
                "os_morphing_resources": import_info.get(
                    'os_morphing_resources'),
                "osmorphing_connection_info": (
                    mock_marshal_conn_info.return_value),
                "osmorphing_info": import_info.get('osmorphing_info', {}),
            }
            self.assertEqual(expected_result, _get_result(*method_args))
            mock_get_provider.assert_called_once_with(
                destination['type'], constants.PROVIDER_TYPE_OS_MORPHING,
                mock.sentinel.event_handler)
            mock_get_conn_info.assert_called_once_with(
                mock.sentinel.ctxt, destination)
            prov_fun.assert_called_once_with(
                mock.sentinel.ctxt, mock_get_conn_info.return_value,
                task_info['target_environment'],
                task_info['instance_deployment_info'])
            mock_validate_value.assert_called_once_with(
                import_info, schemas.CORIOLIS_OS_MORPHING_RESOURCES_SCHEMA,
                raise_on_error=False)
            mock_marshal_conn_info.assert_called_once_with(
                import_info.get('osmorphing_connection_info'))


class DeleteOSMorphingResourcesTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(DeleteOSMorphingResourcesTaskTestCase, self).setUp()
        self.task_runner = osmorphing_tasks.DeleteOSMorphingResourcesTask()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    def test__run(self, mock_get_conn_info, mock_get_provider):
        prov_fun = mock_get_provider.return_value.delete_os_morphing_resources
        destination = mock.MagicMock()
        task_info = {
            "os_morphing_resources": {"res1": "id1"},
            "target_environment": {"opt1": "env1"},
        }

        expected_result = {
            "os_morphing_resources": None,
            "osmorphing_connection_info": None,
        }
        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            destination['type'], constants.PROVIDER_TYPE_OS_MORPHING,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['target_environment'],
            task_info['os_morphing_resources'])
