# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

import copy
from unittest import mock

import ddt

from coriolis import constants
from coriolis import exception
from coriolis.providers import backup_writers
from coriolis import schemas
from coriolis.tasks import replica_tasks
from coriolis.tests import test_base


@ddt.ddt
class ReplicaTasksTestCase(test_base.CoriolisBaseTestCase):

    @ddt.data(
        ({"volumes_info": []}, True),
        ({"volumes_info": None}, True),
        ({"volumes_info": [{"vol_id": "id1"}, {"vol_id": "id2"}]}, False),
        ({}, True),
    )
    def test__get_volumes_info(self, data):
        task_info = data[0]
        if data[1]:
            self.assertRaises(exception.InvalidActionTasksExecutionState,
                              replica_tasks._get_volumes_info, task_info)
        else:
            result = replica_tasks._get_volumes_info(task_info)
            self.assertEqual(result, task_info['volumes_info'])

    @mock.patch('coriolis.utils.sanitize_task_info')
    @ddt.file_data("data/check_ensure_volumes_info_ordering.yml")
    @ddt.unpack
    def test__check_ensure_volumes_info_ordering(
            self, mock_sanitize, export_info, volumes_info,
            exception_expected, expected_result):
        expected_calls = [
            mock.call.mock_sanitize({"volumes_info": volumes_info}),
            mock.call.mock_sanitize({"volumes_info": expected_result}),
        ]
        if exception_expected:
            self.assertRaises(
                exception.InvalidActionTasksExecutionState,
                replica_tasks._check_ensure_volumes_info_ordering,
                export_info, volumes_info)
        else:
            result = replica_tasks._check_ensure_volumes_info_ordering(
                export_info, volumes_info)
            mock_sanitize.has_calls(expected_calls)
            self.assertEqual(result, expected_result)

    @ddt.file_data("data/test_nic_ips_update.yml")
    @ddt.unpack
    def test__preserve_old_export_info_nic_ips(
            self, old_export_info, new_export_info, expected_export_info):
        replica_tasks._preserve_old_export_info_nic_ips(
            old_export_info, new_export_info)
        self.assertEqual(new_export_info, expected_export_info)

    @ddt.file_data("data/test_hostname_update.yml")
    @ddt.unpack
    def test__preserve_hostname_info(
            self, old_export_info, new_export_info, expected_export_info):
        replica_tasks._preserve_hostname_info(old_export_info, new_export_info)
        self.assertEqual(new_export_info, expected_export_info)


class GetInstanceInfoTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(GetInstanceInfoTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.GetInstanceInfoTask()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.schemas.validate_value')
    def test__run(self, mock_validate_value, mock_get_conn_info,
                  mock_get_provider):
        origin = mock.MagicMock()
        task_info = mock.MagicMock()
        prov_fun = mock_get_provider.return_value.get_replica_instance_info
        expected_result = {"export_info": prov_fun.return_value}

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, origin,
            mock.sentinel.destiantion, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            origin['type'], constants.PROVIDER_TYPE_TRANSFER_EXPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(mock.sentinel.ctxt, origin)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['source_environment'], mock.sentinel.instance)
        mock_validate_value.assert_called_once_with(
            prov_fun.return_value, schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA)


class ShutdownInstanceTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(ShutdownInstanceTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.ShutdownInstanceTask()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    def test__run(self, mock_get_conn_info, mock_get_provider):
        origin = mock.MagicMock()
        task_info = mock.MagicMock()
        prov_fun = mock_get_provider.return_value.shutdown_instance

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, origin,
            mock.sentinel.destiantion, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, {})
        mock_get_provider.assert_called_once_with(
            origin['type'], constants.PROVIDER_TYPE_TRANSFER_EXPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(mock.sentinel.ctxt, origin)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['source_environment'], mock.sentinel.instance)


class ReplicateDisksTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(ReplicateDisksTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.ReplicateDisksTask()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.schemas.validate_value')
    @mock.patch.object(replica_tasks, '_get_volumes_info')
    @mock.patch.object(replica_tasks, '_check_ensure_volumes_info_ordering')
    @mock.patch('coriolis.tasks.base.unmarshal_migr_conn_info')
    def test__run(self, mock_unmarshal, mock_check_vol_info, mock_get_vol_info,
                  mock_validate_value, mock_get_conn_info, mock_get_provider):
        origin = mock.MagicMock()
        task_info = mock.MagicMock()
        task_info.get.side_effect = [
            task_info['incremental'], task_info['source_resources']]
        prov_fun = mock_get_provider.return_value.replicate_disks
        expected_result = {"volumes_info": mock_check_vol_info.return_value}
        expected_validation_calls = [
            mock.call.mock_validate_value(
                {"volumes_info": mock_get_vol_info.return_value},
                schemas.CORIOLIS_DISK_SYNC_RESOURCES_INFO_SCHEMA),
            mock.call.mock_validate_value(
                task_info['source_resources_connection_info'],
                schemas.CORIOLIS_REPLICATION_WORKER_CONN_INFO_SCHEMA),
            mock.call.mock_validate_value(
                task_info['target_resources_connection_info'],
                schemas.CORIOLIS_DISK_SYNC_RESOURCES_CONN_INFO_SCHEMA),
            mock.call.mock_validate_value(
                prov_fun.return_value, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA),
        ]
        expected_unmarshal_calls = [
            mock.call.mock_unmarshal(
                task_info['source_resources_connection_info']),
            mock.call.mock_unmarshal(
                task_info['target_resources_connection_info'][
                    'connection_details']),
        ]

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, origin,
            mock.sentinel.destiantion, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            origin['type'], constants.PROVIDER_TYPE_TRANSFER_EXPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(mock.sentinel.ctxt, origin)
        mock_get_vol_info.assert_called_once_with(task_info)
        mock_validate_value.assert_has_calls(expected_validation_calls)
        mock_unmarshal.assert_has_calls(expected_unmarshal_calls)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['source_environment'], mock.sentinel.instance,
            task_info['source_resources'], mock_unmarshal.return_value,
            task_info['target_resources_connection_info'],
            mock_get_vol_info.return_value, task_info['incremental'])
        mock_check_vol_info.assert_called_once_with(
            task_info['export_info'], prov_fun.return_value)


class DeployReplicaDisksTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(DeployReplicaDisksTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.DeployReplicaDisksTask()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.schemas.validate_value')
    @mock.patch.object(replica_tasks, '_check_ensure_volumes_info_ordering')
    def test__run(self, mock_check_vol_info, mock_validate_value,
                  mock_get_conn_info, mock_get_provider):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        task_info.get.side_effect = [task_info['volumes_info']]
        prov_fun = mock_get_provider.return_value.deploy_replica_disks
        expected_result = {"volumes_info": mock_check_vol_info.return_value}

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            destination['type'], constants.PROVIDER_TYPE_TRANSFER_IMPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['target_environment'], mock.sentinel.instance,
            task_info['export_info'], task_info['volumes_info'])
        mock_validate_value.assert_called_once_with(
            prov_fun.return_value, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)
        mock_check_vol_info.assert_called_once_with(
            task_info['export_info'], prov_fun.return_value)


class DeleteReplicaSourceDiskSnapshotsTaskTestCase(
        test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(DeleteReplicaSourceDiskSnapshotsTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.DeleteReplicaSourceDiskSnapshotsTask()

    @mock.patch('coriolis.events.EventManager')
    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch.object(replica_tasks, '_get_volumes_info')
    def test__run(self, mock_get_vol_info, mock_get_conn_info,
                  mock_get_provider, _):
        origin = mock.MagicMock()
        task_info = mock.MagicMock()
        task_info.get.side_effect = [task_info['volumes_info']]
        prov_fun = mock_get_provider.return_value\
            .delete_replica_source_snapshots
        expected_result = {"volumes_info": prov_fun.return_value}

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, origin,
            mock.sentinel.destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            origin['type'], constants.PROVIDER_TYPE_TRANSFER_EXPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, origin)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['source_environment'], mock_get_vol_info.return_value)
        mock_get_vol_info.assert_called_once_with(task_info)

    @mock.patch('coriolis.events.EventManager')
    def test__run_no_volumes_info(self, _):
        task_info = {}
        expected_result = {"volumes_info": []}
        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            mock.sentinel.destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)


class DeleteReplicaDisksTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(DeleteReplicaDisksTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.DeleteReplicaDisksTask()

    @mock.patch('coriolis.events.EventManager', mock.MagicMock())
    @mock.patch('coriolis.utils.sanitize_task_info', mock.MagicMock())
    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch.object(replica_tasks, '_get_volumes_info')
    def test__run(self, mock_get_vol_info, mock_get_conn_info,
                  mock_get_provider):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        task_info.get.side_effect = [task_info['volumes_info']]
        prov_fun = mock_get_provider.return_value.delete_replica_disks
        expected_result = {"volumes_info": []}

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            destination['type'], constants.PROVIDER_TYPE_TRANSFER_IMPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['target_environment'], mock_get_vol_info.return_value)
        mock_get_vol_info.assert_called_once_with(task_info)

    @mock.patch('coriolis.events.EventManager')
    def test__run_no_volumes_info(self, _):
        task_info = {}
        expected_result = {"volumes_info": []}
        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            mock.sentinel.destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)


@ddt.ddt
class DeployReplicaSourceResourcesTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(DeployReplicaSourceResourcesTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.DeployReplicaSourceResourcesTask()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.schemas.validate_value')
    @mock.patch('coriolis.tasks.base.marshal_migr_conn_info')
    @ddt.file_data('data/deploy_replica_source_resources_task_run.yml')
    @ddt.unpack
    def test__run(self, mock_marshal, mock_validate_value, mock_get_conn_info,
                  mock_get_provider, log_expected, replica_resources_info,
                  expected_result):
        origin = mock.MagicMock()
        task_info = mock.MagicMock()
        task_info.get.side_effect = [task_info['source_environment']]
        prov_fun = (
            mock_get_provider.return_value.deploy_replica_source_resources)
        prov_fun.return_value = replica_resources_info
        mock_marshal.return_value = replica_resources_info.get(
            'connection_info', {})

        def _get_result():
            args = [
                mock.sentinel.ctxt, mock.sentinel.instance, origin,
                mock.sentinel.destination, task_info,
                mock.sentinel.event_handler]
            if log_expected:
                with self.assertLogs('coriolis.tasks.replica_tasks',
                                     level=replica_tasks.logging.WARN):
                    result = self.task_runner._run(*args)
                    mock_marshal.assert_not_called()
                    mock_validate_value.assert_not_called()
                    return result

            result = self.task_runner._run(*args)
            mock_marshal.assert_called_once_with(
                replica_resources_info['connection_info'])
            mock_validate_value.assert_called_once_with(
                mock_marshal.return_value,
                schemas.CORIOLIS_REPLICATION_WORKER_CONN_INFO_SCHEMA,
                raise_on_error=False)
            return result

        self.assertEqual(_get_result(), expected_result)
        mock_get_provider.assert_called_once_with(
            origin['type'], constants.PROVIDER_TYPE_TRANSFER_EXPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, origin)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['export_info'], task_info['source_environment'])


@ddt.ddt
class DeleteReplicaSourceResourcesTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(DeleteReplicaSourceResourcesTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.DeleteReplicaSourceResourcesTask()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    def test__run(self, mock_get_conn_info, mock_get_provider):
        origin = mock.MagicMock()
        task_info = mock.MagicMock()
        prov_fun = (
            mock_get_provider.return_value.delete_replica_source_resources)

        expected_result = {
            "source_resources": None, "source_resources_connection_info": None}
        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, origin,
            mock.sentinel.destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            origin['type'], constants.PROVIDER_TYPE_TRANSFER_EXPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, origin)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            origin['source_environment'], task_info['source_resources'])

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    def test__run_no_resources(self, mock_get_conn_info, mock_get_provider):
        origin = mock.MagicMock()
        task_info = {"source_resources": None}
        prov_fun = (
            mock_get_provider.return_value.delete_replica_source_resources)

        expected_result = {
            "source_resources": None, "source_resources_connection_info": None}
        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, origin,
            mock.sentinel.destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            origin['type'], constants.PROVIDER_TYPE_TRANSFER_EXPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, origin)
        prov_fun.assert_not_called()


@ddt.ddt
class DeployReplicaTargetResourcesTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(DeployReplicaTargetResourcesTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.DeployReplicaTargetResourcesTask()

    @mock.patch.object(backup_writers, 'BackupWritersFactory')
    @mock.patch('coriolis.tasks.base.marshal_migr_conn_info')
    @mock.patch('coriolis.schemas.validate_value')
    @ddt.file_data("data/replica_deployment_conn_info_validation.yml")
    @ddt.unpack
    def test__validate_connection_info(self, mock_validate, mock_marshal,
                                       mock_backup_writer, migr_conn_info,
                                       writer_fails, validates_conn_info,
                                       marshals_conn_info):
        conn_details = copy.copy(migr_conn_info.get('connection_details'))
        if writer_fails:
            mock_backup_writer.side_effect = Exception
            with self.assertLogs('coriolis.tasks.replica_tasks',
                                 level=replica_tasks.logging.WARN):
                self.task_runner._validate_connection_info(migr_conn_info)
        else:
            self.assertEqual(
                None,
                self.task_runner._validate_connection_info(migr_conn_info))
            if validates_conn_info:
                mock_validate.assert_called_once_with(
                    migr_conn_info,
                    schemas.CORIOLIS_DISK_SYNC_RESOURCES_CONN_INFO_SCHEMA,
                    raise_on_error=False)
            if marshals_conn_info:
                mock_marshal.assert_called_once_with(conn_details)

    @mock.patch.object(replica_tasks.DeployReplicaTargetResourcesTask,
                       '_validate_connection_info')
    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.tasks.replica_tasks._get_volumes_info')
    @mock.patch(
        'coriolis.tasks.replica_tasks._check_ensure_volumes_info_ordering')
    @mock.patch('coriolis.schemas.validate_value')
    @ddt.file_data('data/deploy_replica_target_resources_task_run.yml')
    @ddt.unpack
    def test__run(self, mock_validate_value, mock_check_vol_info,
                  mock_get_vol_info, mock_get_conn_info, mock_get_provider,
                  mock_validate_conn_info, replica_resources_info,
                  expected_result):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        prov_fun = (
            mock_get_provider.return_value.deploy_replica_target_resources)
        prov_fun.return_value = replica_resources_info
        mock_check_vol_info.return_value = (
            replica_resources_info.get('volumes_info'))
        mock_get_vol_info.return_value = "task_vol_info"

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            destination['type'], constants.PROVIDER_TYPE_TRANSFER_IMPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        mock_get_vol_info.assert_called_once_with(task_info)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['target_environment'], mock_get_vol_info.return_value)
        mock_validate_value.assert_called_once_with(
            replica_resources_info,
            schemas.CORIOLIS_DISK_SYNC_RESOURCES_INFO_SCHEMA,
            raise_on_error=False)


@ddt.ddt
class DeleteReplicaTargetResourcesTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(DeleteReplicaTargetResourcesTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.DeleteReplicaTargetResourcesTask()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @ddt.data(
        ({}, False),
        (None, False),
        ({"res1": "id1", "res2": "id2"}, True),
    )
    def test__run(self, data, mock_get_conn_info, mock_get_provider):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        task_info.get.return_value = data[0]
        prov_fun = (
            mock_get_provider.return_value.delete_replica_target_resources)
        expected_result = {
            "target_resources": None, "target_resources_connection_info": None}

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            destination['type'], constants.PROVIDER_TYPE_TRANSFER_IMPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        if data[1]:
            prov_fun.assert_called_once_with(
                mock.sentinel.ctxt, mock_get_conn_info.return_value,
                task_info['target_environment'], data[0])
        else:
            prov_fun.assert_not_called()


class DeployReplicaInstanceResourcesTaskTestCase(
        test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(DeployReplicaInstanceResourcesTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.DeployReplicaInstanceResourcesTask()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.tasks.replica_tasks._get_volumes_info')
    def test__run(self, mock_get_vol_info, mock_get_conn_info,
                  mock_get_provider):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        task_info.get.return_value = True
        prov_fun = mock_get_provider.return_value.deploy_replica_instance
        expected_result = {
            "instance_deployment_info": (
                prov_fun.return_value['instance_deployment_info'])}

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            destination['type'], constants.PROVIDER_TYPE_TRANSFER_IMPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        mock_get_vol_info.assert_called_once_with(task_info)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['target_environment'], mock.sentinel.instance,
            task_info['export_info'], mock_get_vol_info.return_value, True)


class FinalizeReplicaInstanceDeploymentTaskTestCase(
        test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(FinalizeReplicaInstanceDeploymentTaskTestCase, self).setUp()
        self.task_runner = (
            replica_tasks.FinalizeReplicaInstanceDeploymentTask())

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    def test__run(self, mock_get_conn_info, mock_get_provider):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        prov_fun = mock_get_provider.return_value\
            .finalize_replica_instance_deployment
        expected_result = {"transfer_result": prov_fun.return_value}

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            destination['type'], constants.PROVIDER_TYPE_TRANSFER_IMPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['target_environment'],
            task_info['instance_deployment_info'])

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    def test__run_no_result(self, mock_get_conn_info, mock_get_provider):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        prov_fun = mock_get_provider.return_value\
            .finalize_replica_instance_deployment
        prov_fun.return_value = None
        expected_result = {"transfer_result": None}

        with self.assertLogs('coriolis.tasks.replica_tasks',
                             level=replica_tasks.logging.WARN):
            result = self.task_runner._run(
                mock.sentinel.ctxt, mock.sentinel.instance,
                mock.sentinel.origin, destination, task_info,
                mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            destination['type'], constants.PROVIDER_TYPE_TRANSFER_IMPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['target_environment'],
            task_info['instance_deployment_info'])


class CleanupFailedReplicaInstanceDeploymentTaskTestCase(
        test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(CleanupFailedReplicaInstanceDeploymentTaskTestCase, self).setUp()
        self.task_runner = (
            replica_tasks.CleanupFailedReplicaInstanceDeploymentTask())

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    def test__run(self, mock_get_conn_info, mock_get_provider):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        prov_fun = mock_get_provider.return_value\
            .cleanup_failed_replica_instance_deployment
        expected_result = {"instance_deployment_info": None}

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            destination['type'], constants.PROVIDER_TYPE_TRANSFER_IMPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['target_environment'],
            task_info['instance_deployment_info'])


class CreateReplicaDiskSnapshotsTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(CreateReplicaDiskSnapshotsTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.CreateReplicaDiskSnapshotsTask()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.tasks.replica_tasks._get_volumes_info')
    @mock.patch('coriolis.schemas.validate_value')
    @mock.patch(
        'coriolis.tasks.replica_tasks._check_ensure_volumes_info_ordering')
    def test__run(self, mock_check_ensure_volumes_ordering,
                  mock_validate_value, mock_get_vol_info, mock_get_conn_info,
                  mock_get_provider):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        prov_fun = mock_get_provider.return_value.create_replica_disk_snapshots
        expected_result = {
            "volumes_info": mock_check_ensure_volumes_ordering.return_value}

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            destination['type'], constants.PROVIDER_TYPE_TRANSFER_IMPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        mock_get_vol_info.assert_called_once_with(task_info)
        mock_validate_value.assert_called_once_with(
            prov_fun.return_value,
            schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)
        mock_check_ensure_volumes_ordering.assert_called_once_with(
            task_info['export_info'], prov_fun.return_value)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['target_environment'], mock_get_vol_info.return_value)


class DeleteReplicaTargetDiskSnapshotsTaskTestCase(
        test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(DeleteReplicaTargetDiskSnapshotsTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.DeleteReplicaTargetDiskSnapshotsTask()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.tasks.replica_tasks._get_volumes_info')
    @mock.patch('coriolis.schemas.validate_value')
    @mock.patch(
        'coriolis.tasks.replica_tasks._check_ensure_volumes_info_ordering')
    def test__run(self, mock_check_ensure_volumes_ordering,
                  mock_validate_value, mock_get_vol_info, mock_get_conn_info,
                  mock_get_provider):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        prov_fun = mock_get_provider.return_value\
            .delete_replica_target_disk_snapshots
        expected_result = {
            "volumes_info": mock_check_ensure_volumes_ordering.return_value}

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            destination['type'], constants.PROVIDER_TYPE_TRANSFER_IMPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        mock_get_vol_info.assert_called_once_with(task_info)
        mock_validate_value.assert_called_once_with(
            prov_fun.return_value,
            schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)
        mock_check_ensure_volumes_ordering.assert_called_once_with(
            task_info['export_info'], prov_fun.return_value)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['target_environment'], mock_get_vol_info.return_value)


class RestoreReplicaDiskSnapshotsTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(RestoreReplicaDiskSnapshotsTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.RestoreReplicaDiskSnapshotsTask()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.tasks.replica_tasks._get_volumes_info')
    @mock.patch('coriolis.schemas.validate_value')
    @mock.patch(
        'coriolis.tasks.replica_tasks._check_ensure_volumes_info_ordering')
    def test__run(self, mock_check_ensure_volumes_ordering,
                  mock_validate_value, mock_get_vol_info, mock_get_conn_info,
                  mock_get_provider):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        prov_fun = mock_get_provider.return_value\
            .restore_replica_disk_snapshots
        expected_result = {
            "volumes_info": mock_check_ensure_volumes_ordering.return_value}

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_get_provider.assert_called_once_with(
            destination['type'], constants.PROVIDER_TYPE_TRANSFER_IMPORT,
            mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        mock_get_vol_info.assert_called_once_with(task_info)
        mock_validate_value.assert_called_once_with(
            prov_fun.return_value,
            schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)
        mock_check_ensure_volumes_ordering.assert_called_once_with(
            task_info['export_info'], prov_fun.return_value)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['target_environment'], mock_get_vol_info.return_value)


class ValidateReplicaExecutionSourceInputsTaskTestCase(
        test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(ValidateReplicaExecutionSourceInputsTaskTestCase, self).setUp()
        self.task_runner = (
            replica_tasks.ValidateReplicaExecutionSourceInputsTask())

    @mock.patch('coriolis.events.EventManager')
    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    def test__run(self, mock_get_conn_info, mock_get_provider,
                  mock_event_manager):
        origin = mock.MagicMock()
        task_info = mock.MagicMock()
        prov_fun = mock_get_provider.return_value.validate_replica_export_input

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, origin,
            mock.sentinel.destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, {})
        mock_event_manager.assert_called_once_with(mock.sentinel.event_handler)
        mock_get_provider.assert_called_once_with(
            origin['type'], constants.PROVIDER_TYPE_VALIDATE_TRANSFER_EXPORT,
            mock.sentinel.event_handler, raise_if_not_found=False)
        mock_get_conn_info.assert_called_once_with(mock.sentinel.ctxt, origin)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            mock.sentinel.instance,
            source_environment=task_info['source_environment'])
        mock_event_manager.return_value.progress_update.assert_not_called()

    @mock.patch('coriolis.events.EventManager')
    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    def test__run_no_source_provider(self, mock_get_conn_info,
                                     mock_get_provider, mock_event_manager):
        origin = mock.MagicMock()
        task_info = mock.MagicMock()
        prov_fun = mock_get_provider.return_value.validate_replica_export_input
        mock_get_provider.side_effect = [None]

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, origin,
            mock.sentinel.destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, {})
        mock_event_manager.assert_called_once_with(mock.sentinel.event_handler)
        mock_get_provider.assert_called_once_with(
            origin['type'], constants.PROVIDER_TYPE_VALIDATE_TRANSFER_EXPORT,
            mock.sentinel.event_handler, raise_if_not_found=False)
        mock_get_conn_info.assert_called_once_with(mock.sentinel.ctxt, origin)
        prov_fun.assert_not_called()
        mock_event_manager.return_value.progress_update.assert_called_once()


class ValidateReplicaExecutionDestinationInputsTaskTestCase(
        test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(
            ValidateReplicaExecutionDestinationInputsTaskTestCase,
            self).setUp()
        self.task_runner = (
            replica_tasks.ValidateReplicaExecutionDestinationInputsTask())

    def test__validate_provider_replica_import_input(self):
        provider = mock.MagicMock()
        result = self.task_runner._validate_provider_replica_import_input(
            provider, mock.sentinel.ctxt, mock.sentinel.conn_info,
            mock.sentinel.target_environment, mock.sentinel.export_info)
        self.assertEqual(result, None)
        provider.validate_replica_import_input.assert_called_once_with(
            mock.sentinel.ctxt, mock.sentinel.conn_info,
            mock.sentinel.target_environment, mock.sentinel.export_info,
            check_os_morphing_resources=False, check_final_vm_params=False)

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch.object(
        replica_tasks.ValidateReplicaExecutionDestinationInputsTask,
        '_validate_provider_replica_import_input')
    def test__run(self, mock_validate_replica_inputs, mock_get_conn_info,
                  mock_get_provider):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        task_info.get.side_effect = [task_info['export_info']]

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, None)
        self.assertEqual(result, {})
        mock_get_provider.assert_called_once_with(
            destination['type'],
            constants.PROVIDER_TYPE_VALIDATE_TRANSFER_IMPORT, None,
            raise_if_not_found=False)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        mock_validate_replica_inputs.assert_called_once_with(
            mock_get_provider.return_value, mock.sentinel.ctxt,
            mock_get_conn_info.return_value, task_info['target_environment'],
            task_info['export_info'])

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch.object(
        replica_tasks.ValidateReplicaExecutionDestinationInputsTask,
        '_validate_provider_replica_import_input')
    def test__run_no_destination_provider(
            self, mock_validate_replica_inputs, mock_get_conn_info,
            mock_get_provider):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        mock_get_provider.return_value = None

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, None)
        self.assertEqual(result, {})
        mock_get_provider.assert_called_once_with(
            destination['type'],
            constants.PROVIDER_TYPE_VALIDATE_TRANSFER_IMPORT, None,
            raise_if_not_found=False)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        mock_validate_replica_inputs.assert_not_called()

    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch.object(
        replica_tasks.ValidateReplicaExecutionDestinationInputsTask,
        '_validate_provider_replica_import_input')
    def test__run_no_export_info(
            self, mock_validate_replica_inputs, mock_get_conn_info,
            mock_get_provider):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        task_info.get.side_effect = [None]

        self.assertRaises(
            exception.InvalidActionTasksExecutionState, self.task_runner._run,
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, None)
        mock_get_provider.assert_called_once_with(
            destination['type'],
            constants.PROVIDER_TYPE_VALIDATE_TRANSFER_IMPORT, None,
            raise_if_not_found=False)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        mock_validate_replica_inputs.assert_not_called()


class ValidateReplicaDeploymentParametersTaskTestCase(
        test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(
            ValidateReplicaDeploymentParametersTaskTestCase,
            self).setUp()
        self.task_runner = (
            replica_tasks.ValidateReplicaDeploymentParametersTask())

    @mock.patch('coriolis.events.EventManager')
    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.schemas.validate_value')
    def test__run(self, mock_validate_value, mock_get_conn_info,
                  mock_get_provider, mock_event_manager):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        prov_fun = (
            mock_get_provider.return_value.validate_replica_deployment_input)

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, {})

        mock_event_manager.assert_called_once_with(mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        mock_validate_value.assert_called_once_with(
            task_info['export_info'], schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA)
        mock_get_provider.assert_called_once_with(
            destination['type'],
            constants.PROVIDER_TYPE_VALIDATE_TRANSFER_IMPORT,
            mock.sentinel.event_handler, raise_if_not_found=False)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            task_info['target_environment'], task_info['export_info'])

    @mock.patch('coriolis.events.EventManager')
    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.schemas.validate_value')
    def test__run_no_dest_provider(
            self, mock_validate_value, mock_get_conn_info, mock_get_provider,
            mock_event_manager):
        destination = mock.MagicMock()
        task_info = mock.MagicMock()
        prov_fun = (
            mock_get_provider.return_value.validate_replica_deployment_input)
        mock_get_provider.return_value = None

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, {})

        mock_event_manager.assert_called_once_with(mock.sentinel.event_handler)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        mock_validate_value.assert_called_once_with(
            task_info['export_info'], schemas.CORIOLIS_VM_EXPORT_INFO_SCHEMA)
        mock_get_provider.assert_called_once_with(
            destination['type'],
            constants.PROVIDER_TYPE_VALIDATE_TRANSFER_IMPORT,
            mock.sentinel.event_handler, raise_if_not_found=False)
        prov_fun.assert_not_called()


class UpdateSourceReplicaTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(UpdateSourceReplicaTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.UpdateSourceReplicaTask()

    @mock.patch('coriolis.events.EventManager')
    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.schemas.validate_value')
    def test__run_no_new_source_env(
            self, mock_validate_value, mock_get_conn_info, mock_get_provider,
            mock_event_manager):
        old_source_env = {"old": "opt"}
        volumes_info = [{"id": "vol_id1"}]
        origin = mock.MagicMock()
        origin.get.side_effect = [old_source_env]
        task_info = mock.MagicMock()
        task_info.get.side_effect = [volumes_info, None]
        prov_fun = (mock_get_provider.return_value.
                    check_update_source_environment_params)
        expected_result = {
            "volumes_info": volumes_info, "source_environment": old_source_env}

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, origin,
            mock.sentinel.destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)

        mock_event_manager.assert_called_once_with(mock.sentinel.event_handler)
        mock_get_provider.assert_not_called()
        mock_get_conn_info.assert_not_called()
        mock_validate_value.assert_not_called()
        prov_fun.assert_not_called()

    @mock.patch('coriolis.events.EventManager')
    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.schemas.validate_value')
    def test__run_no_source_provider(
            self, mock_validate_value, mock_get_conn_info, mock_get_provider,
            mock_event_manager):
        old_source_env = {"opt": "old"}
        origin = mock.MagicMock()
        origin.get.side_effect = [old_source_env]

        volumes_info = [{"id": "vol_id1"}]
        new_source_env = {"opt": "new"}
        task_info = mock.MagicMock()
        task_info.get.side_effect = [volumes_info, new_source_env]
        prov_fun = (mock_get_provider.return_value.
                    check_update_source_environment_params)
        mock_get_provider.return_value = None

        self.assertRaises(
            exception.InvalidActionTasksExecutionState,
            self.task_runner._run,
            mock.sentinel.ctxt, mock.sentinel.instance, origin,
            mock.sentinel.destination, task_info, mock.sentinel.event_handler)

        mock_event_manager.assert_called_once_with(mock.sentinel.event_handler)
        mock_get_provider.assert_called_once_with(
            origin['type'], constants.PROVIDER_TYPE_SOURCE_TRANSFER_UPDATE,
            mock.sentinel.event_handler, raise_if_not_found=False)
        mock_get_conn_info.assert_not_called()
        mock_validate_value.assert_not_called()
        prov_fun.assert_not_called()

    @mock.patch('coriolis.events.EventManager')
    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.schemas.validate_value')
    def test__run_no_volumes_info(
            self, mock_validate_value, mock_get_conn_info, mock_get_provider,
            mock_event_manager):
        old_source_env = {"opt": "old"}
        origin = mock.MagicMock()
        origin.get.side_effect = [old_source_env]

        volumes_info = [{"id": "vol_id1"}]
        new_source_env = {"opt": "new"}
        task_info = mock.MagicMock()
        task_info.get.side_effect = [
            volumes_info, new_source_env, volumes_info]
        prov_fun = (mock_get_provider.return_value.
                    check_update_source_environment_params)
        prov_fun.return_value = None
        expected_result = {"volumes_info": volumes_info,
                           "source_environment": new_source_env}
        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, origin,
            mock.sentinel.destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)

        mock_event_manager.assert_called_once_with(mock.sentinel.event_handler)
        mock_get_provider.assert_called_once_with(
            origin['type'], constants.PROVIDER_TYPE_SOURCE_TRANSFER_UPDATE,
            mock.sentinel.event_handler, raise_if_not_found=False)
        mock_get_conn_info.assert_called_once_with(mock.sentinel.ctxt, origin)
        mock_validate_value.assert_not_called()
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            mock.sentinel.instance, volumes_info, old_source_env,
            new_source_env)

    @mock.patch('coriolis.events.EventManager')
    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.schemas.validate_value')
    def test__run(
            self, mock_validate_value, mock_get_conn_info, mock_get_provider,
            mock_event_manager):
        old_source_env = {"opt": "old"}
        origin = mock.MagicMock()
        origin.get.side_effect = [old_source_env]

        volumes_info = [{"id": "vol_id1"}]
        new_source_env = {"opt": "new"}
        task_info = mock.MagicMock()
        task_info.get.side_effect = [volumes_info, new_source_env]
        prov_fun = (mock_get_provider.return_value.
                    check_update_source_environment_params)
        expected_result = {"volumes_info": prov_fun.return_value,
                           "source_environment": new_source_env}
        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, origin,
            mock.sentinel.destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)

        mock_event_manager.assert_called_once_with(mock.sentinel.event_handler)
        mock_get_provider.assert_called_once_with(
            origin['type'], constants.PROVIDER_TYPE_SOURCE_TRANSFER_UPDATE,
            mock.sentinel.event_handler, raise_if_not_found=False)
        mock_get_conn_info.assert_called_once_with(mock.sentinel.ctxt, origin)
        mock_validate_value.assert_called_once_with(
            prov_fun.return_value, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            mock.sentinel.instance, volumes_info, old_source_env,
            new_source_env)


class UpdateDestinationReplicaTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(UpdateDestinationReplicaTaskTestCase, self).setUp()
        self.task_runner = replica_tasks.UpdateDestinationReplicaTask()

    @mock.patch('coriolis.events.EventManager')
    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.schemas.validate_value')
    def test__run_no_new_dest_env(
            self, mock_validate_value, mock_get_conn_info, mock_get_provider,
            mock_event_manager):
        old_dest_env = {"old": "opt"}
        volumes_info = [{"id": "vol_id1"}]
        destination = mock.MagicMock()
        destination.get.side_effect = [old_dest_env]
        task_info = mock.MagicMock()
        task_info.get.side_effect = [volumes_info, None]
        prov_fun = (mock_get_provider.return_value.
                    check_update_destination_environment_params)
        expected_result = {
            "volumes_info": volumes_info, "target_environment": old_dest_env}

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)

        mock_event_manager.assert_called_once_with(mock.sentinel.event_handler)
        mock_get_provider.assert_not_called()
        mock_get_conn_info.assert_not_called()
        mock_validate_value.assert_not_called()
        prov_fun.assert_not_called()

    @mock.patch('coriolis.events.EventManager')
    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.schemas.validate_value')
    def test__run_no_dest_provider(
            self, mock_validate_value, mock_get_conn_info, mock_get_provider,
            mock_event_manager):
        old_dest_env = {"opt": "old"}
        destination = mock.MagicMock()
        destination.get.side_effect = [old_dest_env]

        volumes_info = [{"id": "vol_id1"}]
        new_dest_env = {"opt": "new"}
        task_info = mock.MagicMock()
        task_info.get.side_effect = [volumes_info, new_dest_env]
        prov_fun = (mock_get_provider.return_value.
                    check_update_destination_environment_params)
        mock_get_provider.return_value = None

        self.assertRaises(
            exception.InvalidActionTasksExecutionState,
            self.task_runner._run,
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)

        mock_event_manager.assert_called_once_with(mock.sentinel.event_handler)
        mock_get_provider.assert_called_once_with(
            destination['type'],
            constants.PROVIDER_TYPE_DESTINATION_TRANSFER_UPDATE,
            mock.sentinel.event_handler, raise_if_not_found=False)
        mock_get_conn_info.assert_not_called()
        mock_validate_value.assert_not_called()
        prov_fun.assert_not_called()

    @mock.patch('coriolis.events.EventManager')
    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.schemas.validate_value')
    def test__run_no_volumes_info(
            self, mock_validate_value, mock_get_conn_info, mock_get_provider,
            mock_event_manager):
        old_dest_env = {"opt": "old"}
        destination = mock.MagicMock()
        destination.get.side_effect = [old_dest_env]

        volumes_info = [{"id": "vol_id1"}]
        new_dest_env = {"opt": "new"}
        export_info = {"export": "info"}
        task_info = mock.MagicMock()
        task_info.get.side_effect = [
            volumes_info, new_dest_env, export_info, volumes_info]
        prov_fun = (mock_get_provider.return_value.
                    check_update_destination_environment_params)
        prov_fun.return_value = None
        expected_result = {"volumes_info": volumes_info,
                           "target_environment": new_dest_env}
        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)

        mock_event_manager.assert_called_once_with(mock.sentinel.event_handler)
        mock_get_provider.assert_called_once_with(
            destination['type'],
            constants.PROVIDER_TYPE_DESTINATION_TRANSFER_UPDATE,
            mock.sentinel.event_handler, raise_if_not_found=False)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        mock_validate_value.assert_not_called()
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            export_info, volumes_info, old_dest_env, new_dest_env)

    @mock.patch('coriolis.events.EventManager')
    @mock.patch('coriolis.providers.factory.get_provider')
    @mock.patch('coriolis.tasks.base.get_connection_info')
    @mock.patch('coriolis.schemas.validate_value')
    @mock.patch.object(replica_tasks, '_check_ensure_volumes_info_ordering')
    def test__run(
            self, mock_check_vol_ordering, mock_validate_value,
            mock_get_conn_info, mock_get_provider, mock_event_manager):
        old_dest_env = {"opt": "old"}
        destination = mock.MagicMock()
        destination.get.side_effect = [old_dest_env]

        volumes_info = [{"id": "vol_id1"}]
        new_dest_env = {"opt": "new"}
        export_info = {"name": "instance_name", "id": "instance_id"}
        task_info = mock.MagicMock()
        task_info.get.side_effect = [volumes_info, new_dest_env, export_info]
        prov_fun = (mock_get_provider.return_value.
                    check_update_destination_environment_params)
        expected_result = {
            "volumes_info": mock_check_vol_ordering.return_value,
            "target_environment": new_dest_env}
        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            destination, task_info, mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)

        mock_event_manager.assert_called_once_with(mock.sentinel.event_handler)
        mock_get_provider.assert_called_once_with(
            destination['type'],
            constants.PROVIDER_TYPE_DESTINATION_TRANSFER_UPDATE,
            mock.sentinel.event_handler, raise_if_not_found=False)
        mock_get_conn_info.assert_called_once_with(
            mock.sentinel.ctxt, destination)
        mock_validate_value.assert_called_once_with(
            prov_fun.return_value, schemas.CORIOLIS_VOLUMES_INFO_SCHEMA)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, mock_get_conn_info.return_value,
            export_info, volumes_info, old_dest_env, new_dest_env)
        mock_check_vol_ordering.assert_called_once_with(
            export_info, prov_fun.return_value)
