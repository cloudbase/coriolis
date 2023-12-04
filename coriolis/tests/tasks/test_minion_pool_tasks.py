# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

import ddt

from coriolis import constants
from coriolis import exception
from coriolis.tasks import minion_pool_tasks as mp_tasks
from coriolis.tests import test_base

MOCK_MINION_CONN_INFO = {"conn_info1": "value1", "conn_info2": "value2"}
MOCK_MINION_BACKUP_WRITER_CONN_INFO = {
    "connection_details": {"user": "username"}}
MOCK_MINION_CONN_INFO_FIELD = "destination_minion_connection_info"
MOCK_MINION_PROPS_TASK_INFO_FIELD = "destination_minion_provider_properties"
MOCK_MARSHALED_INFO = {"marshaled": "info"}
MOCK_UNMARSHALED_INFO = {"unmarshaled": "info"}


class CoriolisBaseMinionPoolTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(CoriolisBaseMinionPoolTaskTestCase, self).setUp()
        self.task_runner = None

    def _setup_base_mocks(self):
        self.destination = mock.MagicMock()
        self.task_info = mock.MagicMock()
        self.provider_type = constants.PROVIDER_TYPE_DESTINATION_MINION_POOL

        get_provider_patcher = mock.patch(
            'coriolis.providers.factory.get_provider')
        self.mock_get_provider = get_provider_patcher.start()
        self.addCleanup(get_provider_patcher.stop)

        patcher = mock.patch.multiple(
            self.task_runner.__class__,
            get_required_provider_types=mock.MagicMock(
                return_value={
                    constants.PROVIDER_PLATFORM_DESTINATION: [
                        constants.PROVIDER_TYPE_DESTINATION_MINION_POOL]}),
            get_required_platform=mock.MagicMock(
                return_value=constants.PROVIDER_PLATFORM_DESTINATION),
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        mp_tasks_methods_patches = mock.patch.multiple(
            mp_tasks,
            _check_missing_minion_properties=mock.MagicMock(),
            _get_minion_conn_info=mock.MagicMock(
                return_value=MOCK_MINION_CONN_INFO),
            _get_minion_backup_writer_conn_info=mock.MagicMock(
                return_value=MOCK_MINION_BACKUP_WRITER_CONN_INFO),
            _get_platform_to_target=mock.MagicMock(
                return_value=self.destination),
        )
        mp_tasks_methods_patches.start()
        self.addCleanup(mp_tasks_methods_patches.stop)

        get_conn_info_patcher = mock.patch(
            'coriolis.tasks.base.get_connection_info')
        self.mock_get_conn_info = get_conn_info_patcher.start()
        self.addCleanup(get_conn_info_patcher.stop)

    def _setup_vol_attachment_tasks_mocks(self):
        self.provider_disk_op = mock.MagicMock(
            return_value=(
                self.mock_get_provider.return_value.attach_volumes_to_minion))

        patcher = mock.patch.multiple(
            self.task_runner.__class__,
            _get_volumes_info_from_task_info=mock.MagicMock(
                return_value=self.task_info['volumes_info']),
            _get_minion_properties_task_info_field=mock.MagicMock(
                return_value=MOCK_MINION_PROPS_TASK_INFO_FIELD),
            _get_minion_connection_info_task_info_field=mock.MagicMock(
                return_value=MOCK_MINION_CONN_INFO_FIELD),
            _get_provider_disk_operation=self.provider_disk_op,
            _get_minion_task_info_field_mappings=mock.MagicMock(
                return_value=mp_tasks.TARGET_MINION_TASK_INFO_FIELD_MAPPINGS),
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _setup_validation_tasks_mocks(self):
        prov_fun = self.mock_get_provider.return_value\
            .validate_minion_compatibility_for_transfer
        self.provider_pool_validation_op = mock.MagicMock(
            return_value=prov_fun)

        patcher = mock.patch.multiple(
            self.task_runner.__class__,
            _get_minion_properties_task_info_field=mock.MagicMock(
                return_value="destination_minion_provider_properties"),
            _get_minion_task_info_field_mappings=mock.MagicMock(
                return_value=mp_tasks.TARGET_MINION_TASK_INFO_FIELD_MAPPINGS),
            _get_provider_pool_validation_operation=(
                self.provider_pool_validation_op),
        )
        patcher.start()
        self.addCleanup(patcher.stop)


@ddt.ddt
class MinionPoolTasksMethodsTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(MinionPoolTasksMethodsTestCase, self).setUp()
        marshal_migr_conn_info_patcher = mock.patch(
            'coriolis.tasks.base.marshal_migr_conn_info',
            return_value=MOCK_MARSHALED_INFO)
        self.mock_marshal_conn_info = marshal_migr_conn_info_patcher.start()
        self.addCleanup(marshal_migr_conn_info_patcher.stop)

    @ddt.data(
        {"platform_type": "", "expected_exception": NotImplementedError},

        {"platform_type": constants.PROVIDER_PLATFORM_SOURCE,
         "expected_result": constants.PROVIDER_TYPE_SOURCE_MINION_POOL},

        {"platform_type": constants.PROVIDER_PLATFORM_DESTINATION,
         "expected_result": constants.PROVIDER_TYPE_DESTINATION_MINION_POOL},
    )
    def test__get_required_minion_pool_provider_types_for_platform(self, data):
        platform_type = data.get('platform_type')
        expected_exception = data.get('expected_exception')
        expected_result = data.get('expected_result')
        fun = mp_tasks._get_required_minion_pool_provider_types_for_platform

        if expected_exception:
            self.assertRaises(expected_exception, fun, platform_type)
            return

        result = fun(platform_type)
        self.assertEqual(result, {platform_type: [expected_result]})

    @ddt.data(
        (constants.TASK_PLATFORM_SOURCE, mock.sentinel.origin),
        (constants.TASK_PLATFORM_DESTINATION, mock.sentinel.destination),
        ("", NotImplementedError),
    )
    def test__get_platform_to_target(self, data):
        args = [data[0], mock.sentinel.origin, mock.sentinel.destination]
        if not data[0]:
            self.assertRaises(
                data[1], mp_tasks._get_platform_to_target, *args)
            return

        result = mp_tasks._get_platform_to_target(*args)
        self.assertEqual(result, data[1])

    @ddt.data(
        ({}, None, False),
        ({'prop1': 'value1', 'prop2': 'value2'}, ['prop2'], False),
        ({'prop1': 'value1'}, [], False),
        ({}, ['prop2'], True),
        ({'prop1': 'value1'}, ['prop2'], True),
    )
    def test__check_missing_minion_properties(self, data):
        minion_properties = data[0]
        properties_to_check = data[1]
        logs_missing_props = data[2]

        def _get_result():
            if logs_missing_props:
                with self.assertLogs('coriolis.tasks.minion_pool_tasks',
                                     mp_tasks.logging.WARN):
                    return mp_tasks._check_missing_minion_properties(
                        mock.ANY, minion_properties,
                        properties_to_check=properties_to_check)
            return mp_tasks._check_missing_minion_properties(
                mock.ANY, minion_properties,
                properties_to_check=properties_to_check)

        self.assertEqual(_get_result(), None)

    @ddt.data(
        ({}, {}),
        ({"connection_info": {"some": "info"}}, MOCK_MARSHALED_INFO),
    )
    def test__get_minion_conn_info(self, data):
        minion_properties = data[0]
        expected_result = data[1]
        result = mp_tasks._get_minion_conn_info(minion_properties)
        self.assertEqual(result, expected_result)

    @ddt.data(
        ({}, {}, False),
        ({'backup_writer_connection_info': {"backup": "writer"}},
         {"backup": "writer"}, False),
        ({"backup_writer_connection_info": {
            "connection_details": {"conn": "info"},
            "backup": "writer"}},
         {"connection_details": MOCK_MARSHALED_INFO,
          "backup": "writer"},
         True),
    )
    def test__get_minion_backup_writer_conn_info(self, data):
        minion_properties = data[0]
        connection_details = minion_properties.get(
            'backup_writer_connection_info', {}).get(
                'connection_details', {}).copy()
        expected_result = data[1]
        marshaled = data[2]

        result = mp_tasks._get_minion_backup_writer_conn_info(
            minion_properties)
        self.assertEqual(result, expected_result)
        if marshaled:
            self.mock_marshal_conn_info.assert_called_once_with(
                connection_details)


class _BaseValidateMinionPoolOptsTaskTestCase(
        CoriolisBaseMinionPoolTaskTestCase):

    def setUp(self):
        super(_BaseValidateMinionPoolOptsTaskTestCase, self).setUp()
        self.task_runner = mp_tasks._BaseValidateMinionPoolOptionsTask()
        self._setup_base_mocks()

    def test__run(self):
        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.minion_pool_machine_id,
            mock.sentinel.origin, self.destination, self.task_info,
            mock.sentinel.event_handler)
        self.assertEqual(result, {})
        self.mock_get_provider.assert_called_once_with(
            self.destination['type'], self.provider_type,
            mock.sentinel.event_handler)
        self.mock_get_provider.return_value.\
            validate_minion_pool_environment_options.assert_called_once_with(
                mock.sentinel.ctxt, self.mock_get_conn_info.return_value,
                self.task_info['pool_environment_options'])


class _BaseCreateMinionMachineTaskTestCase(CoriolisBaseMinionPoolTaskTestCase):

    def setUp(self):
        super(_BaseCreateMinionMachineTaskTestCase, self).setUp()
        self.task_runner = mp_tasks._BaseCreateMinionMachineTask()
        self._setup_base_mocks()

    def test__run(self):
        prov_fun = self.mock_get_provider.return_value.create_minion
        expected_result = {
            "minion_connection_info": MOCK_MINION_CONN_INFO,
            "minion_backup_writer_connection_info": (
                MOCK_MINION_BACKUP_WRITER_CONN_INFO),
            "minion_provider_properties": (
                prov_fun.return_value.get.return_value),
        }

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.minion_pool_machine_id,
            mock.sentinel.origin, self.destination, self.task_info,
            mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        self.mock_get_provider.assert_called_once_with(
            self.destination['type'], self.provider_type,
            mock.sentinel.event_handler)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, self.mock_get_conn_info.return_value,
            self.task_info['environment_options'],
            self.task_info['pool_identifier'],
            self.task_info['pool_os_type'],
            self.task_info['pool_shared_resources'],
            mock.sentinel.minion_pool_machine_id)


class _BaseDeleteMinionMachineTaskTestCase(CoriolisBaseMinionPoolTaskTestCase):

    def setUp(self):
        super(_BaseDeleteMinionMachineTaskTestCase, self).setUp()
        self.task_runner = mp_tasks._BaseDeleteMinionMachineTask()
        self._setup_base_mocks()

    def test__run(self):
        prov_fun = self.mock_get_provider.return_value.delete_minion

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.minion_pool_machine_id,
            mock.sentinel.origin, self.destination, self.task_info,
            mock.sentinel.event_handler)
        self.assertEqual(result, {})
        self.mock_get_provider.assert_called_once_with(
            self.destination['type'], self.provider_type,
            mock.sentinel.event_handler)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, self.mock_get_conn_info.return_value,
            self.task_info['minion_provider_properties'])


class _BaseSetUpPoolSupportingResourcesTaskTestCase(
        CoriolisBaseMinionPoolTaskTestCase):

    def setUp(self):
        super(_BaseSetUpPoolSupportingResourcesTaskTestCase, self).setUp()
        self.task_runner = mp_tasks._BaseSetUpPoolSupportingResourcesTask()
        self._setup_base_mocks()

    def test__run(self):
        prov_fun = self.mock_get_provider.return_value.\
            set_up_pool_shared_resources
        expected_result = {
            "pool_shared_resources": prov_fun.return_value
        }

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.minion_pool_machine_id,
            mock.sentinel.origin, self.destination, self.task_info,
            mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        self.mock_get_provider.assert_called_once_with(
            self.destination['type'], self.provider_type,
            mock.sentinel.event_handler)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, self.mock_get_conn_info.return_value,
            self.task_info['environment_options'],
            self.task_info['pool_identifier'])


class _BaseTearDownPoolSupportingResourcesTaskTestCase(
        CoriolisBaseMinionPoolTaskTestCase):

    def setUp(self):
        super(_BaseTearDownPoolSupportingResourcesTaskTestCase, self).setUp()
        self.task_runner = mp_tasks._BaseTearDownPoolSupportingResourcesTask()
        self._setup_base_mocks()

    def test__run(self):
        prov_fun = self.mock_get_provider.return_value.\
            tear_down_pool_shared_resources
        expected_result = {
            "pool_shared_resources": None
        }

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.minion_pool_machine_id,
            mock.sentinel.origin, self.destination, self.task_info,
            mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        self.mock_get_provider.assert_called_once_with(
            self.destination['type'], self.provider_type,
            mock.sentinel.event_handler)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, self.mock_get_conn_info.return_value,
            self.task_info['environment_options'],
            self.task_info['pool_shared_resources'])


@ddt.ddt
class _BaseVolumesMinionMachineAttachmentTask(
        CoriolisBaseMinionPoolTaskTestCase):

    def setUp(self):
        super(_BaseVolumesMinionMachineAttachmentTask, self).setUp()
        self.task_runner = mp_tasks._BaseVolumesMinionMachineAttachmentTask()
        self._setup_base_mocks()
        self._setup_vol_attachment_tasks_mocks()

    def test_get_required_task_info_properties(self):
        expected_result = [
            "destination_minion_provider_properties",
            "destination_minion_backup_writer_connection_info"]
        result = mp_tasks._BaseVolumesMinionMachineAttachmentTask.\
            get_required_task_info_properties()
        self.assertEqual(sorted(result), sorted(expected_result))

    def test_get_returned_task_info_properties(self):
        expected_result = [
            "target_resources", "target_resources_connection_info",
            "destination_minion_provider_properties"]
        result = mp_tasks._BaseVolumesMinionMachineAttachmentTask.\
            get_returned_task_info_properties()
        self.assertEqual(sorted(result), sorted(expected_result))

    @ddt.data(
        ({}, True),
        ({"volumes_info": []}, True),
        ({"volumes_info": [], "minion_properties": {}}, False),
    )
    def test__check_missing_disk_op_result_keys(self, data):
        disk_op_res = data[0]
        raise_expected = data[1]
        self.provider_disk_op.__name__ = "mock_name"

        if raise_expected:
            self.assertRaises(
                exception.CoriolisException,
                self.task_runner._check_missing_disk_op_result_keys,
                mock.sentinel.platform_to_target, disk_op_res)
        else:
            self.assertEqual(
                self.task_runner._check_missing_disk_op_result_keys(
                    mock.sentinel.platform_to_target, disk_op_res),
                None)

    @mock.patch.object(mp_tasks._BaseVolumesMinionMachineAttachmentTask,
                       '_check_missing_disk_op_result_keys')
    def test__run(self, mock_check_missing_disk_op_keys):

        prov_fun = self.provider_disk_op.return_value
        prov_fun_result = prov_fun.return_value
        expected_result = {
            "volumes_info": prov_fun_result['volumes_info'],
            MOCK_MINION_PROPS_TASK_INFO_FIELD: prov_fun_result[
                'minion_properties'],
            "target_resources": (
                prov_fun_result['minion_properties']),
            "target_resources_connection_info": self.task_info[
                'target_resources_connection_info'],
        }

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.minion_pool_machine_id,
            mock.sentinel.origin, self.destination, self.task_info,
            mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        mock_check_missing_disk_op_keys.assert_called_once_with(
            self.destination, prov_fun_result)
        self.mock_get_provider.assert_called_once_with(
            self.destination['type'], self.provider_type,
            mock.sentinel.event_handler)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, self.mock_get_conn_info.return_value,
            self.task_info[MOCK_MINION_PROPS_TASK_INFO_FIELD],
            self.task_info[MOCK_MINION_CONN_INFO_FIELD],
            self.task_info['volumes_info'])


class _BaseAttachVolumesToTransferMinionTaskTestCase(
        test_base.CoriolisBaseTestCase):

    def test_get_required_task_info_properties(self):
        mock_super_call = mock.MagicMock(return_value=["field1", "field2"])
        with mock.patch.object(
                mp_tasks._BaseVolumesMinionMachineAttachmentTask,
                'get_required_task_info_properties',
                mock_super_call):
            result = mp_tasks._BaseAttachVolumesToTransferMinionTask.\
                get_required_task_info_properties()
            self.assertEqual(
                sorted(result), sorted(['field1', 'field2', 'volumes_info']))

    def test_get_returned_task_info_properties(self):
        mock_super_call = mock.MagicMock(return_value=["field1", "field2"])
        with mock.patch.object(
                mp_tasks._BaseVolumesMinionMachineAttachmentTask,
                'get_returned_task_info_properties',
                mock_super_call):
            result = mp_tasks._BaseAttachVolumesToTransferMinionTask.\
                get_returned_task_info_properties()
            self.assertEqual(
                sorted(result), sorted(['field1', 'field2', 'volumes_info']))


@ddt.ddt
class AttachVolumesToOSMorphingMinionTaskTestCase(
        test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(AttachVolumesToOSMorphingMinionTaskTestCase, self).setUp()
        self.task_runner = mp_tasks.AttachVolumesToOSMorphingMinionTask()

    def test_get_required_task_info_properties(self):
        mock_super_call = mock.MagicMock(return_value=["field1", "field2"])
        with mock.patch.object(
                mp_tasks._BaseVolumesMinionMachineAttachmentTask,
                'get_required_task_info_properties',
                mock_super_call):
            result = mp_tasks.AttachVolumesToOSMorphingMinionTask.\
                get_required_task_info_properties()
            self.assertEqual(
                sorted(result),
                sorted(['field1', 'field2', 'instance_deployment_info']))

    def test_get_returned_task_info_properties(self):
        mock_super_call = mock.MagicMock(return_value=["field1", "field2"])
        with mock.patch.object(
                mp_tasks._BaseVolumesMinionMachineAttachmentTask,
                'get_returned_task_info_properties',
                mock_super_call):
            result = mp_tasks.AttachVolumesToOSMorphingMinionTask.\
                get_returned_task_info_properties()
            self.assertEqual(
                sorted(result),
                sorted(['field1', 'field2', 'instance_deployment_info']))

    @mock.patch.object(
        mp_tasks._BaseVolumesMinionMachineAttachmentTask, '_run')
    @ddt.data(
        {
            "task_info": {"instance_deployment_info": {}},
            "res": {"optional": "values"},
            "expected_result": {"optional": "values"}
        },
        {
            "task_info": {"instance_deployment_info": {}},
            "res": {"volumes_info": {"vol1": "id1", "vol2": "id2"}},
            "expected_result": {
                "instance_deployment_info": {
                    "volumes_info": {"vol1": "id1", "vol2": "id2"}}}
        },
    )
    def test__run(self, data, mock_super_run):
        task_info = data.get('task_info', {})
        res = data.get('res', {})
        expected_result = data.get('expected_result', {})
        mock_super_run.return_value = res
        args = [
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            mock.sentinel.destination, task_info, mock.sentinel.event_handler,
        ]

        result = self.task_runner._run(*args)
        self.assertEqual(result, expected_result)
        mock_super_run.assert_called_once_with(*args)


@ddt.ddt
class _BaseValidateMinionCompatibilityTaskTestCase(
        CoriolisBaseMinionPoolTaskTestCase):

    def setUp(self):
        super(_BaseValidateMinionCompatibilityTaskTestCase, self).setUp()
        self.task_runner = mp_tasks._BaseValidateMinionCompatibilityTask()
        self._setup_base_mocks()
        self._setup_validation_tasks_mocks()

    @mock.patch.object(mp_tasks._BaseValidateMinionCompatibilityTask,
                       '_get_transfer_properties_task_info_field',
                       return_value="target_environment")
    def test_get_required_task_info_properties(self, _):
        expected_result = [
            "export_info", "target_environment",
            "destination_minion_provider_properties",
            "destination_minion_backup_writer_connection_info"]
        result = mp_tasks._BaseValidateMinionCompatibilityTask\
            .get_required_task_info_properties()
        self.assertEqual(sorted(result), sorted(expected_result))

    def test_get_returned_task_info_properties(self):
        expected_result = [
            "target_resources", "target_resources_connection_info"]
        result = self.task_runner.__class__.get_returned_task_info_properties()
        self.assertEqual(sorted(result), sorted(expected_result))

    @ddt.data(
        (constants.PROVIDER_PLATFORM_SOURCE, "source_environment"),
        (constants.PROVIDER_PLATFORM_DESTINATION, "target_environment"),
        ("invalid", None),
    )
    def test__get_transfer_properties_task_info_field(self, data):
        test_fun = mp_tasks._BaseValidateMinionCompatibilityTask\
            ._get_transfer_properties_task_info_field
        with mock.patch.object(mp_tasks._BaseValidateMinionCompatibilityTask,
                               'get_required_platform', return_value=data[0]):
            if not data[1]:
                self.assertRaises(exception.CoriolisException, test_fun)
                return

            self.assertEqual(test_fun(), data[1])

    @mock.patch.object(mp_tasks._BaseValidateMinionCompatibilityTask,
                       '_get_transfer_properties_task_info_field',
                       return_value="target_environment")
    def test__run(self, _):
        prov_fun = self.provider_pool_validation_op.return_value
        expected_result = {
            "target_resources": self.task_info['minion_properties'],
            "target_resources_connection_info": self.task_info[
                'target_resources_connection_info'],
        }

        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.minion_pool_machine_id,
            mock.sentinel.origin, self.destination, self.task_info,
            mock.sentinel.event_handler)
        self.assertEqual(result, expected_result)
        self.mock_get_provider.assert_called_once_with(
            self.destination['type'], self.provider_type,
            mock.sentinel.event_handler)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, self.mock_get_conn_info.return_value,
            self.task_info["export_info"],
            self.task_info["target_environment"],
            self.task_info[MOCK_MINION_PROPS_TASK_INFO_FIELD])


class _BaseReleaseMinionTaskTestCase(test_base.CoriolisBaseTestCase):

    def setUp(self):
        super(_BaseReleaseMinionTaskTestCase, self).setUp()
        self.task_runner = mp_tasks._BaseReleaseMinionTask()

    @mock.patch.object(
        mp_tasks._BaseReleaseMinionTask,
        '_get_minion_task_info_field_mappings',
        return_value=mp_tasks.TARGET_MINION_TASK_INFO_FIELD_MAPPINGS)
    def test_get_required_task_info_properties(self, _):
        expected_result = [
            "destination_minion_provider_properties", "target_resources",
            "destination_minion_backup_writer_connection_info",
            "target_resources_connection_info"]
        result = self.task_runner.__class__.get_required_task_info_properties()
        self.assertEqual(sorted(result), sorted(expected_result))

    @mock.patch.object(mp_tasks._BaseReleaseMinionTask,
                       'get_returned_task_info_properties')
    def test__run(self, mock_returned_props):
        mock_returned_props.return_value = [
            "destination_minion_provider_properties", "target_resources",
            "destination_minion_backup_writer_connection_info",
            "target_resources_connection_info"]
        expected_result = {
            "destination_minion_provider_properties": None,
            "target_resources": None,
            "destination_minion_backup_writer_connection_info": None,
            "target_resources_connection_info": None,
        }
        mock_event_handler = mock.MagicMock()
        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            mock.sentinel.destination, mock.sentinel.task_info,
            mock_event_handler)
        self.assertEqual(result, expected_result)


@ddt.ddt
class CollectOSMorphingInfoTaskTestCase(CoriolisBaseMinionPoolTaskTestCase):

    def setUp(self):
        super(CollectOSMorphingInfoTaskTestCase, self).setUp()
        self.task_runner = mp_tasks.CollectOSMorphingInfoTask()
        self._setup_base_mocks()

    @ddt.data(
        ("result", True),
        ({}, True),
        ({"osmorphing_info": {"some": "info"}}, False),
    )
    def test__run(self, data):
        prov_fun = self.mock_get_provider.return_value\
            .get_additional_os_morphing_info
        prov_fun.return_value = data[0]
        raises_exception = data[1]
        args = [
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            self.destination, self.task_info, mock.sentinel.event_handler]
        if raises_exception:
            self.assertRaises(
                exception.CoriolisException, self.task_runner._run, *args)
            return

        expected_result = {
            "osmorphing_info": prov_fun.return_value["osmorphing_info"],
        }
        result = self.task_runner._run(*args)
        self.assertEqual(result, expected_result)
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, self.mock_get_conn_info.return_value,
            self.task_info['target_environment'],
            self.task_info['instance_deployment_info'])


class _BaseHealthcheckMinionMachineTaskTestCase(
        CoriolisBaseMinionPoolTaskTestCase):

    def setUp(self):
        super(_BaseHealthcheckMinionMachineTaskTestCase, self).setUp()
        self.task_runner = mp_tasks._BaseHealthcheckMinionMachineTask()
        self._setup_base_mocks()

    @mock.patch('coriolis.tasks.base.unmarshal_migr_conn_info',
                return_value=MOCK_UNMARSHALED_INFO)
    def test__run(self, mock_unmarshal_conn_info):
        prov_fun = self.mock_get_provider.return_value.healthcheck_minion
        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            self.destination, self.task_info, mock.sentinel.event_handler)
        self.assertEqual(result, {})
        mock_unmarshal_conn_info.assert_called_once_with(
            self.task_info['minion_connection_info'])
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, self.mock_get_conn_info.return_value,
            self.task_info['minion_properties'],
            mock_unmarshal_conn_info.return_value)


class _BasePowerCycleMinionTaskTestCase(CoriolisBaseMinionPoolTaskTestCase):

    def setUp(self):
        super(_BasePowerCycleMinionTaskTestCase, self).setUp()
        self.task_runner = mp_tasks._BasePowerCycleMinionTask()
        self._setup_base_mocks()

    @mock.patch.object(mp_tasks._BasePowerCycleMinionTask,
                       '_get_minion_power_cycle_op')
    def test__run(self, mock_get_power_op):
        prov_fun = mock_get_power_op.return_value
        result = self.task_runner._run(
            mock.sentinel.ctxt, mock.sentinel.instance, mock.sentinel.origin,
            self.destination, self.task_info, mock.sentinel.event_handler)
        self.assertEqual(result, {})
        prov_fun.assert_called_once_with(
            mock.sentinel.ctxt, self.mock_get_conn_info.return_value,
            self.task_info['minion_provider_properties'])
