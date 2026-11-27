# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

from coriolis import constants, exception, utils
from coriolis.providers import base, provider_utils
from coriolis.tests import test_base


class ProviderUtilsTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis provider utils module."""

    def _get_mock_conn_info(self):
        return {
            "ip": "1.2.3.4",
            "port": 2222,
            "username": "someUser",
            "password": "pwned",
            "pkey": mock.Mock(),
        }

    def test_get_storage_mapping_for_disk(self):
        storage_mappings = {
            'disk_mappings': [{'disk_id': '1', 'destination': 'backend1'}],
            'backend_mappings': [{'source': 'backend1', 'destination': 'backend2'}],
            'default': 'default_backend',
        }
        disk_info = {'id': '1', 'storage_backend_identifier': 'backend1'}
        storage_backends = [{'name': 'backend1'}, {'name': 'backend2'}]

        expected_result = 'backend1'
        result = provider_utils.get_storage_mapping_for_disk(
            storage_mappings,
            disk_info,
            storage_backends,
            config_default='default_backend',
            error_on_missing_mapping=False,
            error_on_backend_not_found=False,
        )

        self.assertEqual(expected_result, result)

    def test_get_storage_mapping_for_disk_backend_mapping_found(self):
        storage_mappings = {
            "backend_mappings": [{"source": "source1", "destination": "backend1"}]
        }
        disk_info = {"id": "1", "storage_backend_identifier": "source1"}
        storage_backends = [{"name": "backend1"}]

        with self.assertLogs(level=logging.DEBUG):
            result = provider_utils.get_storage_mapping_for_disk(
                storage_mappings,
                disk_info,
                storage_backends,
                error_on_backend_not_found=False,
                error_on_missing_mapping=False,
            )

            self.assertEqual("backend1", result)

    def test_get_storage_mapping_for_disk_no_mapping_found(self):
        storage_mappings = {}
        disk_info = {"id": "1"}
        storage_backends = [{"name": "backend1"}]

        with self.assertLogs(level=logging.WARN):
            self.assertRaises(
                exception.DiskStorageMappingNotFound,
                provider_utils.get_storage_mapping_for_disk,
                storage_mappings,
                disk_info,
                storage_backends,
                error_on_backend_not_found=False,
                error_on_missing_mapping=True,
            )

    def test_get_storage_mapping_for_disk_backend_not_found(self):
        storage_mappings = {"default": "backend1"}
        disk_info = {"id": "1", "storage_backend_identifier": "source_backend"}
        storage_backends = [{"name": "backend2"}]

        with self.assertLogs('coriolis.providers.provider_utils', level=logging.WARN):
            self.assertRaises(
                exception.StorageBackendNotFound,
                provider_utils.get_storage_mapping_for_disk,
                storage_mappings,
                disk_info,
                storage_backends,
                error_on_backend_not_found=True,
                error_on_missing_mapping=False,
            )

    def test_get_storage_mapping_for_disk_default_backend(self):
        storage_mappings = {'default': 'default_backend'}
        disk_info = {'id': '1', 'storage_backend_identifier': 'backend1'}
        storage_backends = [{'name': 'default_backend'}, {'name': 'backend2'}]

        result = provider_utils.get_storage_mapping_for_disk(
            storage_mappings,
            disk_info,
            storage_backends,
            config_default='default_backend',
            error_on_missing_mapping=False,
            error_on_backend_not_found=False,
        )

        self.assertEqual(result, 'default_backend')

    def test_check_changed_storage_mappings_empty_volumes_info(self):
        old_storage_mappings = {
            'backend_mappings': [{'key': 'value'}],
            'disk_mappings': [{'key': 'value'}],
        }
        new_storage_mappings = {
            'backend_mappings': [{'key': 'value2'}],
            'disk_mappings': [{'key': 'value'}],
        }
        self.assertIsNone(
            provider_utils.check_changed_storage_mappings(
                [], old_storage_mappings, new_storage_mappings
            )
        )

    def test_check_changed_storage_mappings_no_change(self):
        volumes_info = [{'volume_id': '1'}]
        old_storage_mappings = {
            'backend_mappings': [{'key': 'value'}],
            'disk_mappings': [{'key': 'value'}],
        }
        new_storage_mappings = {
            'backend_mappings': [{'key': 'value'}],
            'disk_mappings': [{'key': 'value'}],
        }
        self.assertIsNone(
            provider_utils.check_changed_storage_mappings(
                volumes_info, old_storage_mappings, new_storage_mappings
            )
        )

    def test_check_changed_storage_mappings(self):
        old_storage_mappings = {
            'backend_mappings': [{'key': 'value'}],
            'disk_mappings': [{'key': 'value'}],
        }
        new_storage_mappings = {
            'backend_mappings': [{'key': 'value'}],
            'disk_mappings': [{'key': 'value'}],
        }
        self.assertIsNone(
            provider_utils.check_changed_storage_mappings(
                None, old_storage_mappings, new_storage_mappings
            )
        )

    def test_check_changed_storage_mappings_with_exception(self):
        volumes_info = [{'volume_id': '1'}]
        old_storage_mappings = {
            'backend_mappings': [{'key': 'value'}],
            'disk_mappings': [{'key': 'value'}],
        }
        new_storage_mappings = {
            'backend_mappings': [{'key': 'value2'}],
            'disk_mappings': [{'key': 'value2'}],
        }
        self.assertRaises(
            exception.CoriolisException,
            provider_utils.check_changed_storage_mappings,
            volumes_info,
            old_storage_mappings,
            new_storage_mappings,
        )

    def test_check_changed_storage_mappings_adding_new_mappings_allowed(self):
        volumes_info = [{'volume_id': '1'}]
        old_storage_mappings = {
            'backend_mappings': [{'source': 'backend1', 'destination': 'dest1'}],
            'disk_mappings': [{'disk_id': '1', 'destination': 'dest1'}],
        }
        new_storage_mappings = {
            'backend_mappings': [
                {'source': 'backend1', 'destination': 'dest1'},
                {'source': 'backend2', 'destination': 'dest2'},
            ],
            'disk_mappings': [
                {'disk_id': '1', 'destination': 'dest1'},
                {'disk_id': '2', 'destination': 'dest2'},
            ],
        }
        self.assertIsNone(
            provider_utils.check_changed_storage_mappings(
                volumes_info, old_storage_mappings, new_storage_mappings
            )
        )

    def test_check_changed_storage_mappings_backend_mappings_changed(self):
        volumes_info = [{'volume_id': '1'}]
        old_storage_mappings = {
            'backend_mappings': [{'source': 'backend1', 'destination': 'dest1'}],
            'disk_mappings': [{'disk_id': '1', 'destination': 'dest1'}],
        }
        new_storage_mappings = {
            'backend_mappings': [{'source': 'backend1', 'destination': 'dest2'}],
            'disk_mappings': [{'disk_id': '1', 'destination': 'dest1'}],
        }
        self.assertRaises(
            exception.CoriolisException,
            provider_utils.check_changed_storage_mappings,
            volumes_info,
            old_storage_mappings,
            new_storage_mappings,
        )

    def test_check_changed_storage_mappings_missing_backend_mapping(self):
        volumes_info = [{'volume_id': '1'}]
        old_storage_mappings = {
            'backend_mappings': [
                {'source': 'backend1', 'destination': 'dest1'},
                {'source': 'backend2', 'destination': 'dest2'},
            ],
            'disk_mappings': [{'disk_id': '1', 'destination': 'dest1'}],
        }
        new_storage_mappings = {
            'backend_mappings': [{'source': 'backend1', 'destination': 'dest1'}],
            'disk_mappings': [{'disk_id': '1', 'destination': 'dest1'}],
        }
        self.assertRaises(
            exception.CoriolisException,
            provider_utils.check_changed_storage_mappings,
            volumes_info,
            old_storage_mappings,
            new_storage_mappings,
        )

    def test_check_changed_storage_mappings_missing_disk_mapping(self):
        volumes_info = [{'volume_id': '1'}]
        old_storage_mappings = {
            'backend_mappings': [{'source': 'backend1', 'destination': 'dest1'}],
            'disk_mappings': [
                {'disk_id': '1', 'destination': 'dest1'},
                {'disk_id': '2', 'destination': 'dest2'},
            ],
        }
        new_storage_mappings = {
            'backend_mappings': [{'source': 'backend1', 'destination': 'dest1'}],
            'disk_mappings': [{'disk_id': '1', 'destination': 'dest1'}],
        }
        self.assertRaises(
            exception.CoriolisException,
            provider_utils.check_changed_storage_mappings,
            volumes_info,
            old_storage_mappings,
            new_storage_mappings,
        )

    def test_check_changed_storage_mappings_empty_old_mappings(self):
        volumes_info = [{'volume_id': '1'}]
        old_storage_mappings = {}
        new_storage_mappings = {
            'backend_mappings': [{'source': 'backend1', 'destination': 'dest1'}],
            'disk_mappings': [{'disk_id': '1', 'destination': 'dest1'}],
        }
        self.assertIsNone(
            provider_utils.check_changed_storage_mappings(
                volumes_info, old_storage_mappings, new_storage_mappings
            )
        )

    def test_check_changed_storage_mappings_empty_new_mappings(self):
        volumes_info = [{'volume_id': '1'}]
        old_storage_mappings = {
            'backend_mappings': [{'source': 'backend1', 'destination': 'dest1'}],
            'disk_mappings': [{'disk_id': '1', 'destination': 'dest1'}],
        }
        new_storage_mappings = {}
        self.assertRaises(
            exception.CoriolisException,
            provider_utils.check_changed_storage_mappings,
            volumes_info,
            old_storage_mappings,
            new_storage_mappings,
        )

    @mock.patch.object(utils, "connect_ssh")
    @mock.patch.object(utils, "exec_ssh_cmd")
    @mock.patch("time.sleep")
    def test_poll_instance_ssh(
        self,
        mock_sleep,
        mock_exec_ssh,
        mock_connect,
    ):
        mock_ssh_conn = mock.Mock()
        mock_connect.side_effect = [
            Exception,
            mock_ssh_conn,
            mock_ssh_conn,
        ]
        mock_exec_ssh.side_effect = [
            Exception,
            mock.sentinel.stdout,
        ]
        poll_interval = 5

        connection_info = self._get_mock_conn_info()
        provider_utils.poll_instance_until_reachable(
            connection_info=connection_info,
            protocol=constants.PROTOCOL_SSH,
            timeout=30,
            poll_interval=poll_interval,
        )

        mock_connect.assert_has_calls(
            [
                mock.call(
                    hostname=connection_info["ip"],
                    port=connection_info["port"],
                    username=connection_info["username"],
                    password=connection_info["password"],
                    pkey=connection_info["pkey"],
                )
            ]
            * 2
        )
        mock_exec_ssh.assert_has_calls([mock.call(mock_ssh_conn, "exit 0")] * 2)
        mock_ssh_conn.close.assert_has_calls([mock.call()] * 2)
        mock_sleep.assert_has_calls([mock.call(poll_interval)] * 2)

    @mock.patch.object(utils, "connect_ssh")
    @mock.patch.object(utils, "exec_ssh_cmd")
    @mock.patch("time.sleep")
    @mock.patch("time.time")
    def test_poll_instance_ssh_timeout(
        self,
        mock_time,
        mock_sleep,
        mock_exec_ssh,
        mock_connect,
    ):
        poll_interval = 5
        mock_time.side_effect = [x * 10 for x in range(20)]
        mock_connect.side_effect = IOError
        connection_info = self._get_mock_conn_info()
        self.assertRaises(
            exception.CoriolisException,
            provider_utils.poll_instance_until_reachable,
            connection_info=connection_info,
            protocol=constants.PROTOCOL_SSH,
            timeout=30,
            poll_interval=poll_interval,
        )

    @mock.patch("coriolis.wsman.WSManConnection", new_callable=mock.Mock)
    @mock.patch("time.sleep")
    def test_poll_instance_winrm(
        self,
        mock_sleep,
        mock_wsman,
    ):
        mock_conn = mock.Mock()
        mock_conn.exec_ps_command.side_effect = [Exception, mock.sentinel.stdout]
        mock_wsman.from_connection_info.return_value = mock_conn
        poll_interval = 5

        connection_info = self._get_mock_conn_info()
        provider_utils.poll_instance_until_reachable(
            connection_info=connection_info,
            protocol=constants.PROTOCOL_WINRM,
            timeout=30,
            poll_interval=poll_interval,
        )

        mock_wsman.from_connection_info.assert_has_calls(
            [mock.call(connection_info)] * 2, any_order=True
        )
        mock_conn.exec_ps_command.assert_has_calls([mock.call("whoami")] * 2)
        mock_sleep.assert_called_once_with(poll_interval)

    @mock.patch("coriolis.wsman.WSManConnection", new_callable=mock.Mock)
    @mock.patch("time.sleep")
    @mock.patch("time.time")
    def test_poll_instance_winrm_timeout(
        self,
        mock_time,
        mock_sleep,
        mock_wsman,
    ):
        poll_interval = 5
        mock_time.side_effect = [x * 10 for x in range(20)]
        mock_conn = mock.Mock()
        mock_conn.exec_ps_command.side_effect = IOError
        mock_wsman.from_connection_info.return_value = mock_conn
        connection_info = self._get_mock_conn_info()
        self.assertRaises(
            exception.CoriolisException,
            provider_utils.poll_instance_until_reachable,
            connection_info=connection_info,
            protocol=constants.PROTOCOL_WINRM,
            timeout=30,
            poll_interval=poll_interval,
        )


class NormalizeEnvironmentTestCase(test_base.CoriolisBaseTestCase):
    def test_drops_deprecated_keys(self):
        environment = {
            "zone": "zone1",
            "minion_network": "net-legacy",
            "network_map": {"source-net": "net-1"},
        }

        result = provider_utils.normalize_environment(
            environment, deprecated_keys=("minion_network", "use_config_drive")
        )

        self.assertEqual(
            {"zone": "zone1", "network_map": {"source-net": "net-1"}},
            result,
        )
        self.assertEqual("net-legacy", environment["minion_network"])

    def test_remaps_when_new_key_is_unset(self):
        environment = {"zone": "zone1", "service_offering": "legacy-offering"}

        result = provider_utils.normalize_environment(
            environment,
            key_renames=(("service_offering", "migr_worker_service_offering"),),
        )

        self.assertEqual(
            {
                "zone": "zone1",
                "migr_worker_service_offering": "legacy-offering",
            },
            result,
        )
        self.assertEqual("legacy-offering", environment["service_offering"])

    def test_keeps_current_value(self):
        environment = {
            "migr_worker_service_offering": "current-offering",
            "service_offering": "legacy-offering",
        }

        result = provider_utils.normalize_environment(
            environment,
            key_renames=(("service_offering", "migr_worker_service_offering"),),
        )

        self.assertEqual(
            {"migr_worker_service_offering": "current-offering"},
            result,
        )

    def test_first_old_key_with_a_value_wins(self):
        environment = {
            "linux_minion_service_offering": "",
            "windows_minion_service_offering": "windows-offering",
            "service_offering": "service-offering",
        }
        key_renames = (
            ("linux_minion_service_offering", "migr_worker_service_offering"),
            ("windows_minion_service_offering", "migr_worker_service_offering"),
            ("service_offering", "migr_worker_service_offering"),
        )

        result = provider_utils.normalize_environment(
            environment, key_renames=key_renames
        )

        self.assertEqual(
            {"migr_worker_service_offering": "windows-offering"},
            result,
        )

    def test_fills_empty_new_key(self):
        environment = {"migr_worker_service_offering": "", "service_offering": "legacy"}

        result = provider_utils.normalize_environment(
            environment,
            key_renames=(("service_offering", "migr_worker_service_offering"),),
        )

        self.assertEqual({"migr_worker_service_offering": "legacy"}, result)

    def test_empty_input(self):
        self.assertEqual({}, provider_utils.normalize_environment(None))
        self.assertEqual({}, provider_utils.normalize_environment("zone1"))
        self.assertEqual({}, provider_utils.normalize_environment({}))

    def test_deprecated_environment_message(self):
        environment = {
            "zone": "zone1",
            "service_offering": "legacy-offering",
            "use_config_drive": True,
        }

        message = provider_utils.deprecated_environment_message(
            environment,
            key_renames=(("service_offering", "migr_worker_service_offering"),),
            deprecated_keys=("use_config_drive", "minion_network"),
        )

        self.assertEqual(
            "These target environment options are deprecated.\n"
            "Use migr_worker_service_offering instead of service_offering.\n"
            "Remove use_config_drive.",
            message,
        )
        self.assertIsNone(
            provider_utils.deprecated_environment_message(
                {"zone": "zone1"},
                key_renames=(("service_offering", "migr_worker_service_offering"),),
                deprecated_keys=("use_config_drive",),
            )
        )
        self.assertIsNone(provider_utils.deprecated_environment_message(None))

    def test_deprecated_environment_message_groups_one_replacement(self):
        environment = {
            "minion_service_offering": "current",
            "service_offering": "older",
            "linux_template": "linux-id",
        }

        message = provider_utils.deprecated_environment_message(
            environment,
            key_renames=(
                ("minion_service_offering", "migr_worker_service_offering"),
                ("service_offering", "migr_worker_service_offering"),
                ("linux_template", "migr_worker_linux_template"),
            ),
        )

        self.assertEqual(
            "These target environment options are deprecated.\n"
            "Use migr_worker_service_offering instead of "
            "minion_service_offering and service_offering.\n"
            "Use migr_worker_linux_template instead of linux_template.",
            message,
        )


class _RenameProvider(base.BaseProvider):
    platform = "test"

    target_environment_key_renames = (("old", "new"),)
    deprecated_target_environment_keys = ("gone",)


class _PlainProvider(base.BaseProvider):
    platform = "plain"


class NormalizeTargetEnvironmentTestCase(test_base.CoriolisBaseTestCase):
    def test_provider_lists_remap_and_drop(self):
        provider = _RenameProvider()
        original = {"old": "value", "gone": True, "keep": 1}

        result = provider.normalize_target_environment(original)

        self.assertEqual({"new": "value", "keep": 1}, result)
        self.assertEqual({"old": "value", "gone": True, "keep": 1}, original)

    def test_default_lists_keep_every_key(self):
        provider = _PlainProvider()
        original = {"zone": "zone1", "use_config_drive": True}

        result = provider.normalize_target_environment(original)

        self.assertEqual(original, result)
        self.assertIsNot(original, result)
