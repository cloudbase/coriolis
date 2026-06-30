# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
from unittest import mock

from coriolis import constants, exception, utils
from coriolis.providers import provider_utils
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
