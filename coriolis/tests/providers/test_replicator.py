# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
import os
from unittest import mock

from oslo_utils import units

from coriolis import exception
from coriolis.providers import provider_utils
from coriolis.providers import replicator as replicator_module
from coriolis.tests import test_base
from coriolis.tests import testutils


class ClientTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Client class."""

    def setUp(self):
        super(ClientTestCase, self).setUp()
        self.ip = mock.sentinel.ip
        self.port = mock.sentinel.port
        self.ip_via_tunnel = mock.sentinel.ip_via_tunnel
        self.port_via_tunnel = mock.sentinel.port_via_tunnel
        self.credentials = {
            'username': mock.sentinel.username,
            'password': mock.sentinel.password,
        }
        self.chunk = {
            "offset": 0,
            "length": 11
        }
        self.device = mock.sentinel.device
        self.disk = mock.sentinel.disk
        self.ssh_conn_info = {
            "hostname": mock.sentinel.hostname,
            "port": mock.sentinel.ssh_port,
            "username": mock.sentinel.username,
            "pkey": mock.sentinel.pkey,
            "password": mock.sentinel.password,
        }
        self.event_handler = mock.MagicMock()
        self.mock_response = mock.MagicMock()

        with mock.patch.object(replicator_module.Client, '_get_session',
                               return_value=None), \
            mock.patch.object(replicator_module.Client, '_test_connection',
                              return_value=None):
            self.client = replicator_module.Client(
                self.ip, self.port, self.credentials, self.ssh_conn_info,
                self.event_handler)

        self.client._cli = mock.MagicMock()

    def test_repl_host(self):
        self.client._ip_via_tunnel = self.ip_via_tunnel

        result = self.client.repl_host
        self.assertEqual(result, self.ip_via_tunnel)

    def test_repl_host_no_ip_via_tunnel(self):
        result = self.client.repl_host
        self.assertEqual(result, self.ip)

    def test_repl_port(self):
        self.client._port_via_tunnel = self.port_via_tunnel

        result = self.client.repl_port
        self.assertEqual(result, self.port_via_tunnel)

    def test_repl_port_no_port_via_tunnel(self):
        result = self.client.repl_port
        self.assertEqual(result, self.port)

    def test_base_uri(self):
        self.client._ip_via_tunnel = self.ip_via_tunnel
        self.client._port_via_tunnel = self.port_via_tunnel
        expected_uri_via_tunnel = "https://%s:%s" % (self.ip_via_tunnel,
                                                     self.port_via_tunnel)
        result = self.client._base_uri
        self.assertEqual(result, expected_uri_via_tunnel)

    @mock.patch.object(replicator_module.Client, '_get_ssh_tunnel')
    def test__setup_tunnel_connection(self, mock_get_ssh_tunnel):
        mock_tunnel = mock.MagicMock()
        mock_tunnel.local_bind_address = (
            self.ip_via_tunnel, self.port_via_tunnel)
        mock_get_ssh_tunnel.return_value = mock_tunnel

        self.client._setup_tunnel_connection()

        mock_get_ssh_tunnel.assert_called_once()
        mock_tunnel.start.assert_called_once()

        self.assertEqual(self.client._ip_via_tunnel, self.ip_via_tunnel)
        self.assertEqual(self.client._port_via_tunnel, self.port_via_tunnel)

    @mock.patch.object(replicator_module.utils, 'wait_for_port_connectivity')
    def test__test_connection_no_tunnel_no_exception(self, mock_wait_for_port):
        self.client._use_tunnel = False

        self.client._test_connection()

        mock_wait_for_port.assert_called_once_with(
            self.ip, self.port, max_wait=30)

    @mock.patch.object(replicator_module.utils, 'wait_for_port_connectivity')
    @mock.patch.object(replicator_module.Client, '_setup_tunnel_connection')
    def test__test_connection_with_tunnel_no_exception(
            self, mock_setup_tunnel, mock_wait_for_port):
        self.client._use_tunnel = True

        self.client._test_connection()

        mock_setup_tunnel.assert_called_once()
        mock_wait_for_port.assert_called_once_with(
            self.ip, self.port, max_wait=30)

    @mock.patch.object(replicator_module.utils, 'wait_for_port_connectivity')
    @mock.patch.object(replicator_module.Client, '_setup_tunnel_connection')
    def test_test_connection_no_tunnel_with_exception(
            self, mock_setup_tunnel, mock_wait_for_port):
        self.client._use_tunnel = False
        mock_wait_for_port.side_effect = [BaseException(), None]

        self.client._test_connection()

        mock_setup_tunnel.assert_called_once()

    @mock.patch.object(replicator_module.utils, 'wait_for_port_connectivity')
    @mock.patch.object(replicator_module.Client, '_setup_tunnel_connection')
    def test_test_connection_with_tunnel_with_exception(
            self, mock_setup_tunnel, mock_wait_for_port):
        mock_tunnel = mock.MagicMock()
        self.client._tunnel = mock_tunnel
        self.client._use_tunnel = True
        mock_wait_for_port.side_effect = BaseException()

        with self.assertLogs('coriolis.providers.replicator',
                             logging.WARNING):
            self.assertRaises(BaseException, self.client._test_connection)

        mock_setup_tunnel.assert_called_once()
        mock_tunnel.stop.assert_called_once()

    @mock.patch.object(replicator_module, 'SSHTunnelForwarder')
    def test__get_ssh_tunnel(self, mock_SSHTunnelForwarder):
        result = self.client._get_ssh_tunnel()

        mock_SSHTunnelForwarder.assert_called_once_with((
            self.ssh_conn_info["hostname"], self.ssh_conn_info["port"]),
            ssh_username=self.ssh_conn_info["username"],
            ssh_pkey=self.ssh_conn_info["pkey"],
            ssh_password=self.ssh_conn_info["password"],
            remote_bind_address=("127.0.0.1", self.port),
            local_bind_address=("127.0.0.1", 0)
        )

        self.assertEqual(result, mock_SSHTunnelForwarder.return_value)

    def test__get_ssh_tunnel_no_credentials(self):
        self.client._ssh_conn_info = {
            "hostname": self.ssh_conn_info["hostname"],
            "port": self.ssh_conn_info["port"],
            "username": self.ssh_conn_info["username"],
        }
        self.assertRaises(exception.CoriolisException,
                          self.client._get_ssh_tunnel)

    def test_raw_disk_uri(self):
        result = self.client.raw_disk_uri(self.device)
        expected_uri = "%s/device/%s" % (self.client._base_uri, self.device)

        self.assertEqual(result, expected_uri)

    @mock.patch.object(provider_utils, 'ProviderSession')
    def test__get_session(self, mock_Session):
        self.client._creds = {
            "client_cert": mock.sentinel.client_cert,
            "client_key": mock.sentinel.client_key,
            "ca_cert": mock.sentinel.ca_cert,
        }
        result = self.client._get_session()

        mock_Session.assert_called_once()
        self.assertEqual(result, mock_Session.return_value)
        self.assertEqual(result.cert, (
            self.client._creds["client_cert"],
            self.client._creds["client_key"]))
        self.assertEqual(result.verify, self.client._creds["ca_cert"])

    def test_get_status(self):
        self.client._cli.get.return_value = self.mock_response

        original_get_status = testutils.get_wrapped_function(
            self.client.get_status)

        result = original_get_status(self.client, device=self.device,
                                     brief=True)

        self.assertEqual(result, self.mock_response.json.return_value)
        self.client._cli.get.assert_called_once_with(
            "https://%s:%s/api/v1/dev/%s/" % (self.ip, self.port, self.device),
            params={"brief": True},
            timeout=replicator_module.CONF.replicator.default_requests_timeout,
        )
        self.mock_response.raise_for_status.assert_called_once()

    def test_get_status_no_device(self):
        self.client._cli.get.return_value = self.mock_response

        original_get_status = testutils.get_wrapped_function(
            self.client.get_status)

        result = original_get_status(self.client, device=None, brief=True)

        self.assertEqual(result, self.mock_response.json.return_value)
        self.client._cli.get.assert_called_once_with(
            "https://%s:%s/api/v1/dev" % (self.ip, self.port),
            params={"brief": True},
            timeout=replicator_module.CONF.replicator.default_requests_timeout,
        )

    def test_get_chunks(self):
        self.client._cli.get.return_value = self.mock_response

        original_get_chunks = testutils.get_wrapped_function(
            self.client.get_chunks)

        result = original_get_chunks(self.client, self.device,
                                     skip_zeros=False)

        self.assertEqual(result, self.mock_response.json.return_value)
        self.client._cli.get.assert_called_once_with(
            "https://%s:%s/api/v1/dev/%s/chunks/" % (
                self.ip, self.port, self.device),
            params={"skipZeros": False},
            timeout=replicator_module.CONF.replicator.default_requests_timeout,
        )
        self.mock_response.raise_for_status.assert_called_once()

    def test_get_changes(self):
        self.client._cli.get.return_value = self.mock_response

        original_get_changes = testutils.get_wrapped_function(
            self.client.get_changes)

        result = original_get_changes(self.client, self.device)

        self.assertEqual(result, self.mock_response.json.return_value)
        self.client._cli.get.assert_called_once_with(
            "https://%s:%s/api/v1/dev/%s/chunks/changes/" % (
                self.ip, self.port, self.device),
            timeout=replicator_module.CONF.replicator.default_requests_timeout,
        )
        self.mock_response.raise_for_status.assert_called_once()

    def test_get_disk_size(self):
        self.client._cli.head.return_value = self.mock_response

        original_get_disk_size = testutils.get_wrapped_function(
            self.client.get_disk_size)

        result = original_get_disk_size(self.client, self.device)

        self.assertEqual(
            result, int(self.mock_response.headers["Content-Length"]))
        self.client._cli.head.assert_called_once_with(
            "https://%s:%s/device/%s" % (self.ip, self.port, self.device),
            timeout=replicator_module.CONF.replicator.default_requests_timeout,
        )
        self.mock_response.raise_for_status.assert_called_once()

    def test_download_chunk(self):
        self.client._use_compression = True
        self.client._cli.get.return_value = self.mock_response

        original_download_chunk = testutils.get_wrapped_function(
            self.client.download_chunk)

        result = original_download_chunk(self.client, self.disk, self.chunk)

        self.assertEqual(result, self.mock_response.content)
        self.client._cli.get.assert_called_once_with(
            "https://%s:%s/device/%s" % (self.ip, self.port, self.disk),
            headers={"Range": "bytes=0-10"},
            timeout=replicator_module.CONF.replicator.default_requests_timeout,
        )
        self.mock_response.raise_for_status.assert_called_once()

    def test_download_chunk_no_compression(self):
        self.client._use_compression = False
        self.client._cli.get.return_value = self.mock_response

        self.chunk["offset"] = 100
        self.chunk["length"] = 200

        original_download_chunk = testutils.get_wrapped_function(
            self.client.download_chunk)

        result = original_download_chunk(self.client, self.disk, self.chunk)

        self.assertEqual(result, self.mock_response.content)
        self.client._cli.get.assert_called_once_with(
            "https://%s:%s/device/%s" % (self.ip, self.port, self.disk),
            headers={"Range": "bytes=%s-%s" % (
                self.chunk["offset"],
                self.chunk["offset"] + self.chunk["length"] - 1),
                "Accept-encoding": "identity"},
            timeout=replicator_module.CONF.replicator.default_requests_timeout,
        )
        self.mock_response.raise_for_status.assert_called_once()


class ReplicatorTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Replicator class."""

    def setUp(self):
        super(ReplicatorTestCase, self).setUp()
        self.conn_info = {
            'ip': mock.sentinel.ip,
            'username': mock.sentinel.username,
            'password': mock.sentinel.password,
            'port': mock.sentinel.port,
        }
        self.volumes_info = [{
            'disk_id': 'test_disk',
            'other_key': mock.sentinel.other_key,
        }]
        self.replica_state = 'replica_state'
        self._ssh = mock.MagicMock()
        self._credentials = mock.MagicMock()
        self._attachf = mock.MagicMock()
        self.source_volumes_info = mock.MagicMock()
        self.backup_writer = mock.MagicMock()
        self.event_manager = mock.MagicMock()
        self.disk = 'test_disk'
        self.path = '/path/to/disk'
        self.size = 1000

        with mock.patch.object(replicator_module, 'CONF',
                               return_value=mock.MagicMock()), \
             mock.patch.object(replicator_module.Replicator, '_setup_ssh',
                               return_value=None):
            self.replicator = replicator_module.Replicator(
                self.conn_info,
                self.event_manager,
                self.volumes_info,
                self.replica_state
            )
        self.replicator._cli = mock.MagicMock()
        self.replicator._event_manager = mock.MagicMock()

    @mock.patch('shutil.rmtree')
    def test__del__(self, mock_rmtree):
        mock_rmtree.return_value = None
        self.replicator._cert_dir = '/tmp/cert_dir'

        del self.replicator

        mock_rmtree.assert_called_once_with('/tmp/cert_dir')

    @mock.patch.object(
        replicator_module.Replicator, '_parse_source_ssh_conn_info'
    )
    @mock.patch.object(
        replicator_module.Replicator, '_parse_replicator_conn_info'
    )
    @mock.patch.object(replicator_module, 'Client')
    def test__init_replicator_client(
            self, mock_Client, mock_parse_replicator_conn_info,
            mock_parse_source_ssh_conn_info):
        mock_parse_replicator_conn_info.return_value = self.conn_info
        mock_parse_source_ssh_conn_info.return_value = self.conn_info
        mock_Client.return_value = None

        self.replicator._init_replicator_client(self.conn_info)

        mock_parse_source_ssh_conn_info.assert_called_once_with(
            self.replicator._conn_info)
        mock_parse_replicator_conn_info.assert_called_once_with(
            self.replicator._conn_info)
        mock_Client.assert_called_once_with(
            self.conn_info['ip'], self.conn_info['port'],
            self.conn_info, self.conn_info, self.replicator._event_manager,
            use_compression=self.replicator._use_compression,
            use_tunnel=self.replicator._use_tunnel)

    @mock.patch.object(replicator_module.Replicator, '_get_ssh_client')
    @mock.patch.object(
        replicator_module.Replicator, '_parse_source_ssh_conn_info'
    )
    def test__setup_ssh(self, mock_parse_source_ssh_conn_info,
                        mock_get_ssh_client):
        mock_parse_source_ssh_conn_info.return_value = self.conn_info

        result = self.replicator._setup_ssh()

        mock_parse_source_ssh_conn_info.assert_called_once_with(
            self.replicator._conn_info)
        mock_get_ssh_client.assert_called_once_with(self.conn_info)

        self.assertEqual(result, mock_get_ssh_client.return_value)

    @mock.patch.object(replicator_module.Replicator, '_setup_ssh')
    def test__reconnect_ssh(self, mock_setup_ssh):
        self.replicator._ssh = self._ssh
        self.replicator._cert_dir = mock.MagicMock()

        result = self.replicator._reconnect_ssh()

        self._ssh.close.assert_called_once()
        mock_setup_ssh.assert_called_once()
        self.assertEqual(result, mock_setup_ssh.return_value)

    @mock.patch.object(replicator_module.Replicator, '_setup_replicator')
    @mock.patch.object(replicator_module.Replicator, '_init_replicator_client')
    def test_init_replicator(self, mock_init_replicator_client,
                             mock_setup_replicator):

        self.replicator._ssh = self._ssh
        mock_setup_replicator.return_value = self._credentials

        self.replicator.init_replicator()

        mock_setup_replicator.assert_called_once_with(self._ssh)
        mock_init_replicator_client.assert_called_once_with(self._credentials)

    @mock.patch.object(replicator_module.Client, 'get_status')
    def test_get_current_disks_status(self, mock_get_status):
        self.replicator._cli = mock_get_status
        result = self.replicator.get_current_disks_status()

        self.assertEqual(result, mock_get_status.get_status.return_value)

    @mock.patch.object(replicator_module.Client, 'get_status')
    def test_attach_new_disk(self, mock_get_status):
        self.replicator._cli = mock_get_status

        mock_return_value_before = [
            {'device-path': '/dev/xvdf', 'disk_id': 'existing_disk'}]
        mock_return_value_after = [
            {'device-path': '/dev/xvdf', 'disk_id': 'existing_disk'},
            {'device-path': '/dev/xvdg', 'disk_id': 'test_disk'}]

        mock_get_status.get_status.side_effect = [
            mock_return_value_before, mock_return_value_after]

        result = self.replicator.attach_new_disk(self.disk, self._attachf)

        self._attachf.assert_called_once()

        expected_return_value = {
            'disk_id': 'test_disk',
            'disk_path': '/dev/xvdg',
            'other_key': self.volumes_info[0]['other_key']
        }
        self.assertEqual(result, expected_return_value)

    def test_attach_new_disk_no_matching_volumes(self):
        self.replicator._volumes_info = [
            {'disk_id': 'other_disk', 'other_key': 'other_value'}]

        self.assertRaises(exception.CoriolisException,
                          self.replicator.attach_new_disk, self.disk,
                          self._attachf)

    def test_attach_new_disk_multiple_matching_volumes(self):
        self.replicator._volumes_info = [
            {'disk_id': 'test_disk', 'other_key': 'other_value'},
            {'disk_id': 'test_disk', 'other_key': 'other_value'}]

        self.assertRaises(exception.CoriolisException,
                          self.replicator.attach_new_disk, self.disk,
                          self._attachf)

    @mock.patch.object(replicator_module.Client, 'get_status')
    def test_attach_new_disk_multiple_new_device_paths(self, mock_get_status):
        self.replicator._cli = mock_get_status

        mock_return_value_before = [
            {'device-path': '/dev/xvdf', 'disk_id': 'test_disk'}]
        mock_return_value_after = [
            {'device-path': '/dev/xvdf', 'disk_id': 'test_disk'},
            {'device-path': '/dev/xvdf2', 'disk_id': 'new_disk'},
            {'device-path': '/dev/xvdf3', 'disk_id': 'new_disk'}]

        mock_get_status.get_status.side_effect = [
            mock_return_value_before, mock_return_value_after]

        self.assertRaises(exception.CoriolisException,
                          self.replicator.attach_new_disk, self.disk,
                          self._attachf)

    @mock.patch.object(replicator_module.Client, 'get_status')
    def test_attach_new_disk_missing_device_paths(self, mock_get_status):
        self.replicator._cli = mock_get_status

        mock_return_value_before = [
            {'device-path': '/dev/xvdf', 'disk_id': 'test_disk'},
            {'device-path': '/dev/xvdg', 'disk_id': 'old_disk'}]
        mock_return_value_after = [
            {'device-path': '/dev/xvdf', 'disk_id': 'test_disk'},
            {'device-path': '/dev/xvdh', 'disk_id': 'new_disk'}]

        mock_get_status.get_status.side_effect = [
            mock_return_value_before, mock_return_value_after]

        with self.assertLogs('coriolis.providers.replicator',
                             logging.WARN):
            self.replicator.attach_new_disk(self.disk, self._attachf)

    @mock.patch.object(replicator_module.Client, 'get_status')
    @mock.patch('time.sleep')
    def test_attach_new_disk_no_new_device_paths(self, _, mock_get_status):
        self.replicator._cli = mock_get_status

        mock_return_value_before = [{'device-path': 'path1'}]
        self.replicator._volumes_info = [
            {'disk_id': 'disk1', 'disk_path': 'path1'}]

        mock_get_status.get_status.side_effect = [
            mock_return_value_before, mock_return_value_before]

        def attachf():
            pass

        self.assertRaises(exception.CoriolisException,
                          self.replicator.attach_new_disk,
                          'disk1', attachf, retry_count=1)

    @mock.patch.object(replicator_module.Client, 'get_status')
    @mock.patch('time.sleep')
    def test_attach_new_disk_new_device_paths_after_retry(self, _,
                                                          mock_get_status):
        self.replicator._cli = mock_get_status

        mock_return_value_before = [{'device-path': 'path1'}]
        mock_return_value_after = [
            {'device-path': 'path1'}, {'device-path': 'path2'}]

        self.replicator._volumes_info = [
            {'disk_id': 'disk1', 'disk_path': 'path1'}]

        mock_get_status.get_status.side_effect = [
            mock_return_value_before, mock_return_value_before,
            mock_return_value_after]

        def attachf():
            pass

        self.replicator.attach_new_disk('disk1', attachf, retry_count=2,
                                        retry_period=0)

    @mock.patch.object(replicator_module.Client, 'get_status')
    def test_wait_for_chunks(self, mock_get_status):
        self.replicator._cli = mock_get_status
        mock_get_status.get_status.return_value = [
            {
                "device-path": mock.sentinel.disk,
                "size": 1024 * units.Mi,
                "checksum-status": {"percentage": 100}
            },
            {
                "device-path": mock.sentinel.disk,
                "size": 1024 * units.Mi,
                "checksum-status": {"percentage": 100}
            }
        ]

        self.replicator._event_manager.add_percentage_step.\
            return_value = mock.sentinel.perc_step

        self.replicator.wait_for_chunks()

        mock_get_status.get_status.assert_called_once()
        self.replicator._event_manager.add_percentage_step.assert_called_once()
        self.replicator._event_manager.set_percentage_step.assert_has_calls([
            mock.call(mock.sentinel.perc_step, 100),
            mock.call(mock.sentinel.perc_step, 100)
        ])

    def test_wait_for_chunks_not_initialized(self):
        self.replicator._cli = None
        self.assertRaises(exception.CoriolisException,
                          self.replicator.wait_for_chunks)

    @mock.patch.object(replicator_module.utils, 'start_service')
    def test_start(self, mock_start_service):
        self.replicator.start()
        mock_start_service.assert_called_once_with(
            self.replicator._ssh, replicator_module.REPLICATOR_SVC_NAME)

    @mock.patch.object(replicator_module.utils, 'stop_service')
    def test_stop(self, mock_stop_service):
        self.replicator.stop()
        mock_stop_service.assert_called_once_with(
            self.replicator._ssh, replicator_module.REPLICATOR_SVC_NAME)

    @mock.patch.object(replicator_module.utils, 'restart_service')
    def test_restart(self, mock_stop_service):
        self.replicator.restart()
        mock_stop_service.assert_called_once_with(
            self.replicator._ssh, replicator_module.REPLICATOR_SVC_NAME)

    @mock.patch('builtins.open')
    @mock.patch.object(replicator_module.json, 'dump')
    @mock.patch.object(replicator_module.tempfile, 'mkstemp')
    @mock.patch.object(replicator_module.Replicator, '_copy_file')
    @mock.patch.object(replicator_module.Replicator, 'restart')
    @mock.patch.object(replicator_module.Client, '_test_connection')
    def test_update_state(
            self, mock_test_connection, mock_restart, mock_copy_file,
            mock_mkstemp, mock_dump, mock_open):
        mock_mkstemp.return_value = (None, mock.sentinel.state)
        mock_dump.return_value = None

        self.replicator.update_state(mock.sentinel.state, restart=False)

        mock_mkstemp.assert_called_once()
        mock_open.assert_called_once_with(mock.sentinel.state, 'w')
        mock_copy_file.assert_called_once_with(
            self.replicator._ssh, mock_mkstemp.return_value[1],
            replicator_module.REPLICATOR_STATE)
        mock_restart.assert_not_called()
        mock_test_connection.assert_not_called()

    @mock.patch('builtins.open')
    @mock.patch.object(replicator_module.json, 'dump')
    @mock.patch.object(replicator_module.tempfile, 'mkstemp')
    @mock.patch.object(replicator_module.Replicator, '_copy_file')
    @mock.patch.object(replicator_module.Replicator, 'restart')
    @mock.patch.object(replicator_module.Client, '_test_connection')
    def test_update_state_with_restart(
            self, mock_test_connection, mock_restart, mock_copy_file,
            mock_mkstemp, mock_dump, mock_open):
        mock_mkstemp.return_value = (None, mock.sentinel.state)
        mock_dump.return_value = None

        self.replicator._cli._test_connection = mock_test_connection

        self.replicator.update_state(mock.sentinel.state, restart=True)

        mock_mkstemp.assert_called_once()
        mock_open.assert_called_once_with(mock.sentinel.state, 'w')
        mock_copy_file.assert_called_once_with(
            self.replicator._ssh, mock_mkstemp.return_value[1],
            replicator_module.REPLICATOR_STATE)
        mock_restart.assert_called_once()
        mock_test_connection.assert_called_once()

    @mock.patch.object(replicator_module.paramiko, 'SSHClient')
    def test__get_ssh_client(self, mock_ssh_client):
        self._ssh = mock_ssh_client.return_value

        original_get_ssh_client = testutils.get_wrapped_function(
            self.replicator._get_ssh_client)

        result = original_get_ssh_client(self.replicator, self.conn_info)

        mock_ssh_client.assert_called_once()
        self._ssh.set_missing_host_key_policy.assert_called_once_with(
            mock.ANY)
        self._ssh.connect.assert_called_once_with(**self.conn_info)

        self.assertEqual(result, mock_ssh_client.return_value)

    @mock.patch.object(replicator_module.paramiko, 'SSHClient')
    def test__get_ssh_client_exception(self, mock_ssh_client):
        self._ssh = mock_ssh_client.return_value
        self._ssh.connect.side_effect = (
            replicator_module.paramiko.ssh_exception.SSHException())

        original_get_ssh_client = testutils.get_wrapped_function(
            self.replicator._get_ssh_client)

        self.assertRaises(exception.CoriolisException, original_get_ssh_client,
                          self.replicator, self.conn_info)

    def test__parse_source_ssh_conn_info(self):
        expected_arg = {
            "hostname": self.conn_info["ip"],
            "port": self.conn_info["port"],
            "username": self.conn_info["username"],
            "password": self.conn_info["password"],
            "pkey": None,
            "banner_timeout": (
                replicator_module.CONF.replicator.default_requests_timeout),
        }

        result = self.replicator._parse_source_ssh_conn_info(self.conn_info)

        self.assertEqual(result, expected_arg)

    def test__parse_source_ssh_conn_info_missing_required_field(self):
        # Remove the Username from the connection info to test the missing
        # required.
        self.conn_info.pop("username")

        self.assertRaises(exception.CoriolisException,
                          self.replicator._parse_source_ssh_conn_info,
                          self.conn_info)

    def test__parse_source_ssh_conn_info_missing_password(self):
        self.conn_info.pop("password")
        self.assertRaises(exception.CoriolisException,
                          self.replicator._parse_source_ssh_conn_info,
                          self.conn_info)

    @mock.patch.object(replicator_module.utils, 'deserialize_key')
    @mock.patch.object(replicator_module, 'CONF')
    def test__parse_source_ssh_conn_info_with_string_pkey(
            self, mock_CONF, mock_deserialize_key):
        self.conn_info["pkey"] = "test_pkey"

        self.replicator._parse_source_ssh_conn_info(self.conn_info)
        mock_deserialize_key.assert_called_once_with(
            "test_pkey",
            mock_CONF.serialization.temp_keypair_password)

    @mock.patch.object(replicator_module.tempfile, 'mkstemp')
    @mock.patch('builtins.open')
    def test__get_replicator_state_file(self, mock_open, mock_mkstemp):
        mock_mkstemp.return_value = (None, mock.sentinel.state)

        result = self.replicator._get_replicator_state_file()

        mock_mkstemp.assert_called_once()
        mock_open.assert_called_once_with(mock.sentinel.state, 'w')

        self.assertEqual(result, mock_mkstemp.return_value[1])

    def test__parse_replicator_conn_info(self):
        result = self.replicator._parse_replicator_conn_info(self.conn_info)

        expected_result = {
            "ip": self.conn_info["ip"],
            "port": replicator_module.CONF.replicator.port,
        }

        self.assertEqual(result, expected_result)

    @mock.patch.object(replicator_module.tempfile, 'mktemp')
    @mock.patch('paramiko.SFTPClient.from_transport')
    @mock.patch.object(replicator_module.utils, 'exec_ssh_cmd')
    def test__copy_file(self, mock_exec_ssh_cmd, mock_from_transport,
                        mock_mktemp):
        mock_sftp = mock.MagicMock()
        mock_from_transport.return_value = mock_sftp

        original_copy_file = testutils.get_wrapped_function(
            self.replicator._copy_file)

        original_copy_file(self.replicator, self._ssh, mock.sentinel.localPath,
                           mock.sentinel.remotePath)

        mock_mktemp.assert_called_once_with(dir='/tmp')
        mock_from_transport.assert_called_once_with(
            self._ssh.get_transport())
        mock_sftp.put.assert_called_once_with(
            mock.sentinel.localPath, mock_mktemp.return_value)
        mock_exec_ssh_cmd.assert_called_once_with(
            self._ssh, "sudo mv %s %s" % (
                mock_mktemp.return_value, mock.sentinel.remotePath),
            get_pty=True)
        mock_sftp.close.assert_called_once()

    @mock.patch.object(os.path, 'join')
    @mock.patch.object(replicator_module.utils, 'get_resources_bin_dir')
    @mock.patch.object(replicator_module.Replicator, '_copy_file')
    @mock.patch.object(replicator_module.utils, 'exec_ssh_cmd')
    def test__copy_replicator_cmd(
            self, mock_exec_ssh_cmd, mock_copy_file,
            mock_get_resources_bin_dir, mock_join):
        original_copy_replicator_cmd = testutils.get_wrapped_function(
            self.replicator._copy_replicator_cmd)

        original_copy_replicator_cmd(self.replicator, self._ssh)

        mock_get_resources_bin_dir.assert_called_once()
        mock_join.assert_called_once_with(
            mock_get_resources_bin_dir.return_value, 'replicator')
        mock_copy_file.assert_called_once_with(
            self._ssh, mock_join.return_value,
            replicator_module.REPLICATOR_PATH)
        mock_exec_ssh_cmd.assert_called_once_with(
            self._ssh, "sudo chmod +x %s" %
            replicator_module.REPLICATOR_PATH,
            get_pty=True)

    @mock.patch.object(replicator_module.utils, 'exec_ssh_cmd')
    def test_setup_replicator_group(self, mock_exec_ssh_cmd):
        mock_exec_ssh_cmd.return_value = '1'

        result = self.replicator._setup_replicator_group(self._ssh)

        mock_exec_ssh_cmd.assert_called_once_with(
            self._ssh,
            "getent group %s > /dev/null && echo 1 || echo 0" % (
                replicator_module.REPLICATOR_GROUP_NAME))

        self.assertEqual(result, True)

    @mock.patch.object(replicator_module.utils, 'exec_ssh_cmd')
    def test__setup_replicator_no_group(self, mock_exec_ssh_cmd):
        group_name = mock.sentinel.group_name
        mock_exec_ssh_cmd.return_value = 0

        result = self.replicator._setup_replicator_group(
            self._ssh, group_name=group_name)

        mock_exec_ssh_cmd.assert_has_calls([
            mock.call(self._ssh,
                      "getent group %s > /dev/null && "
                      "echo 1 || echo 0" %
                      replicator_module.REPLICATOR_GROUP_NAME),
            mock.call(self._ssh,
                      "sudo groupadd %s" % group_name, get_pty=True),
            mock.call(self._ssh, "sudo usermod -aG %s %s" % (
                replicator_module.REPLICATOR_GROUP_NAME,
                self.conn_info["username"]), get_pty=True)])

        self.assertFalse(result)

    @mock.patch.object(replicator_module.utils, 'exec_ssh_cmd')
    def test__setup_replicator_user(self, mock_exec_ssh_cmd):
        mock_exec_ssh_cmd.return_value = '1'

        self.replicator._setup_replicator_user(self._ssh)

        mock_exec_ssh_cmd.assert_called_once_with(
            self._ssh,
            "getent passwd %s > /dev/null && echo 1 || echo 0" %
            replicator_module.REPLICATOR_USERNAME)

    @mock.patch.object(replicator_module.utils, 'exec_ssh_cmd')
    def test__setup_replicator_user_no_user(self, mock_exec_ssh_cmd):
        mock_exec_ssh_cmd.return_value = 0

        self.replicator._setup_replicator_user(self._ssh)

        mock_exec_ssh_cmd.assert_has_calls([
            mock.call(self._ssh,
                      "getent passwd %s > /dev/null && "
                      "echo 1 || echo 0" %
                      replicator_module.REPLICATOR_USERNAME),
            mock.call(self._ssh,
                      "sudo useradd -m -s /bin/bash -g %s %s" %
                      (replicator_module.REPLICATOR_USERNAME,
                       replicator_module.REPLICATOR_GROUP_NAME),
                      get_pty=True),
            mock.call(self._ssh,
                      "sudo usermod -aG disk %s" %
                      replicator_module.REPLICATOR_USERNAME, get_pty=True)])

    @mock.patch.object(replicator_module.utils, 'create_service')
    def test__exec_replicator_cmd(self, mock_create_service):
        certs = {
            "ca_crt": mock.sentinel.ca_crt,
            "srv_crt": mock.sentinel.srv_crt,
            "srv_key": mock.sentinel.srv_key,
        }
        original_exec_replicator = testutils.get_wrapped_function(
            self.replicator._exec_replicator)

        original_exec_replicator(
            self.replicator, self._ssh, self.conn_info['port'], certs,
            mock.sentinel.state)

        mock_create_service.assert_called_once_with(
            self._ssh, mock.ANY,
            replicator_module.REPLICATOR_SVC_NAME,
            run_as=replicator_module.REPLICATOR_USERNAME)

    @mock.patch.object(replicator_module.utils, 'read_ssh_file')
    def test__fetch_remote_file(self, mock_read_ssh_file):
        with mock.patch('builtins.open', mock.mock_open()) as data:
            self.replicator._fetch_remote_file(
                self._ssh, mock.sentinel.remote_file, mock.sentinel.local_file)
            data.assert_called_once_with(mock.sentinel.local_file, 'wb')
            data.return_value.write.assert_called_once_with(
                mock_read_ssh_file.return_value)

    @mock.patch.object(os.path, 'isfile')
    @mock.patch.object(replicator_module.utils, 'test_ssh_path')
    def test__setup_certificates(self, mock_test_ssh_path, mock_isfile):
        mock_test_ssh_path.return_value = True
        mock_isfile.return_value = True

        original_setup_certificates = testutils.get_wrapped_function(
            self.replicator._setup_certificates)

        result = original_setup_certificates(
            self.replicator, self._ssh, self.conn_info)

        expected_result = {
            "local":
            {"ca_cert": self.replicator._cert_dir + "/ca-cert.pem",
             "client_cert": self.replicator._cert_dir + "/client-cert.pem",
             "client_key": self.replicator._cert_dir + "/client-key.pem"},
            "remote":
            {"ca_crt": replicator_module.REPLICATOR_DIR + "/ca-cert.pem",
             "srv_crt": replicator_module.REPLICATOR_DIR + "/srv-cert.pem",
             "srv_key": replicator_module.REPLICATOR_DIR + "/srv-key.pem"}}

        self.assertEqual(result, expected_result)

    @mock.patch.object(replicator_module.Replicator, '_fetch_remote_file')
    @mock.patch.object(replicator_module.utils, 'test_ssh_path')
    @mock.patch.object(replicator_module.utils, 'exec_ssh_cmd')
    def test_setup_certificates_no_files_exist(
            self, mock_exec_ssh_cmd, mock_test_ssh_path,
            mock_fetch_remote_file):
        mock_test_ssh_path.return_value = False

        original_setup_certificates = testutils.get_wrapped_function(
            self.replicator._setup_certificates)

        original_setup_certificates(
            self.replicator, self._ssh, self.conn_info)

        expected_calls = [
            mock.call(self._ssh, "sudo mkdir -p %s" %
                      replicator_module.REPLICATOR_DIR, get_pty=True),
            mock.call(self._ssh, "sudo %s gen-certs -output-dir"
                      % replicator_module.REPLICATOR_PATH +
                      " %s -certificate-hosts 127.0.0.1,%s" %
                      (replicator_module.REPLICATOR_DIR, self.conn_info['ip']),
                      get_pty=True),
            mock.call(self._ssh, "sudo chown -R %s:%s %s" %
                      (replicator_module.REPLICATOR_USERNAME,
                       replicator_module.REPLICATOR_GROUP_NAME,
                       replicator_module.REPLICATOR_DIR), get_pty=True),
            mock.call(self._ssh, "sudo chmod -R g+r %s" %
                      replicator_module.REPLICATOR_DIR, get_pty=True)]

        mock_exec_ssh_cmd.assert_has_calls(expected_calls)
        self.assertEqual(mock_fetch_remote_file.call_count, 3)

    @mock.patch.object(
        replicator_module.Replicator, '_get_replicator_state_file'
    )
    @mock.patch.object(replicator_module.Replicator, '_copy_file')
    @mock.patch.object(replicator_module.utils, 'exec_ssh_cmd')
    @mock.patch.object(replicator_module.os, 'remove')
    @mock.patch.object(
        replicator_module.Replicator, '_parse_replicator_conn_info'
    )
    @mock.patch.object(replicator_module.Replicator, '_copy_replicator_cmd')
    @mock.patch.object(replicator_module.Replicator, '_setup_replicator_group')
    @mock.patch.object(replicator_module.Replicator, '_reconnect_ssh')
    @mock.patch.object(replicator_module.Replicator, '_setup_replicator_user')
    @mock.patch.object(replicator_module.Replicator, '_setup_certificates')
    @mock.patch.object(replicator_module.Replicator, '_exec_replicator')
    @mock.patch.object(replicator_module.Replicator, 'start')
    def test__setup_replicator(
            self, mock_start, mock_exec_replicator, mock_setup_certificates,
            mock_setup_replicator_user, mock_reconnect_ssh,
            mock_setup_replicator_group, mock_copy_replicator_cmd,
            mock_parse_replicator_conn_info, mock_os_remove,
            mock_exec_ssh_cmd, mock_copy_file,
            mock_get_replicator_state_file):
        mock_setup_replicator_group.return_value = True

        original_setup_replicator = testutils.get_wrapped_function(
            self.replicator._setup_replicator)

        result = original_setup_replicator(self.replicator, self._ssh)

        mock_get_replicator_state_file.assert_called_once()
        mock_copy_file.assert_called_once_with(
            self._ssh,
            mock_get_replicator_state_file.return_value,
            replicator_module.REPLICATOR_STATE)
        mock_exec_ssh_cmd.assert_has_calls([
            mock.call(
                self._ssh,
                "sudo chmod 755 %s" % replicator_module.REPLICATOR_STATE,
                get_pty=True
            ),
            mock.call(
                self._ssh,
                "sudo chcon -t bin_t /usr/bin/replicator",
                get_pty=True
            ),
        ])
        mock_os_remove.assert_called_once_with(
            mock_get_replicator_state_file.return_value)
        mock_parse_replicator_conn_info.assert_called_once_with(
            self.replicator._conn_info)
        mock_copy_replicator_cmd.assert_called_once_with(self._ssh)
        mock_setup_replicator_group.assert_called_once_with(
            self._ssh,
            group_name=replicator_module.REPLICATOR_GROUP_NAME)
        mock_reconnect_ssh.assert_not_called()
        mock_setup_replicator_user.assert_called_once_with(self._ssh)
        mock_setup_certificates.assert_called_once_with(
            self._ssh,
            mock_parse_replicator_conn_info.return_value)
        mock_exec_replicator.assert_called_once_with(
            self._ssh,
            mock_parse_replicator_conn_info.return_value['port'],
            mock_setup_certificates.return_value['remote'],
            replicator_module.REPLICATOR_STATE)
        mock_start.assert_called_once()

        self.assertEqual(result, mock_setup_certificates.return_value['local'])

    @mock.patch.object(replicator_module.utils, 'exec_ssh_cmd')
    def test__change_binary_se_context(self, mock_exec_ssh_cmd):
        self.replicator._change_binary_se_context(self._ssh)

        mock_exec_ssh_cmd.assert_called_once_with(
            self._ssh,
            "sudo chcon -t bin_t %s" % replicator_module.REPLICATOR_PATH,
            get_pty=True)

    @mock.patch.object(replicator_module.utils, 'exec_ssh_cmd')
    def test__change_binary_se_context_with_exception(self, mock_exec_ssh_cmd):
        mock_exec_ssh_cmd.side_effect = exception.CoriolisException()

        with self.assertLogs('coriolis.providers.replicator',
                             level=logging.WARN):
            self.replicator._change_binary_se_context(self._ssh)

    @mock.patch.object(
        replicator_module.Replicator, '_get_replicator_state_file'
    )
    @mock.patch.object(replicator_module.Replicator, '_copy_file')
    @mock.patch.object(replicator_module.utils, 'exec_ssh_cmd')
    @mock.patch.object(replicator_module.os, 'remove')
    @mock.patch.object(
        replicator_module.Replicator, '_parse_replicator_conn_info'
    )
    @mock.patch.object(replicator_module.Replicator, '_copy_replicator_cmd')
    @mock.patch.object(replicator_module.Replicator, '_setup_replicator_group')
    @mock.patch.object(replicator_module.Replicator, '_reconnect_ssh')
    @mock.patch.object(replicator_module.Replicator, '_setup_replicator_user')
    @mock.patch.object(replicator_module.Replicator, '_setup_certificates')
    @mock.patch.object(replicator_module.Replicator, '_exec_replicator')
    @mock.patch.object(replicator_module.Replicator, 'start')
    def test__setup_replicator_group_not_existed(
            self, mock_start, mock_exec_replicator, mock_setup_certificates,
            mock_setup_replicator_user, mock_reconnect_ssh,
            mock_setup_replicator_group, mock_copy_replicator_cmd,
            mock_parse_replicator_conn_info, mock_os_remove,
            mock_exec_ssh_cmd, mock_copy_file,
            mock_get_replicator_state_file):
        mock_setup_replicator_group.return_value = False
        mock_reconnect_ssh.return_value = self._ssh

        self.replicator._setup_replicator(self._ssh)
        mock_reconnect_ssh.assert_called_once()

    def test__get_size_from_chunks(self):
        chunks = [
            {"offset": 0, "length": 10},
            {"offset": 10, "length": 10},
        ]
        result = self.replicator._get_size_from_chunks(chunks)

        expected_result = 20 / units.Mi
        self.assertEqual(result, expected_result)

    def test__find_vol_state(self):
        state = [
            {"device-name": mock.sentinel.device_name,
             "other-data": mock.sentinel.other_data},
            {"device-name": mock.sentinel.device_name2,
             "other-data": mock.sentinel.other_data2},
        ]
        expected_result = {
            "device-name": mock.sentinel.device_name,
            "other-data": mock.sentinel.other_data
        }
        result = self.replicator._find_vol_state(
            mock.sentinel.device_name, state)

        self.assertEqual(expected_result, result)

    def test__find_vol_state_not_found(self):
        state = [
            {"device-name": mock.sentinel.device_name,
             "other-data": mock.sentinel.other_data},
            {"device-name": mock.sentinel.device_name2,
             "other-data": mock.sentinel.other_data2},
        ]
        result = self.replicator._find_vol_state("nonexistent_device", state)

        self.assertIsNone(result)

    @mock.patch.object(replicator_module, 'Client')
    def test_replicate_disks(self, mock_Client):
        self.replicator._cli = mock_Client.return_value
        self.replicator._cli.get_changes.return_value = [
            {'length': 100, 'offset': 0}, {'length': 200, 'offset': 100}]

        self.replicator._volumes_info = [
            {"disk_id": "test_disk", "disk_path": "/dev/sdb", "zeroed": True}]
        source_volumes_info = [
            {"disk_id": "test_disk", "disk_path": "/dev/sdb", "zeroed": True}]
        self.replicator._repl_state = ['non-empty']

        result = self.replicator.replicate_disks(
            source_volumes_info, self.backup_writer)

        self.backup_writer.open.assert_called_with("", "test_disk")
        self.replicator._cli.download_chunk.assert_has_calls([
            mock.call("sdb", {"length": 100, "offset": 0}),
            mock.call("sdb", {"length": 200, "offset": 100})])

        self.assertEqual(result, self.replicator._repl_state)
        self.assertEqual(result, self.replicator._cli.get_status.return_value)

    def test_replicate_disks_with_nonexistent_volume(self):
        self.replicator._volumes_info = [
            {"disk_id": "vol1", "disk_path": "/dev/sdb1"},
            {"disk_id": "vol2", "disk_path": "/dev/sdb2"},
        ]
        source_volumes_info = [
            {"disk_id": "vol4", "disk_path": "/dev/sdb4",
             "other-data": "data4"},
        ]

        self.assertRaises(exception.CoriolisException,
                          self.replicator.replicate_disks,
                          source_volumes_info, self.backup_writer)

    @mock.patch.object(replicator_module.Replicator, '_find_vol_state')
    @mock.patch.object(replicator_module, 'Client')
    def test_replicate_disks_initial_sync(self, mock_Client,
                                          mock_find_vol_state):
        self.replicator._cli = mock_Client.return_value

        self.replicator._cli.get_changes.return_value = [
            {'length': 100, 'offset': 0}, {'length': 200, 'offset': 100}]
        self.replicator._volumes_info = [
            {"disk_id": "test_disk", "zeroed": True}]
        source_volumes_info = [
            {"disk_id": "test_disk", "disk_path": "/dev/sdb", "zeroed": True}]
        self.replicator._repl_state = None

        self.replicator.replicate_disks(
            source_volumes_info, self.backup_writer)

        mock_find_vol_state.assert_called_with(
            "sdb", self.replicator._cli.get_status.return_value)
        self.replicator._cli.get_chunks.assert_called_with(
            "sdb", skip_zeros=True)

    def test_replicate_disks_no_chunks(self):
        self.replicator._cli.get_changes.return_value = []
        self.replicator._volumes_info = [
            {"disk_id": "test_disk", "zeroed": False}]
        source_volumes_info = [
            {"disk_id": "test_disk", "disk_path": "/dev/sdb"}]

        result = self.replicator.replicate_disks(
            source_volumes_info, self.backup_writer)

        self.backup_writer.open.assert_not_called()
        self.assertEqual(result, self.replicator._repl_state)

    @mock.patch('builtins.open', new_callable=mock.mock_open)
    @mock.patch.object(replicator_module, 'Client')
    def test__download_full_disk(self, mock_Client, mock_open):
        self.replicator._cli = mock_Client.return_value
        self.replicator._cli.raw_disk_uri.return_value = mock.sentinel.uri
        self.replicator._cli.get_chunks.return_value = [
            {'offset': 0, 'length': 100}
        ]
        self.replicator._cli._cli.get.return_value.__enter__.\
            return_value.iter_content.return_value = [
                b'chunk1', b'chunk2', b'chunk3']
        self.replicator._repl_state = {
            self.disk: {'disk_path': '/dev/sdb', 'disk_id': self.disk}
        }
        self.replicator._event_manager.add_percentage_step.\
            return_value = mock.sentinel.perc_step
        mock_open.return_value.write.side_effect = lambda chunk: len(chunk)

        self.replicator._download_full_disk(self.disk, self.path)

        self.replicator._event_manager.add_percentage_step.assert_called_once()

        mock_open.assert_called_once_with(self.path, 'wb')
        file_handle = mock_open.return_value
        file_handle.write.assert_has_calls([
            mock.call(b'chunk1'),
            mock.call(b'chunk2'),
            mock.call(b'chunk3')]
        )
        self.replicator._event_manager.set_percentage_step.assert_has_calls([
            mock.call(mock.sentinel.perc_step, 6),
            mock.call(mock.sentinel.perc_step, 12),
            mock.call(mock.sentinel.perc_step, 18)])

    @mock.patch('builtins.open', new_callable=mock.mock_open)
    @mock.patch.object(replicator_module, 'Client')
    @mock.patch.object(replicator_module.Replicator, '_get_size_from_chunks')
    def test__download_sparse_disk(
            self, mock_get_size_from_chunks, mock_Client, mock_open):
        self.replicator._cli = mock_Client.return_value
        chunks = [{'offset': 0, 'length': 100}]

        self.replicator._download_sparse_disk(
            self.disk, mock.sentinel.path, chunks)

        mock_open.assert_called_once_with(mock.sentinel.path, 'wb')
        file_handle = mock_open.return_value

        file_handle.truncate.assert_called_once_with(
            self.replicator._cli.get_disk_size.return_value)
        file_handle.seek.assert_called_once_with(0)
        file_handle.write.assert_called_once_with(
            self.replicator._cli.download_chunk.return_value)

        self.replicator._event_manager.add_percentage_step.assert_called_once()
        self.replicator._event_manager.set_percentage_step.assert_called_with(
            self.replicator._event_manager.add_percentage_step.return_value, 1)

    @mock.patch.object(replicator_module.Replicator, '_download_full_disk')
    @mock.patch.object(replicator_module.Replicator, '_download_sparse_disk')
    @mock.patch.object(replicator_module, 'Client')
    def test_download_disk(self, mock_Client, mock_download_sparse_disk,
                           mock_download_full_disk):
        self.replicator._cli = mock_Client.return_value
        disk = "/dev/test_disk"
        self.replicator._cli.get_chunks.return_value = []

        self.replicator.download_disk(disk, self.path)

        self.replicator._cli.get_chunks.assert_called_once_with(
            device=self.disk, skip_zeros=True)
        mock_download_full_disk.assert_called_once_with(
            self.disk, self.path)
        mock_download_sparse_disk.assert_not_called()

    @mock.patch.object(replicator_module, 'Client')
    @mock.patch.object(replicator_module.Replicator, '_download_full_disk')
    @mock.patch.object(replicator_module.Replicator, '_download_sparse_disk')
    def test_download_disk_with_chunks(
            self, mock_download_sparse_disk, mock_download_full_disk,
            mock_Client):
        self.replicator._cli = mock_Client.return_value

        disk = "/dev/test_disk"
        self.replicator._cli.get_chunks.return_value = [
            {'offset': 0, 'length': 100}]

        self.replicator.download_disk(disk, self.path)

        self.replicator._cli.get_chunks.assert_called_once_with(
            device=self.disk, skip_zeros=True)
        mock_download_full_disk.assert_not_called()
        mock_download_sparse_disk.assert_called_once_with(
            self.disk, self.path, self.replicator._cli.get_chunks.return_value)
