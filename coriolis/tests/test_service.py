# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

import logging
import os
from unittest import mock

import ddt

from coriolis import service
from coriolis.tests import test_base


@ddt.ddt
class ServiceTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis Service class."""

    @ddt.data(
        (['--worker-process-count', '5'], 5, [], False),
        (['--worker-process-count', '10'], 10, [], False),
        (['--worker-process-count', '15'], 15, [], False),
        (['--worker-process-count', '20'], 20, [], False),
        ([], None, [], False),
        (['--worker-process-count', '5', '--unknown-arg'], 5,
         ['--unknown-arg'], False),
        (['--worker-process-count', '-5'], None, None, True),
        (['--worker-process-count', '0'], None, None, True),
        (['--worker-process-count', 'not-a-number'], None, None, True)
    )
    @ddt.unpack
    def test_get_worker_count_from_args(self, input_args, expected_count,
                                        expected_unknown_args,
                                        expect_exception):
        if expect_exception:
            self.assertRaises(SystemExit, service.get_worker_count_from_args,
                              input_args)
        else:
            worker_count, unknown_args = service.get_worker_count_from_args(
                input_args)
            self.assertEqual(worker_count, expected_count)
            self.assertEqual(unknown_args, expected_unknown_args)

    @ddt.data(
        ({}, False, False, [], logging.WARN),
        ({'lock_path': ""}, False, False, [], logging.WARN),
        ({'lock_path': "/path/to/locks"}, False, False, [], logging.WARN),
        ({'lock_path': "/path/to/locks"}, True, False, [], logging.WARN),
        ({'lock_path': "/path/to/locks"}, True, True, ['file1', 'file2'],
         logging.WARN),
        ({'lock_path': "/path/to/locks"}, True, True, [], logging.INFO),
    )
    @ddt.unpack
    @mock.patch.object(os.path, 'exists')
    @mock.patch.object(os.path, 'isdir')
    @mock.patch.object(os, 'listdir')
    def test_check_locks_dir_empty(self, oslo_concurrency, exists, isdir,
                                   listdir, expected_log_level, mock_listdir,
                                   mock_isdir, mock_exists):
        mock_exists.return_value = exists
        mock_isdir.return_value = isdir
        mock_listdir.return_value = listdir
        with mock.patch.object(service.CONF, 'oslo_concurrency',
                               oslo_concurrency):
            with self.assertLogs('coriolis.service', level=expected_log_level):
                service.check_locks_dir_empty()


class WSGIServiceTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis WSGIService class."""

    @mock.patch.object(service.wsgi.Loader, 'load_app')
    @mock.patch.object(service, 'rpc')
    @mock.patch.object(service, 'CONF')
    @mock.patch.object(service.wsgi, 'Server')
    def test_init(self, mock_server, mock_conf, mock_rpc, mock_load_app):
        mock_conf.api_migration_listen = mock.sentinel.listen
        mock_conf.api_migration_listen_port = mock.sentinel.listen_port
        mock_conf.api_migration_workers = mock.sentinel.workers
        mock_conf.api_migration_listen = mock.sentinel.listen
        mock_rpc.init.return_value = mock.sentinel.transport

        wsgi_service = service.WSGIService(mock.sentinel.name, worker_count=10,
                                           init_rpc=True)

        mock_rpc.init.assert_called_once_with()
        mock_load_app.assert_called_once_with(mock.sentinel.name)
        mock_server.assert_called_once_with(
            mock_conf,
            mock.sentinel.name,
            mock_load_app.return_value,
            host=mock_conf.api_migration_listen,
            port=mock_conf.api_migration_listen_port)
        self.assertEqual(wsgi_service._host, mock_conf.api_migration_listen)
        self.assertEqual(wsgi_service._port,
                         mock_conf.api_migration_listen_port)
        self.assertEqual(wsgi_service._workers, 10)
        self.assertEqual(wsgi_service._app, mock_load_app.return_value)
        self.assertEqual(wsgi_service._server, mock_server.return_value)

    @mock.patch('platform.system')
    @mock.patch('oslo_service.wsgi.Server')
    @mock.patch('oslo_service.wsgi.Loader.load_app')
    @mock.patch('coriolis.rpc.messaging.get_transport')
    @mock.patch.object(service, 'CONF')
    def test_init_windows(self, mock_conf, mock_get_transport, mock_load_app,
                          mock_server, mock_system):
        mock_system.return_value = "Windows"
        mock_conf.api_migration_workers = None
        mock_load_app.return_value = mock.MagicMock()
        mock_server.return_value = mock.MagicMock()
        mock_get_transport.return_value = mock.MagicMock()

        result = service.WSGIService('test_service', None, True)

        self.assertEqual(result._workers, 1)

    @mock.patch('platform.system')
    @mock.patch('oslo_service.wsgi.Server')
    @mock.patch('oslo_service.wsgi.Loader.load_app')
    @mock.patch('oslo_concurrency.processutils.get_worker_count')
    @mock.patch('coriolis.rpc.messaging.get_transport')
    @mock.patch.object(service, 'CONF')
    def test_init_sets_workers_based_on_system_on_non_windows(
            self, mock_conf, mock_get_transport, mock_get_worker_count,
            mock_load_app, mock_server, mock_system):
        mock_system.return_value = "Linux"
        mock_conf.api_migration_workers = None
        mock_load_app.return_value = mock.MagicMock()
        mock_server.return_value = mock.MagicMock()
        mock_get_transport.return_value = mock.MagicMock()

        result = service.WSGIService('test_service', None, True)

        self.assertEqual(result._workers, mock_get_worker_count.return_value)

    @mock.patch.object(service, 'CONF')
    @mock.patch('oslo_service.wsgi.Loader.load_app')
    @mock.patch('coriolis.rpc.messaging.get_transport')
    @mock.patch('eventlet.listen')
    def test_service_methods(self, mock_listen, mock_get_transport,
                             mock_load_app, mock_conf):
        mock_conf.api_migration_workers = 10
        mock_load_app.return_value = mock.MagicMock()
        mock_get_transport.return_value = mock.MagicMock()
        mock_socket = mock.MagicMock()
        mock_socket.getsockname.return_value = ('localhost', 8080)
        mock_listen.return_value = mock_socket

        result = service.WSGIService('test_service', None, True)

        result._server = mock.MagicMock()

        self.assertEqual(result.get_workers_count(),
                         mock_conf.api_migration_workers)

        result.start()
        result._server.start.assert_called_once()

        result.stop()
        result._server.stop.assert_called_once()

        result.wait()
        result._server.wait.assert_called_once()

        result.reset()
        result._server.reset.assert_called_once()


class MessagingServiceTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis MessagingService class."""

    @mock.patch.object(service, 'rpc')
    @mock.patch.object(service, 'CONF')
    @mock.patch.object(service.messaging, 'Target')
    @mock.patch.object(service.utils, 'get_hostname')
    def test_init(self, mock_get_hostname, mock_target, mock_conf, mock_rpc):
        mock_conf.api_migration_listen = mock.sentinel.listen
        mock_conf.api_migration_listen_port = mock.sentinel.listen_port
        mock_conf.api_migration_workers = mock.sentinel.workers
        mock_conf.api_migration_listen = mock.sentinel.listen
        mock_rpc.init.return_value = mock.sentinel.transport
        mock_get_hostname.return_value = mock.sentinel.hostname

        messaging_service = service.MessagingService(
            mock.sentinel.topic, mock.sentinel.endpoints,
            mock.sentinel.version, worker_count=10, init_rpc=True)

        mock_rpc.init.assert_called_once_with()
        mock_target.assert_called_once_with(
            topic=mock.sentinel.topic,
            server=mock.sentinel.hostname,
            version=mock.sentinel.version)
        mock_rpc.get_server.assert_called_once_with(
            mock_target.return_value, mock.sentinel.endpoints)
        self.assertEqual(messaging_service._workers, 10)
        self.assertEqual(messaging_service._server,
                         mock_rpc.get_server.return_value)

    @mock.patch('platform.system')
    @mock.patch.object(service, 'rpc')
    @mock.patch.object(service, 'CONF')
    @mock.patch.object(service.messaging, 'Target')
    @mock.patch.object(service.utils, 'get_hostname')
    def test_init_windows(self, mock_get_hostname, mock_target, mock_conf,
                          mock_rpc, mock_system):
        mock_system.return_value = "Windows"
        mock_conf.api_migration_listen = mock.sentinel.listen
        mock_conf.api_migration_listen_port = mock.sentinel.listen_port
        mock_conf.api_migration_workers = mock.sentinel.workers
        mock_conf.api_migration_listen = mock.sentinel.listen
        mock_rpc.init.return_value = mock.sentinel.transport
        mock_get_hostname.return_value = mock.sentinel.hostname

        messaging_service = service.MessagingService(
            mock.sentinel.topic, mock.sentinel.endpoints,
            mock.sentinel.version, worker_count=10, init_rpc=True)

        mock_rpc.init.assert_called_once_with()
        mock_target.assert_called_once_with(
            topic=mock.sentinel.topic,
            server=mock.sentinel.hostname,
            version=mock.sentinel.version)
        mock_rpc.get_server.assert_called_once_with(
            mock_target.return_value, mock.sentinel.endpoints)
        self.assertEqual(messaging_service._workers, 1)
        self.assertEqual(messaging_service._server,
                         mock_rpc.get_server.return_value)

    @mock.patch.object(service, 'rpc')
    @mock.patch.object(service, 'CONF')
    @mock.patch.object(service.utils, 'get_hostname')
    @mock.patch.object(service.processutils, 'get_worker_count')
    def test_init_with_none_worker_count(self, mock_get_worker_count,
                                         mock_get_hostname,
                                         mock_conf, mock_rpc):
        mock_conf.api_migration_listen = mock.sentinel.listen
        mock_conf.api_migration_listen_port = mock.sentinel.listen_port
        mock_conf.api_migration_listen = mock.sentinel.listen
        mock_rpc.init.return_value = mock.sentinel.transport
        mock_get_hostname.return_value = mock.sentinel.hostname

        result = service.MessagingService(
            mock.sentinel.topic, mock.sentinel.endpoints,
            mock.sentinel.version, worker_count=None, init_rpc=True)

        self.assertEqual(result._workers, mock_get_worker_count.return_value)

    @mock.patch.object(service, 'rpc')
    @mock.patch.object(service, 'CONF')
    @mock.patch.object(service.utils, 'get_hostname')
    @mock.patch.object(service.processutils, 'get_worker_count')
    def test_service_methods(self, mock_get_worker_count, mock_get_hostname,
                             mock_conf, mock_rpc):
        mock_conf.api_migration_listen = mock.sentinel.listen
        mock_conf.api_migration_listen_port = mock.sentinel.listen_port
        mock_conf.api_migration_workers = mock.sentinel.worker_count
        mock_conf.api_migration_listen = mock.sentinel.listen
        mock_rpc.init.return_value = mock.sentinel.transport
        mock_get_hostname.return_value = mock.sentinel.hostname
        mock_get_worker_count.return_value = mock.sentinel.worker_count

        mock_server = mock.MagicMock()
        mock_rpc.get_server.return_value = mock_server

        result = service.MessagingService(
            mock.sentinel.topic, mock.sentinel.endpoints,
            mock.sentinel.version, worker_count=None, init_rpc=True)

        self.assertEqual(result.get_workers_count(),
                         mock.sentinel.worker_count)

        result.start()
        mock_server.start.assert_called_once()

        result.stop()
        mock_server.stop.assert_called_once()

        result.wait()

        result.reset()
        mock_server.reset.assert_called_once()
