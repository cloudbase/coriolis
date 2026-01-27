# Copyright 2024 Cloudbase Solutions Srl
# All Rights Reserved.

import datetime
import logging
import os
import tempfile
from unittest import mock

from coriolis import exception
from coriolis.providers import backup_writers
from coriolis.providers import provider_utils
from coriolis.tests import test_base
from coriolis.tests import testutils


class CoriolisTestException(Exception):
    pass


class BackupWritersTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis backup_writers module."""

    def setUp(self):
        super(BackupWritersTestCase, self).setUp()
        self.mock_ssh = mock.MagicMock()

    @mock.patch('coriolis.utils.test_ssh_path')
    @mock.patch('coriolis.utils.exec_ssh_cmd')
    def test__disable_lvm2_lvmetad(self, mock_exec_ssh_cmd,
                                   mock_test_ssh_path):
        cfg = "/etc/lvm/lvm.conf"
        mock_test_ssh_path.return_value = True

        backup_writers._disable_lvm2_lvmetad(self.mock_ssh)

        mock_test_ssh_path.assert_called_once_with(self.mock_ssh, cfg)
        expected_calls = [
            mock.call(
                self.mock_ssh,
                'sudo sed -i "s/use_lvmetad.*=.*1/use_lvmetad = 0/g" %s' %
                cfg, get_pty=True),
            mock.call(self.mock_ssh,
                      'sudo service lvm2-lvmetad stop', get_pty=True),
            mock.call(self.mock_ssh, 'sudo vgchange -an', get_pty=True)]
        mock_exec_ssh_cmd.assert_has_calls(expected_calls)

    @mock.patch('coriolis.utils.test_ssh_path')
    @mock.patch('coriolis.utils.exec_ssh_cmd')
    def test__disable_lvm_metad_udev_rule(self, mock_exec_ssh_cmd,
                                          mock_test_ssh_path):
        rule_path = ["/lib/udev/rules.d/69-lvm-metad.rules",
                     "/lib/udev/rules.d/69-dm-lvm.rules"]
        mock_test_ssh_path.return_value = True

        backup_writers._disable_lvm_metad_udev_rule(self.mock_ssh)

        mock_test_ssh_path.assert_has_calls(
            [mock.call(self.mock_ssh, rule_path[0]),
             mock.call(self.mock_ssh, rule_path[1])])

        expected_calls = [
            mock.call(self.mock_ssh, 'sudo rm %s' % rule_path[0],
                      get_pty=True),
            mock.call(self.mock_ssh, 'sudo rm %s' % rule_path[1],
                      get_pty=True)]
        mock_exec_ssh_cmd.assert_has_calls(expected_calls)

    def test__check_deserialize_key(self):
        mock_rsa_key = mock.MagicMock(spec=backup_writers.paramiko.RSAKey)

        result = backup_writers._check_deserialize_key(mock_rsa_key)

        self.assertEqual(result, mock_rsa_key)

    @mock.patch('coriolis.utils.deserialize_key')
    def test__check_deserialize_key_with_pem_string(
            self, mock_deserialize_key):
        mock_key = 'mock_key'
        mock_deserialized_key = backup_writers.paramiko.RSAKey.generate(
            bits=2048)
        mock_deserialize_key.return_value = mock_deserialized_key

        backup_writers._check_deserialize_key(mock_key)

        mock_deserialize_key.assert_called_once_with(
            mock_key, backup_writers.CONF.serialization.temp_keypair_password)

    @mock.patch('coriolis.utils.deserialize_key')
    def test__check_deserialize_key_with_exception(self, mock_deserialize_key):
        mock_deserialize_key.side_effect = exception.CoriolisException()

        self.assertRaises(exception.CoriolisException,
                          backup_writers._check_deserialize_key,
                          mock.sentinel.key)


class BackupWritersFactoryTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis BackupWritersFactory class."""

    # Helper functions
    def _get_writer(self, backend):
        writer_connection_info = {"backend": backend, "connection_details": {}}
        return backup_writers.BackupWritersFactory(writer_connection_info, {})

    def _get_factory(self, info):
        return backup_writers.BackupWritersFactory(info, {})

    @mock.patch.object(backup_writers.SSHBackupWriter, 'from_connection_info')
    def test_get_writer_with_ssh(self, mock_ssh_backup_writer):
        factory = self._get_writer(backup_writers.BACKUP_WRITER_SSH)

        result = factory.get_writer()
        mock_ssh_backup_writer.assert_called_with(
            factory._conn_info, factory._volumes_info)

        self.assertEqual(result, mock_ssh_backup_writer.return_value)

    @mock.patch.object(backup_writers.HTTPBackupWriter, 'from_connection_info')
    def test_get_writer_with_http(self, mock_http_backup_writer):
        factory = self._get_writer(backup_writers.BACKUP_WRITER_HTTP)

        result = factory.get_writer()
        mock_http_backup_writer.assert_called_with(
            factory._conn_info, factory._volumes_info)

        self.assertEqual(result, mock_http_backup_writer.return_value)

    @mock.patch.object(backup_writers.FileBackupWriter, 'from_connection_info')
    def test_get_writer_with_file(self, mock_file_backup_writer):
        factory = self._get_writer(backup_writers.BACKUP_WRITER_FILE)

        result = factory.get_writer()
        mock_file_backup_writer.assert_called_with(
            factory._conn_info, factory._volumes_info)

        self.assertEqual(result, mock_file_backup_writer.return_value)

    def test_get_writer_with_exception(self):
        with mock.patch.object(backup_writers.BackupWritersFactory,
                               '_validate_info') as mock_validate_info:
            mock_validate_info.return_value = None
            factory = self._get_writer("invalid backup writer type")
            self.assertRaises(exception.CoriolisException, factory.get_writer)

    def test__validate_info_with_non_dict(self):
        self.assertRaises(exception.CoriolisException,
                          self._get_factory, "not a dict")

    def test__validate_info_with_missing_backend(self):
        self.assertRaises(exception.CoriolisException,
                          self._get_factory, {"connection_details": {}})

    def test__validate_info_with_invalid_backend(self):
        self.assertRaises(exception.CoriolisException,
                          self._get_factory,
                          {"backend": "invalid", "connection_details": {}})

    def test__validate_info_with_missing_connection_details(self):
        backup_writers.BackupWritersFactory._conn_info = None
        self.assertRaises(exception.CoriolisException,
                          self._get_factory, {"backend": "ssh"})


class BaseBackupWriterTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis BaseBackupWriter class."""

    @mock.patch.object(
        backup_writers.BaseBackupWriter, '__abstractmethods__', set()
    )
    def setUp(self):
        super(BaseBackupWriterTestCase, self).setUp()
        self.writer = backup_writers.BaseBackupWriter()

    @mock.patch.object(backup_writers.BaseBackupWriter, '_get_impl')
    def test_open(self, mock_get_impl):
        with self.writer.open(
                mock.sentinel.path, mock.sentinel.disk_id) as impl:
            mock_get_impl.assert_called_once_with(
                mock.sentinel.path, mock.sentinel.disk_id)
            impl._open.assert_called_once()

        impl.close.assert_called_once()

    @mock.patch.object(backup_writers.BaseBackupWriter, '_get_impl')
    def test_open_with_exception(self, mock_get_impl):
        ex = CoriolisTestException()
        mock_get_impl.return_value._open.side_effect = ex

        def open_context():
            with self.writer.open(mock.sentinel.path, mock.sentinel.disk_id):
                pass

        self.assertRaises(CoriolisTestException, open_context)

        impl = mock_get_impl.return_value
        impl._handle_exception.assert_called_once_with(ex)
        impl.close.assert_called_once()


class FileBackupWriterImplTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis FileBackupWriterImpl class."""

    def setUp(self):
        super(FileBackupWriterImplTestCase, self).setUp()
        self.writer = backup_writers.FileBackupWriterImpl(
            mock.sentinel.path, mock.sentinel.disk_id)
        self.mock_file = mock.MagicMock()
        self.writer._file = self.mock_file

    @mock.patch('builtins.open')
    def test__open(self, mock_open):
        self.writer._open()

        calls = [mock.call(self.writer._path, 'ab+'),
                 mock.call().close(),
                 mock.call(self.writer._path, 'rb+')]
        mock_open.assert_has_calls(calls)

    def test_seek(self):
        self.writer.seek(mock.sentinel.pos)
        self.writer._file.seek.assert_called_once_with(mock.sentinel.pos)

    def test_truncate(self):
        self.writer.truncate(mock.sentinel.size)
        self.writer._file.truncate.assert_called_once_with(mock.sentinel.size)

    def test_write(self):
        self.writer.write(mock.sentinel.data)
        self.writer._file.write.assert_called_once_with(mock.sentinel.data)

    @mock.patch('os.system')
    def test_close(self, mock_system):
        self.writer.close()

        self.mock_file.close.assert_called_once()
        mock_system.assert_called_once_with("sudo sync")
        self.assertIsNone(self.writer._file)


class FileBackupWriterTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis FileBackupWriter class."""

    def setUp(self):
        super(FileBackupWriterTestCase, self).setUp()
        self.writer = backup_writers.FileBackupWriter()

    @mock.patch.object(backup_writers, 'FileBackupWriterImpl')
    def test__get_impl(self, mock_file_backup_writer_impl):
        result = self.writer._get_impl(
            mock.sentinel.path, mock.sentinel.disk_id)

        mock_file_backup_writer_impl.assert_called_once_with(
            mock.sentinel.path, mock.sentinel.disk_id)
        self.assertEqual(result, mock_file_backup_writer_impl.return_value)

    def test_from_connection_info(self):
        result = backup_writers.FileBackupWriter.from_connection_info(
            mock.sentinel.connection_info, mock.sentinel.volume_info)

        self.assertIsInstance(result, backup_writers.FileBackupWriter)


class SSHBackupWriterImplTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis SSHBackupWriterImpl class."""

    def setUp(self):
        super(SSHBackupWriterImplTestCase, self).setUp()
        self.writer = backup_writers.SSHBackupWriterImpl(
            mock.sentinel.path, mock.sentinel.path)
        self._ssh = mock.MagicMock()
        self._stdin = mock.MagicMock()
        self._stdout = mock.MagicMock()
        self._stderr = mock.MagicMock()
        self._enc_queue = mock.MagicMock()
        self._sender_queue = mock.MagicMock()
        self.writer._ssh = self._ssh
        self.writer._stdin = self._stdin
        self.writer._stdout = self._stdout

    def test__set_ssh_client(self):
        self.writer._set_ssh_client(self._ssh)
        self.assertEqual(self.writer._ssh, self._ssh)

    def test__exec_helper_cmd(self):
        self._ssh.exec_command.return_value = (
            self._stdin, self._stdout, self._stderr)

        original_exec_helper_cmd = testutils.get_wrapped_function(
            self.writer._exec_helper_cmd)

        original_exec_helper_cmd(self.writer)

        self._ssh.exec_command.assert_called_once_with(
            "chmod +x write_data && sudo ./write_data")

    @mock.patch('coriolis.data_transfer.encode_data')
    def test__encode_data(self, mock_encode_data):
        result = self.writer._encode_data(
            'test_content', mock.sentinel.offset, 456)

        mock_encode_data.assert_called_once_with(
            456, mock.sentinel.path, mock.sentinel.offset, 'test_content',
            compress=self.writer._compress_transfer)

        self.assertEqual(result, mock_encode_data.return_value)

    @mock.patch('coriolis.data_transfer.encode_eod')
    def test__encode_eod(self, mock_encode_eod):
        self.writer._msg_id = mock.sentinel.msg_id

        result = self.writer._encode_eod()

        mock_encode_eod.assert_called_once_with(mock.sentinel.msg_id)
        self.assertEqual(result, mock_encode_eod.return_value)

    def test__send_msg(self):
        self._stdout.channel.exit_status_ready.return_value = False

        original_send_msg = testutils.get_wrapped_function(
            self.writer._send_msg)

        original_send_msg(self.writer, mock.sentinel.data)

        self._stdout.channel.exit_status_ready.assert_called_once()
        self._stdin.write.assert_called_once_with(mock.sentinel.data)
        self._stdin.flush.assert_called_once()
        self._stdout.read.assert_called_once_with(4)

    def test__send_msg_with_exception(self):
        self._stdout.channel.exit_status_ready.return_value = True
        self._stdout.channel.recv_exit_status.return_value = 1

        original_send_msg = testutils.get_wrapped_function(
            self.writer._send_msg)

        self.assertRaises(exception.CoriolisException, original_send_msg,
                          self.writer, mock.sentinel.data)

    @mock.patch.object(backup_writers, 'eventlet')
    @mock.patch.object(backup_writers.SSHBackupWriterImpl, '_encoder')
    @mock.patch.object(backup_writers.SSHBackupWriterImpl, '_sender')
    @mock.patch.object(backup_writers.SSHBackupWriterImpl, '_exec_helper_cmd')
    def test__open(self, mock_exec_helper_cmd, mock_sender, mock_encoder,
                   mock_eventlet):
        self.writer._open()

        mock_exec_helper_cmd.assert_called_once()
        mock_eventlet.spawn.assert_has_calls(
            [mock.call(mock_sender), mock.call(mock_encoder),
             mock.call(mock_encoder), mock.call(mock_encoder)])

        self.assertEqual(len(self.writer._encoder_evt), 3)

    def test_seek(self):
        self.writer.seek(mock.sentinel.offset)
        self.assertEqual(self.writer._offset, mock.sentinel.offset)

    @mock.patch.object(backup_writers.SSHBackupWriterImpl, '_send_msg')
    def test__sender(self, mock_send_msg):
        mock_send_msg.side_effect = [
            mock.sentinel.data, CoriolisTestException()]
        self.writer._sender_q = self._sender_queue

        self.assertRaises(CoriolisTestException, self.writer._sender)

        mock_send_msg.assert_called_with(
            self.writer._sender_q.get.return_value)
        self.assertEqual(self._sender_queue.task_done.call_count, 2)

    @mock.patch.object(backup_writers.SSHBackupWriterImpl, '_encode_data')
    def test__encoder(self, mock_encode_data):
        self.writer._enc_q = self._enc_queue
        self.writer._sender_q = self._sender_queue

        mock_encode_data.side_effect = [
            {"data": mock.sentinel.data,
             "offset": mock.sentinel.offset,
             "msg_id": mock.sentinel.msg_id},
            CoriolisTestException()]

        self.assertRaises(CoriolisTestException, self.writer._encoder)

        mock_encode_data.assert_called_with(
            self.writer._enc_q.get.return_value["data"],
            self.writer._enc_q.get.return_value["offset"],
            self.writer._enc_q.get.return_value["msg_id"]
        )

        self._sender_queue.put.assert_called_once_with(
            {"data": mock.sentinel.data,
             "offset": mock.sentinel.offset,
             "msg_id": mock.sentinel.msg_id}
        )

        self.assertEqual(self._enc_queue.task_done.call_count, 2)

    def test_write(self):
        self.writer._closing = False
        self.writer._exception = None
        self.writer._enc_q = self._enc_queue
        self.writer._offset = 0
        self.writer._msg_id = 0

        self.writer.write('test_data')

        expected_payload = {
            "offset": 0,
            "data": 'test_data',
            "msg_id": 0
        }
        self._enc_queue.put.assert_called_once_with(expected_payload)
        self.assertEqual(self.writer._offset, len('test_data'))
        self.assertEqual(self.writer._msg_id, 1)

    def test_write_with_closing(self):
        self.writer._closing = True
        self.assertRaises(exception.CoriolisException, self.writer.write,
                          mock.sentinel.data)

    def test_write_with_exception(self):
        self.writer._exception = exception.CoriolisException()
        self.assertRaises(exception.CoriolisException, self.writer.write,
                          mock.sentinel.data)

    def test__wait_for_queues(self):
        self.writer._enc_q = mock.MagicMock()
        self.writer._sender_q = mock.MagicMock()
        self.writer._exception = None

        self.writer._enc_q.unfinished_tasks = True
        self.writer._sender_q.unfinished_tasks = True

        start_time = datetime.datetime.now()
        timeout_time = start_time + datetime.timedelta(seconds=600)

        with mock.patch("datetime.datetime") as \
            mock_datetime, mock.patch("time.sleep"):
            mock_datetime.now.side_effect = [start_time, timeout_time]

            self.assertRaises(exception.CoriolisException,
                              self.writer._wait_for_queues)

    def test__wait_for_queues_no_unfinished_tasks(self):
        self.writer._enc_q = mock.MagicMock()
        self.writer._sender_q = mock.MagicMock()
        self.writer._exception = None

        self.writer._enc_q.unfinished_tasks = False
        self.writer._sender_q.unfinished_tasks = False

        self.assertIsNone(self.writer._wait_for_queues())

    @mock.patch.object(backup_writers.SSHBackupWriterImpl, '_wait_for_queues')
    def test_close_with_exception(self, mock_wait_for_queues):
        mock_wait_for_queues.return_value = None
        self.writer._exception = Exception()
        self.assertRaises(exception.CoriolisException, self.writer.close)

    @mock.patch.object(backup_writers.SSHBackupWriterImpl, '_wait_for_queues')
    @mock.patch.object(backup_writers.SSHBackupWriterImpl, '_send_msg')
    @mock.patch.object(backup_writers.SSHBackupWriterImpl, '_encode_eod')
    def test_close_with_ssh(self, mock_encode_eod, mock_send_msg,
                            mock_wait_for_queues):
        self.writer._ssh = self._ssh
        mock_wait_for_queues.return_value = None
        self.writer._exception = None
        self.writer.close()

        mock_send_msg.assert_called_once_with(mock_encode_eod.return_value)
        self._ssh.exec_command.assert_called_once_with("sudo sync")
        self._ssh.close.assert_called_once()
        self.assertIsNone(self.writer._ssh)

    @mock.patch.object(backup_writers.SSHBackupWriterImpl, '_wait_for_queues')
    @mock.patch.object(backup_writers, 'eventlet')
    def test_close_with_sender_evt(self, mock_eventlet, mock_wait_for_queues):
        mock_wait_for_queues.return_value = None
        mock_sender_evt = mock.MagicMock()
        self.writer._sender_evt = mock_sender_evt
        self.writer._ssh = None

        self.writer.close()

        mock_eventlet.kill.assert_called_once_with(mock_sender_evt)
        self.assertIsNone(self.writer._sender_evt)

    @mock.patch.object(backup_writers.SSHBackupWriterImpl, '_wait_for_queues')
    @mock.patch.object(backup_writers, 'eventlet')
    def test_close_with_encoder_evt(self, mock_eventlet, mock_wait_for_queues):
        mock_wait_for_queues.return_value = None
        mock_encoder_evt = [mock.MagicMock(), mock.MagicMock()]
        self.writer._encoder_evt = mock_encoder_evt
        self.writer._ssh = None

        self.writer.close()

        mock_eventlet.kill.assert_has_calls([
            mock.call(mock_encoder_evt[0]),
            mock.call(mock_encoder_evt[1])])
        self.assertEqual(self.writer._encoder_evt, [])

    def test__handle_exception_with_exit_status(self):
        self.writer._stdout.channel.exit_status_ready.return_value = True
        self.writer._stdout.channel.recv_exit_status.return_value = 1
        self.writer._ssh = self._ssh

        self.assertRaises(exception.CoriolisException,
                          self.writer._handle_exception, Exception())

        self._ssh.close.assert_called_once()
        self.assertIsNone(self.writer._ssh)

    def test__handle_exception_without_exit_status(self):
        self.writer._stdout.channel.exit_status_ready.return_value = False
        self.writer._ssh = self._ssh

        self.assertRaises(exception.CoriolisException,
                          self.writer._handle_exception, Exception())

        self._ssh.close.assert_called_once()
        self.assertIsNone(self.writer._ssh)


class SSHBackupWriterTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis SSHBackupWriter class."""

    def setUp(self):
        super(SSHBackupWriterTestCase, self).setUp()
        self.ip = mock.sentinel.ip
        self.port = mock.sentinel.port
        self._ssh = mock.MagicMock()
        self._sftp = mock.MagicMock()

        self.pkey = None
        self.volume_info = {
            "disk_id": mock.sentinel.disk_id,
            "path": mock.sentinel.path,
            "volume_dev": mock.sentinel.volume_dev,
        }
        self.conn_info = {
            "ip": self.ip,
            "port": self.port,
            "username": mock.sentinel.username,
            "pkey": self.pkey,
            "password": mock.sentinel.password,
        }
        self.writer = backup_writers.SSHBackupWriter(
            self.ip, self.port, mock.sentinel.username, self.pkey,
            mock.sentinel.password, self.volume_info)

    @mock.patch.object(backup_writers, '_check_deserialize_key')
    def test_from_connection_info(self, mock_deserialize_key):
        self.conn_info["pkey"] = mock.sentinel.pkey

        result = backup_writers.SSHBackupWriter.from_connection_info(
            self.conn_info, mock.sentinel.volume_info)

        mock_deserialize_key.assert_called_once_with(mock.sentinel.pkey)

        self.assertIsInstance(result, backup_writers.SSHBackupWriter)

    def test_from_connection_info_missing_required_field(self):
        # Remove the IP from the connection info to test the missing required.
        self.conn_info.pop("ip")

        self.assertRaises(exception.CoriolisException,
                          backup_writers.SSHBackupWriter.from_connection_info,
                          self.conn_info, self.volume_info)

    def test_from_connection_info_missing_password(self):
        self.conn_info.pop("password")

        self.assertRaises(exception.CoriolisException,
                          backup_writers.SSHBackupWriter.from_connection_info,
                          self.conn_info, self.volume_info)

    @mock.patch.object(backup_writers.SSHBackupWriter, '_connect_ssh')
    @mock.patch.object(backup_writers, '_disable_lvm_metad_udev_rule')
    @mock.patch.object(backup_writers, '_disable_lvm2_lvmetad')
    @mock.patch.object(backup_writers, 'SSHBackupWriterImpl')
    @mock.patch.object(backup_writers.SSHBackupWriter, '_copy_helper_cmd')
    def test__get_impl(self, mock_copy_helper_cmd, mock_ssh_backup_writer_impl,
                       mock_disable_lvm2_lvmetad,
                       mock_disable_lvm_metad_udev_rule, mock_connect_ssh):
        mock_connect_ssh.return_value = self._ssh
        self.writer._volumes_info = [self.volume_info]

        result = self.writer._get_impl(
            self.volume_info["path"], self.volume_info["disk_id"])

        mock_connect_ssh.assert_called_once_with()
        mock_disable_lvm_metad_udev_rule.assert_called_once_with(self._ssh)
        mock_disable_lvm2_lvmetad.assert_called_once_with(self._ssh)
        mock_copy_helper_cmd.assert_called_once_with(self._ssh)
        mock_ssh_backup_writer_impl.return_value.\
            _set_ssh_client.assert_called_once_with(self._ssh)
        mock_ssh_backup_writer_impl.assert_called_once_with(
            mock.sentinel.volume_dev, mock.sentinel.disk_id)

        self.assertEqual(result, mock_ssh_backup_writer_impl.return_value)

    @mock.patch.object(backup_writers.SSHBackupWriter, '_connect_ssh')
    @mock.patch.object(backup_writers, '_disable_lvm_metad_udev_rule')
    @mock.patch.object(backup_writers, '_disable_lvm2_lvmetad')
    def test__get_impl_no_matching_disk_id(
            self, mock_disable_lvm2_lvmetad, mock_disable_lvm_metad_udev_rule,
            mock_connect_ssh):
        self.writer._volumes_info = [self.volume_info]

        self.assertRaises(exception.CoriolisException, self.writer._get_impl,
                          mock.sentinel.path, "invalid_disk_id")

    @mock.patch.object(backup_writers.SSHBackupWriter, '_connect_ssh')
    @mock.patch.object(backup_writers, '_disable_lvm_metad_udev_rule')
    @mock.patch.object(backup_writers, '_disable_lvm2_lvmetad')
    def test__get_impl_multiple_matching_disks(
            self, mock_disable_lvm2_lvmetad, mock_disable_lvm_metad_udev_rule,
            mock_connect_ssh):
        mock_connect_ssh.return_value = self._ssh
        self.writer._volumes_info = [self.volume_info, self.volume_info]

        self.assertRaises(exception.CoriolisException, self.writer._get_impl,
                          mock.sentinel.path, mock.sentinel.disk_id)

    @mock.patch('paramiko.SSHClient')
    def test__copy_helper_cmd(self, mock_ssh_client):
        mock_ssh = mock_ssh_client.return_value
        mock_ssh.open_sftp.return_value = self._sftp

        original_copy_helper_cmd = testutils.get_wrapped_function(
            self.writer._copy_helper_cmd)

        original_copy_helper_cmd(self.writer, mock_ssh)

        self._sftp.stat.assert_called_once_with('write_data')
        self._sftp.put.assert_not_called()
        self._sftp.close.assert_called_once()

    @mock.patch.object(backup_writers.utils, 'get_resources_bin_dir')
    @mock.patch('paramiko.SSHClient')
    def test__copy_helper_cmd_file_does_not_exist(
            self, mock_ssh_client, mock_get_resources_bin_dir):
        mock_ssh = mock_ssh_client.return_value
        mock_ssh.open_sftp.return_value = self._sftp
        local_path = os.path.join(
            mock_get_resources_bin_dir.return_value, 'write_data')

        self._sftp.stat.side_effect = IOError(
            backup_writers.errno.ENOENT, 'No such file or directory')

        original_copy_helper_cmd = testutils.get_wrapped_function(
            self.writer._copy_helper_cmd)

        original_copy_helper_cmd(self.writer, mock_ssh)

        self._sftp.put.assert_called_once_with(local_path, 'write_data')
        self._sftp.close.assert_called_once()

    @mock.patch('paramiko.SSHClient')
    def test__copy_helper_cmd_stat_error(self, mock_ssh_client):
        mock_ssh = mock_ssh_client.return_value
        mock_ssh.open_sftp.return_value = self._sftp
        self._sftp.stat.side_effect = IOError()

        original_copy_helper_cmd = testutils.get_wrapped_function(
            self.writer._copy_helper_cmd)

        self.assertRaises(
            IOError, original_copy_helper_cmd, self.writer, mock_ssh)

        self._sftp.put.assert_not_called()
        self._sftp.close.assert_called_once()

    @mock.patch('paramiko.SSHClient')
    def test__connect_ssh(self, mock_ssh_client):
        mock_ssh_client.return_value = self._ssh

        original_connect_ssh = testutils.get_wrapped_function(
            self.writer._connect_ssh)

        result = original_connect_ssh(self.writer)

        self._ssh.set_missing_host_key_policy.assert_called_once_with(mock.ANY)
        self._ssh.connect.assert_called_once_with(
            hostname=self.writer._ip,
            port=self.writer._port,
            username=self.writer._username,
            pkey=self.writer._pkey,
            password=self.writer._password
        )
        self.assertEqual(result, self._ssh)

    @mock.patch('paramiko.SSHClient')
    def test__connect_ssh_with_exception(self, mock_ssh_client):
        mock_ssh_client.return_value = self._ssh
        self._ssh.connect.side_effect = CoriolisTestException()

        original_connect_ssh = testutils.get_wrapped_function(
            self.writer._connect_ssh)

        self.assertRaises(CoriolisTestException, original_connect_ssh,
                          self.writer)

        self._ssh.close.assert_called_once()


class HTTPBackupWriterImplTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis HTTPBackupWriterImpl class."""

    def setUp(self):
        super(HTTPBackupWriterImplTestCase, self).setUp()
        self.path = "path/test_path"
        self.disk_id = mock.sentinel.disk_id
        self.info = {
            "ip": mock.sentinel.ip,
            "port": mock.sentinel.port,
            "client_crt": mock.sentinel.client_crt,
            "client_key": mock.sentinel.client_key,
            "ca_crt": mock.sentinel.ca_crt,
            "id": mock.sentinel.id
        }
        self.writer = backup_writers.HTTPBackupWriterImpl(
            self.path, self.disk_id)

    def test__set_info(self):
        self.writer._set_info(self.info)
        self.assertEqual(self.writer._ip, self.info["ip"])
        self.assertEqual(self.writer._port, self.info["port"])
        self.assertEqual(self.writer._crt, self.info["client_crt"])
        self.assertEqual(self.writer._key, self.info["client_key"])
        self.assertEqual(self.writer._ca, self.info["ca_crt"])
        self.assertEqual(self.writer._id, self.info["id"])

    def test__set_info_missing_info(self):
        self.assertRaises(exception.CoriolisException,
                          self.writer._set_info, {})

    def test__uri(self):
        self.writer._ip = self.info["ip"]
        self.writer._port = self.info["port"]
        self.writer._path = self.path

        result = self.writer._uri

        self.assertEqual(result, "https://%s:%s/api/v1/%s" % (
            self.writer._ip, self.writer._port, self.writer._path))

    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_ensure_session')
    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_uri')
    @mock.patch.object(backup_writers, 'CONF')
    @mock.patch('requests.Session')
    def test__acquire(self, mock_session_class, mock_conf, mock_uri,
                      mock_ensure_session):
        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.content = 'OK'

        mock_session = mock_session_class.return_value
        mock_session.get.return_value = mock_response

        self.writer._session = mock_session

        original_acquire = testutils.get_wrapped_function(self.writer._acquire)

        original_acquire(self.writer)

        mock_ensure_session.assert_called_once()
        mock_session.get.assert_called_once_with(
            f"{mock_uri}/acquire",
            headers={"X-Client-Token": self.writer._id},
            timeout=mock_conf.default_requests_timeout)
        mock_response.raise_for_status.assert_called_once()

    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_ensure_session')
    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_uri')
    @mock.patch.object(backup_writers, 'CONF')
    @mock.patch('requests.Session')
    def test__release(self, mock_session_class, mock_conf, mock_uri,
                      mock_ensure_session):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.content = 'OK'

        mock_session = mock_session_class.return_value
        mock_session.get.return_value = mock_response

        self.writer._session = mock_session

        original_release = testutils.get_wrapped_function(self.writer._release)

        original_release(self.writer)

        mock_ensure_session.assert_called_once()
        mock_session.get.assert_called_once_with(
            f"{mock_uri}/release",
            headers={"X-Client-Token": self.writer._id},
            timeout=mock_conf.default_requests_timeout)
        mock_response.raise_for_status.assert_called_once()

    @mock.patch.object(provider_utils, 'ProviderSession')
    def test__init_session(self, mock_session_class):
        self.writer._session = mock.Mock(return_value=None)
        self.writer._crt = self.info["client_crt"]
        self.writer._key = self.info["client_key"]
        self.writer._ca = self.info["ca_crt"]

        self.writer._init_session()

        mock_session_class.assert_called_once_with()
        self.writer._session.close.assert_not_called()

        self.assertEqual(self.writer._session, mock_session_class.return_value)
        self.assertEqual(self.writer._session.cert,
                         (self.writer._crt, self.writer._key))
        self.assertEqual(self.writer._session.verify, self.writer._ca)

    @mock.patch.object(provider_utils, 'ProviderSession')
    def test__init_session_exists(self, mock_session_class):
        self.writer._session = mock_session_class.return_value
        self.writer._crt = self.info["client_crt"]
        self.writer._key = self.info["client_key"]
        self.writer._ca = self.info["ca_crt"]

        self.writer._init_session()

        self.writer._session.close.assert_called_once()
        mock_session_class.assert_called_once_with()

        self.assertEqual(self.writer._session, mock_session_class.return_value)
        self.assertEqual(self.writer._session.cert,
                         (self.writer._crt, self.writer._key))
        self.assertEqual(self.writer._session.verify, self.writer._ca)

    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_init_session')
    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_acquire')
    @mock.patch.object(backup_writers, 'eventlet')
    def test__open(self, mock_eventlet, mock_acquire, mock_init_session):
        self.writer._compressor_count = None

        self.writer._open()

        mock_init_session.assert_called_once()
        mock_acquire.assert_called_once()
        mock_eventlet.spawn.assert_has_calls(
            [mock.call(self.writer._sender),
             mock.call(self.writer._compressor)])

        self.assertEqual(self.writer._compressor_count, 1)

    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_init_session')
    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_acquire')
    @mock.patch.object(backup_writers, 'eventlet')
    def test__open_compressor_count_not_none_or_zero(
            self, mock_eventlet, mock_acquire, mock_init_session):
        self.writer._compressor_count = 2

        self.writer._open()

        self.assertEqual(
            len(self.writer._compressor_evt), self.writer._compressor_count)
        self.assertEqual(mock_eventlet.spawn.call_count, 3)

    def test_seek(self):
        self.writer.seek(mock.sentinel.offset)
        self.assertEqual(self.writer._offset, mock.sentinel.offset)

    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_init_session')
    def test__ensure_session(self, mock_init_session):
        self.writer._session = None

        self.writer._ensure_session()
        mock_init_session.assert_called_once()

    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_init_session')
    def test__ensure_session_exists(self, mock_init_session):
        self.writer._session = mock.MagicMock()
        self.writer._write_error = True

        self.writer._ensure_session()
        mock_init_session.assert_called_once()

    @mock.patch('coriolis.data_transfer.compression_proxy')
    def test__compressor(self, mock_compression_proxy):
        self.writer._comp_q = mock.MagicMock()
        self.writer._sender_q = mock.MagicMock()
        self.writer._compress_transfer = True

        self.writer._comp_q.get.return_value = {
            "offset": mock.sentinel.offset,
            "data": mock.sentinel.data}

        mock_compression_proxy.side_effect = [
            (mock.sentinel.data, True),
            CoriolisTestException()]

        self.assertRaises(CoriolisTestException, self.writer._compressor)

        self.assertEqual(self.writer._comp_q.get.call_count, 2)
        mock_compression_proxy.assert_called_with(
            self.writer._comp_q.get.return_value["data"],
            backup_writers.constants.COMPRESSION_FORMAT_GZIP)
        self.writer._sender_q.put.assert_called_once_with(
            {'encoding': 'gzip',
             'offset': mock.sentinel.offset,
             'chunk': mock.sentinel.data})
        self.assertEqual(self.writer._comp_q.task_done.call_count, 2)

    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_ensure_session')
    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_uri')
    @mock.patch.object(backup_writers, 'CONF')
    def test__sender(self, mock_conf, mock_uri, mock_ensure_session):
        self.writer._session = mock.MagicMock()
        self.writer._sender_q = mock.MagicMock()
        self.writer._sender_q.get.return_value = {
            "offset": mock.sentinel.offset,
            "data": mock.sentinel.data,
            "chunk": 'compressed_data',
            "encoding": "gzip"
        }

        mock_response = mock.MagicMock()
        mock_response.status_code = 200
        mock_response.content = "OK"
        mock_response.raise_for_status.side_effect = [None, BaseException()]
        self.writer._session.post.return_value = mock_response

        with self.assertLogs('coriolis.providers.backup_writers',
                             level=logging.ERROR):
            self.assertRaises(BaseException, self.writer._sender)

        expected_headers = {
            "X-Write-Offset": str(mock.sentinel.offset),
            "X-Client-Token": self.writer._id,
            "content-encoding": "gzip",
        }

        self.writer._session.post.assert_called_with(
            mock_uri,
            headers=expected_headers,
            data='compressed_data',
            timeout=mock_conf.default_requests_timeout)

        mock_ensure_session.assert_called()
        mock_response.raise_for_status.assert_called()
        self.assertEqual(self.writer._sender_q.task_done.call_count, 2)
        self.assertEqual(self.writer._write_error, False)
        self.assertIsInstance(self.writer._exception, BaseException)

    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_ensure_session')
    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_uri')
    @mock.patch.object(backup_writers, 'CONF')
    def test__sender_with_exception(self, mock_conf, mock_uri,
                                    mock_ensure_session):
        self.writer._session = mock.MagicMock()
        self.writer._sender_q = mock.MagicMock()
        mock_response = mock.MagicMock()
        mock_response.raise_for_status.side_effect = CoriolisTestException()
        self.writer._session.post.return_value = mock_response

        self.writer._sender_q.get.return_value = {
            "offset": mock.sentinel.offset,
            "data": mock.sentinel.data,
            "chunk": 'compressed_data',
        }

        with self.assertLogs('coriolis.providers.backup_writers',
                             level=logging.WARNING):
            self.assertRaises(CoriolisTestException, self.writer._sender)

        expected_headers = {
            "X-Write-Offset": str(mock.sentinel.offset),
            "X-Client-Token": self.writer._id,
        }

        self.writer._session.post.assert_called_with(
            mock_uri,
            headers=expected_headers,
            data='compressed_data',
            timeout=mock_conf.default_requests_timeout)

        mock_ensure_session.assert_called()
        self.writer._sender_q.task_done.assert_called_once()
        self.assertEqual(self.writer._write_error, True)

    def test_write(self):
        self.writer._closing = False
        self.writer._exception = None

        self.writer._comp_q = mock.MagicMock()
        self.writer._sender_q = mock.MagicMock()
        self.writer._offset = 0

        original_write = testutils.get_wrapped_function(self.writer.write)

        original_write(self.writer, 'test_data')

        expected_payload = {
            "offset": 0,
            "data": 'test_data',
        }
        self.writer._comp_q.put.assert_called_once_with(expected_payload)
        self.assertEqual(self.writer._offset, len('test_data'))

    def test_write_with_closing(self):
        self.writer._closing = True

        original_write = testutils.get_wrapped_function(self.writer.write)

        self.assertRaises(exception.CoriolisException, original_write,
                          self.writer, mock.sentinel.data)

    def test_write_with_exception(self):
        self.writer._exception = exception.CoriolisException()

        original_write = testutils.get_wrapped_function(self.writer.write)

        self.assertRaises(exception.CoriolisException, original_write,
                          self.writer, mock.sentinel.data)

    @mock.patch('time.sleep')
    def test__wait_for_queues(self, mock_sleep):
        mock_sleep.side_effect = CoriolisTestException()

        self.writer._comp_q = mock.MagicMock()
        self.writer._sender_q = mock.MagicMock()
        self.writer._exception = None

        self.writer._comp_q.unfinished_tasks = True
        self.writer._sender_q.unfinished_tasks = True

        self.assertRaises(CoriolisTestException, self.writer._wait_for_queues)

        self.assertEqual(mock_sleep.call_count, 1)

    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_wait_for_queues')
    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_release')
    def test_close(self, mock_release, mock_wait_for_queues):
        self.writer._closing = True

        self.writer._session = mock.MagicMock()
        self.writer._sender_evt = mock.MagicMock()
        self.writer._comp_q = mock.MagicMock()
        self.writer._compressor_evt = [mock.MagicMock(), mock.MagicMock()]

        self.writer.close()

        mock_wait_for_queues.assert_called_once()
        mock_release.assert_called_once()
        self.assertIsNone(self.writer._session)
        self.assertIsNone(self.writer._sender_evt)
        self.assertIsNone(self.writer._compressor_evt)

    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_wait_for_queues')
    @mock.patch.object(backup_writers.HTTPBackupWriterImpl, '_release')
    def test_close_with_exception(self, mock_release, mock_wait_for_queues):
        self.writer._exception = exception.CoriolisException()
        mock_release.side_effect = Exception()

        with self.assertLogs('coriolis.providers.backup_writers',
                             level=logging.ERROR):
            self.assertRaises(exception.CoriolisException, self.writer.close)


class HTTPBackupWriterBootstrapperTestcase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis HTTPBackupWriterBootstrapper class."""

    @mock.patch('coriolis.providers.backup_writers.paramiko.SSHClient')
    def setUp(self, mock_ssh_client):
        super(HTTPBackupWriterBootstrapperTestcase, self).setUp()
        self.writer_port = 22
        self.ssh_conn_info = {
            "ip": mock.sentinel.ip,
            "port": mock.sentinel.port,
            "pkey": None,
            "username": mock.sentinel.username,
            "password": mock.sentinel.pkey,
        }
        self._ssh = mock_ssh_client.return_value

        self.bootstrapper = backup_writers.HTTPBackupWriterBootstrapper(
            self.ssh_conn_info, self.writer_port)

    @mock.patch.object(backup_writers, '_check_deserialize_key')
    @mock.patch.object(
        backup_writers.HTTPBackupWriterBootstrapper, '_connect_ssh'
    )
    def test__init__missing_required_params(self, mock_connect_ssh,
                                            mock_deserialize_key):
        # Remove the writer_port to test the missing required.
        self.ssh_conn_info.pop("ip")

        self.assertRaises(exception.CoriolisException,
                          backup_writers.HTTPBackupWriterBootstrapper,
                          self.ssh_conn_info, self.writer_port)

    @mock.patch.object(backup_writers, '_check_deserialize_key')
    @mock.patch.object(
        backup_writers.HTTPBackupWriterBootstrapper, '_connect_ssh'
    )
    def test__init__missing_password(self, mock_connect_ssh,
                                     mock_deserialize_key):
        self.ssh_conn_info.pop("password")
        self.assertRaises(exception.CoriolisException,
                          backup_writers.HTTPBackupWriterBootstrapper,
                          self.ssh_conn_info, self.writer_port)

    @mock.patch.object(backup_writers, '_check_deserialize_key')
    @mock.patch.object(
        backup_writers.HTTPBackupWriterBootstrapper, '_connect_ssh'
    )
    def test__init__with_pkey(self, mock_connect_ssh, mock_deserialize_key):
        self.ssh_conn_info["pkey"] = mock.sentinel.pkey

        writer = backup_writers.HTTPBackupWriterBootstrapper(
            self.ssh_conn_info, self.writer_port)

        mock_deserialize_key.assert_called_once_with(mock.sentinel.pkey)
        self.assertEqual(writer._pkey, mock_deserialize_key.return_value)

    @mock.patch('paramiko.SSHClient')
    def test__connect_ssh(self, mock_ssh_client):
        mock_ssh = mock_ssh_client.return_value

        original_connect_ssh = testutils.get_wrapped_function(
            self.bootstrapper._connect_ssh)

        result = original_connect_ssh(self.bootstrapper)

        mock_ssh.set_missing_host_key_policy.assert_called_once_with(mock.ANY)
        mock_ssh.connect.assert_called_once_with(
            hostname=self.bootstrapper._ip,
            port=self.bootstrapper._port,
            username=self.bootstrapper._username,
            pkey=self.bootstrapper._pkey,
            password=self.bootstrapper._password
        )
        self.assertEqual(result, mock_ssh)

    @mock.patch('paramiko.SSHClient')
    def test__connect_ssh_with_exception(self, mock_ssh_client):
        mock_ssh_client.return_value = self._ssh
        self._ssh.connect.side_effect = [None, CoriolisTestException()]

        bootstrapper = backup_writers.HTTPBackupWriterBootstrapper(
            self.ssh_conn_info, self.writer_port)

        original = testutils.get_wrapped_function(bootstrapper._connect_ssh)

        self.assertRaises(CoriolisTestException, original, bootstrapper)

        self._ssh.close.assert_called_once()

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    def test__inject_dport_allow_rule(self, mock_exec_ssh_cmd):
        self.bootstrapper._inject_dport_allow_rule(self._ssh)

        mock_exec_ssh_cmd.assert_called_once_with(
            self._ssh,
            "sudo nft insert rule ip filter INPUT tcp dport %(port)s counter "
            "accept || "
            "sudo iptables -I INPUT -p tcp --dport %(port)s -j ACCEPT" % {
                "port": self.writer_port},
            get_pty=True)

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    def test__inject_dport_allow_rule_with_exception(self, mock_exec_ssh_cmd):
        mock_exec_ssh_cmd.side_effect = exception.CoriolisException()

        with self.assertLogs('coriolis.providers.backup_writers',
                             level=logging.WARN):
            self.bootstrapper._inject_dport_allow_rule(self._ssh)

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    def test__add_firewalld_port(self, mock_exec_ssh_cmd):
        self.bootstrapper._add_firewalld_port(self._ssh)

        mock_exec_ssh_cmd.assert_called_once_with(
            self._ssh,
            "sudo firewall-cmd --add-port=%s/tcp" %
            self.writer_port, get_pty=True)

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    def test__add_firewalld_port_with_exception(self, mock_exec_ssh_cmd):
        mock_exec_ssh_cmd.side_effect = exception.CoriolisException()

        with self.assertLogs('coriolis.providers.backup_writers',
                             level=logging.WARN):
            self.bootstrapper._add_firewalld_port(self._ssh)

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    def test__change_binary_se_context(self, mock_exec_ssh_cmd):
        self.bootstrapper._change_binary_se_context(self._ssh)

        mock_exec_ssh_cmd.assert_called_once_with(
            self._ssh,
            'sudo chcon -t bin_t /usr/bin/coriolis-writer', get_pty=True)

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    def test__change_binary_se_context_with_exception(self, mock_exec_ssh_cmd):
        mock_exec_ssh_cmd.side_effect = exception.CoriolisException()

        with self.assertLogs('coriolis.providers.backup_writers',
                             level=logging.WARN):
            self.bootstrapper._change_binary_se_context(self._ssh)

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    def test__copy_writer_file(self, mock_exec_ssh_cmd):
        mock_sftp = mock.MagicMock()
        self._ssh.open_sftp.return_value = mock_sftp

        original_copy_writer_file = testutils.get_wrapped_function(
            self.bootstrapper._copy_writer)

        original_copy_writer_file(self.bootstrapper, self._ssh)

        mock_sftp.stat.assert_called_once_with(self.bootstrapper._writer_cmd)
        mock_exec_ssh_cmd.assert_not_called()
        mock_sftp.close.assert_called_once()

    @mock.patch.object(backup_writers.utils, 'get_resources_bin_dir')
    @mock.patch('coriolis.utils.exec_ssh_cmd')
    def test__copy_writer_file_does_not_exist(
            self, mock_exec_ssh_cmd, mock_get_resources_bin_dir):
        local_path = os.path.join(
            mock_get_resources_bin_dir.return_value,
            backup_writers._CORIOLIS_HTTP_WRITER_CMD)

        remote_tmp_path = os.path.join(
            "/tmp", backup_writers._CORIOLIS_HTTP_WRITER_CMD)

        mock_sftp = mock.MagicMock()
        self._ssh.open_sftp.return_value = mock_sftp
        mock_sftp.stat.side_effect = IOError(
            backup_writers.errno.ENOENT, 'No such file or directory')

        original_copy_writer_file = testutils.get_wrapped_function(
            self.bootstrapper._copy_writer)

        original_copy_writer_file(self.bootstrapper, self._ssh)

        mock_sftp.put.assert_called_once_with(local_path, remote_tmp_path)
        mock_exec_ssh_cmd.assert_has_calls([
            mock.call(
                self._ssh, "sudo mv %s %s" % (
                    remote_tmp_path, self.bootstrapper._writer_cmd),
                get_pty=True
            ),
            mock.call(
                self._ssh, "sudo chmod +x %s" % self.bootstrapper._writer_cmd,
                get_pty=True)])
        mock_sftp.close.assert_called_once()

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    def test__copy_writer_stat_error(self, mock_exec_ssh_cmd):
        mock_sftp = mock.MagicMock()
        self._ssh.open_sftp.return_value = mock_sftp
        mock_sftp.stat.side_effect = IOError(
            backup_writers.errno.EACCES, 'Permission denied')

        original_copy_writer_file = testutils.get_wrapped_function(
            self.bootstrapper._copy_writer)
        self.assertRaises(IOError, original_copy_writer_file,
                          self.bootstrapper, self._ssh)

        mock_sftp.put.assert_not_called()
        mock_exec_ssh_cmd.assert_not_called()
        mock_sftp.close.assert_called_once()

    @mock.patch('coriolis.utils.read_ssh_file')
    @mock.patch('coriolis.utils.exec_ssh_cmd')
    def test__fetch_remote_file(self, mock_exec_ssh_cmd, mock_read_ssh_file):
        with mock.patch('builtins.open', mock.mock_open()) as data:
            self.bootstrapper._fetch_remote_file(
                self._ssh, mock.sentinel.remote_file, mock.sentinel.local_file)
            data.assert_called_once_with(mock.sentinel.local_file, 'wb')
            mock_exec_ssh_cmd.assert_called_once_with(
                self._ssh, "sudo chmod +r %s" % mock.sentinel.remote_file,
                get_pty=True)

            data.return_value.write.assert_called_once_with(
                mock_read_ssh_file.return_value)

    @mock.patch('coriolis.utils.test_ssh_path')
    @mock.patch('coriolis.utils.exec_ssh_cmd')
    def test__setup_certificates(self, mock_exec_ssh_cmd, mock_test_ssh_path):
        mock_test_ssh_path.return_value = True

        expected_result = {
            "srv_crt": "/etc/coriolis-writer/srv-cert.pem",
            "srv_key": "/etc/coriolis-writer/srv-key.pem",
            "ca_crt": "/etc/coriolis-writer/ca-cert.pem",
            "client_crt": "/etc/coriolis-writer/client-cert.pem",
            "client_key": "/etc/coriolis-writer/client-key.pem"
        }

        result = self.bootstrapper._setup_certificates(self._ssh)

        self.assertEqual(result, expected_result)
        mock_exec_ssh_cmd.assert_not_called()

    @mock.patch('coriolis.utils.test_ssh_path')
    @mock.patch('coriolis.utils.exec_ssh_cmd')
    def test__setup_certificates_no_files_exist(
            self, mock_exec_ssh_cmd, mock_test_ssh_path):
        mock_test_ssh_path.return_value = False

        self.bootstrapper._setup_certificates(self._ssh)

        mock_exec_ssh_cmd.assert_any_call(
            self._ssh, "sudo mkdir -p /etc/coriolis-writer", get_pty=True)
        mock_exec_ssh_cmd.assert_any_call(
            self._ssh,
            "sudo %(writer_cmd)s generate-certificates -output-dir "
            "%(cert_dir)s -certificate-hosts %(extra_hosts)s" % {
                "writer_cmd": self.bootstrapper._writer_cmd,
                "cert_dir": "/etc/coriolis-writer",
                "extra_hosts": self.bootstrapper._ip,
            },
            get_pty=True)

    @mock.patch('coriolis.utils.exec_ssh_cmd')
    def test__read_remote_file_sudo(self, mock_exec_ssh_cmd):
        result = self.bootstrapper._read_remote_file_sudo(
            mock.sentinel.remote_path)

        mock_exec_ssh_cmd.assert_called_once_with(
            self._ssh, "sudo cat %s" % mock.sentinel.remote_path, get_pty=True)
        self.assertEqual(
            result, mock_exec_ssh_cmd.return_value)

    @mock.patch.object(
        backup_writers.HTTPBackupWriterBootstrapper,
        '_change_binary_se_context'
    )
    @mock.patch.object(
        backup_writers.HTTPBackupWriterBootstrapper, '_inject_dport_allow_rule'
    )
    @mock.patch.object(
        backup_writers.HTTPBackupWriterBootstrapper, '_add_firewalld_port'
    )
    @mock.patch('coriolis.utils.create_service')
    def test__init_writer(
            self, mock_create_service, mock_add_firewalld_port,
            mock_inject_dport_allow_rule, mock_change_binary_se_context):
        cert_paths = {
            "ca_crt": mock.sentinel.ca_crt,
            "srv_key": mock.sentinel.srv_key,
            "srv_crt": mock.sentinel.srv_crt,
        }

        self.bootstrapper._init_writer(self._ssh, cert_paths)

        mock_change_binary_se_context.assert_called_once_with(self._ssh)
        mock_create_service.assert_called_once_with(
            self._ssh,
            "%s run -ca-cert %s -key %s -cert %s -listen-port %s" % (
                self.bootstrapper._writer_cmd,
                cert_paths["ca_crt"],
                cert_paths["srv_key"],
                cert_paths["srv_crt"],
                self.bootstrapper._writer_port,
            ),
            backup_writers._CORIOLIS_HTTP_WRITER_CMD,
            start=True,
        )
        mock_inject_dport_allow_rule.assert_called_once_with(self._ssh)
        mock_add_firewalld_port.assert_called_once_with(self._ssh)

    @mock.patch.object(
        backup_writers.HTTPBackupWriterBootstrapper, '_read_remote_file_sudo'
    )
    @mock.patch.object(
        backup_writers.HTTPBackupWriterBootstrapper, '_init_writer'
    )
    @mock.patch.object(
        backup_writers.HTTPBackupWriterBootstrapper, '_setup_certificates'
    )
    @mock.patch.object(
        backup_writers.HTTPBackupWriterBootstrapper, '_copy_writer'
    )
    @mock.patch.object(backup_writers, '_disable_lvm2_lvmetad')
    @mock.patch.object(backup_writers, '_disable_lvm_metad_udev_rule')
    def test_setup_writer(self, mock_disable_lvm_metad_udev,
                          mock_disable_lvm2_lvmetad, mock_copy_writer,
                          mock_setup_certificates, mock_init_writer,
                          mock_read_remote_file_sudo):
        cert_paths = {
            "client_crt": mock.sentinel.client_crt,
            "client_key": mock.sentinel.client_key,
            "ca_crt": mock.sentinel.ca_crt,
        }
        mock_setup_certificates.return_value = cert_paths

        result = self.bootstrapper.setup_writer()

        mock_disable_lvm_metad_udev.assert_called_once_with(self._ssh)
        mock_disable_lvm2_lvmetad.assert_called_once_with(self._ssh)
        mock_copy_writer.assert_called_once_with(self._ssh)
        mock_setup_certificates.assert_called_once_with(self._ssh)
        mock_init_writer.assert_called_once_with(self._ssh, cert_paths)

        expected_result = {
            "ip": self.bootstrapper._ip,
            "port": self.bootstrapper._writer_port,
            "certificates": {
                "client_crt": mock_read_remote_file_sudo.return_value,
                "client_key": mock_read_remote_file_sudo.return_value,
                "ca_crt": mock_read_remote_file_sudo.return_value,
            }
        }
        self.assertEqual(result, expected_result)


class HTTPBackupWriterTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis HTTPBackupWriter class."""

    def setUp(self):
        super(HTTPBackupWriterTestCase, self).setUp()
        self.ip = mock.sentinel.ip
        self.port = mock.sentinel.port
        self.volumes_info = [{
            "disk_id": mock.sentinel.disk_id,
            "volume_dev": mock.sentinel.volume_dev}]
        self.certificates = {
            "client_crt": mock.sentinel.client_crt,
            "client_key": mock.sentinel.client_key,
            "ca_crt": mock.sentinel.ca_crt,
        }

        self.writer = backup_writers.HTTPBackupWriter(
            self.ip, self.port, self.volumes_info, self.certificates)

    def test__init__no_certificates(self):
        self.assertRaises(
            exception.CoriolisException, backup_writers.HTTPBackupWriter,
            self.ip, self.port, mock.sentinel.volumes_info, None)

    def test_from_connection_info(self):
        volumes_info = [{
            "disk_id": mock.sentinel.disk_id,
            "volume_dev": mock.sentinel.volume_dev}]
        certificates = {
            "client_crt": mock.sentinel.client_crt,
            "client_key": mock.sentinel.client_key,
            "ca_crt": mock.sentinel.ca_crt,
        }
        conn_info = {
            "ip": mock.sentinel.ip,
            "port": mock.sentinel.port,
            "certificates": {
                "client_crt": certificates["client_crt"],
                "client_key": certificates["client_key"],
                "ca_crt": certificates["ca_crt"],
            }
        }
        result = backup_writers.HTTPBackupWriter.from_connection_info(
            conn_info, volumes_info)
        self.assertIsInstance(result, backup_writers.HTTPBackupWriter)
        self.assertEqual(result._ip, conn_info["ip"])
        self.assertEqual(result._port, conn_info["port"])
        self.assertEqual(result._volumes_info, volumes_info)
        self.assertEqual(result._certificates, certificates)

    def test_from_connection_info_missing_required_fields(self):
        conn_info = {
            "ip": mock.sentinel.ip,
            "port": mock.sentinel.port,
        }

        self.assertRaises(
            exception.CoriolisException,
            backup_writers.HTTPBackupWriter.from_connection_info,
            conn_info, mock.sentinel.volumes_info)

    def test_from_connection_info_missing_cert_options(self):
        volumes_info = [{
            "disk_id": mock.sentinel.disk_id,
            "volume_dev": mock.sentinel.volume_dev}]
        certificates = {
            "client_crt": mock.sentinel.client_crt,
            "client_key": mock.sentinel.client_key,
            "ca_crt": mock.sentinel.ca_crt,
        }
        conn_info = {
            "ip": mock.sentinel.ip,
            "port": mock.sentinel.port,
            "certificates": {
                "client_crt": certificates.get("client_crt"),
                "client_key": certificates.get("client_key"),
            }
        }

        self.assertRaises(
            exception.CoriolisException,
            backup_writers.HTTPBackupWriter.from_connection_info,
            conn_info, volumes_info)

    def test__del__(self):
        # Create a temporary directory to test the deletion
        tmp_dir = tempfile.mkdtemp()
        self.writer._crt_dir = tmp_dir

        del self.writer

        # Check that the directory no longer exists
        self.assertFalse(os.path.isdir(tmp_dir))

    @mock.patch('shutil.rmtree')
    def test__del_with_exception(self, mock_rmtree):
        tmp_dir = tempfile.mkdtemp()
        self.writer._crt_dir = tmp_dir
        mock_rmtree.side_effect = CoriolisTestException()

        del self.writer

        self.assertTrue(os.path.isdir(tmp_dir))

    @mock.patch.object(backup_writers.utils, 'wait_for_port_connectivity')
    def test__wait_for_conn(self, mock_wait_for_port_connectivity):
        self.writer._wait_for_conn()

        mock_wait_for_port_connectivity.assert_called_once_with(
            self.writer._ip, self.writer._port)

    def test__write_cert_files_no_certificates(self):
        self.writer._certificates = None

        self.assertRaises(
            exception.CoriolisException, self.writer._write_cert_files)

    def test__write_cert_files_exists(self):
        self.writer._cert_paths = {
            "client_crt": mock.sentinel.client_crt,
            "client_key": mock.sentinel.client_key,
            "ca_crt": mock.sentinel.ca_crt,
        }

        result = self.writer._write_cert_files()
        self.assertEqual(result, self.writer._cert_paths)

    @mock.patch('builtins.open')
    @mock.patch('tempfile.mkstemp')
    def test__write_cert_files_write_new_certs(self, mock_mkstemp, mock_open):
        mock_mkstemp.side_effect = [
            (0, mock.sentinel.client_crt),
            (1, mock.sentinel.client_key),
            (2, mock.sentinel.ca_crt)]
        self.writer._certificates = self.certificates

        result = self.writer._write_cert_files()

        expected_cert_paths = {
            "client_crt": self.writer._cert_paths["client_crt"],
            "client_key": self.writer._cert_paths["client_key"],
            "ca_crt": self.writer._cert_paths["ca_crt"],
        }
        self.assertEqual(result, expected_cert_paths)

        mock_open.assert_has_calls([
            mock.call(self.writer._cert_paths["client_crt"], "w"),
            mock.call(self.writer._cert_paths["client_key"], "w"),
            mock.call(self.writer._cert_paths["ca_crt"], "w")], any_order=True)

    @mock.patch.object(backup_writers, 'HTTPBackupWriterImpl')
    @mock.patch.object(backup_writers.HTTPBackupWriter, '_wait_for_conn')
    @mock.patch.object(backup_writers.HTTPBackupWriter, '_write_cert_files')
    def test__get_impl(self, mock_write_cert_files, mock_wait_for_conn,
                       mock_http_backup_writer_impl):
        result = self.writer._get_impl(mock.sentinel.volume_dev,
                                       mock.sentinel.disk_id)

        self.assertEqual(result, mock_http_backup_writer_impl.return_value)

        mock_write_cert_files.assert_called_once_with()
        mock_wait_for_conn.assert_called_once_with()
        mock_http_backup_writer_impl.assert_called_once_with(
            mock.sentinel.volume_dev, mock.sentinel.disk_id,
            compressor_count=self.writer._compressor_count,
            compress_transfer=backup_writers.CONF.compress_transfers)
        mock_http_backup_writer_impl.return_value._set_info.\
            assert_called_once_with({
                "ip": self.ip,
                "port": self.port,
                "client_crt": mock_write_cert_files.return_value["client_crt"],
                "client_key": mock_write_cert_files.return_value["client_key"],
                "ca_crt": mock_write_cert_files.return_value["ca_crt"],
                "id": self.writer._id,
            })
