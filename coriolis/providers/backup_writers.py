# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import abc
import contextlib
import errno
import os
import threading

from oslo_config import cfg
from oslo_log import log as logging
import paramiko
from six import with_metaclass

from coriolis import data_transfer
from coriolis import exception
from coriolis import utils


CONF = cfg.CONF
opts = [
    cfg.BoolOpt('compress_transfers',
                default=True,
                help='Use compression if possible during disk transfers'),
]
CONF.register_opts(opts)

LOG = logging.getLogger(__name__)


class BaseBackupWriterImpl(with_metaclass(abc.ABCMeta)):
    def __init__(self, path, disk_id):
        self._path = path
        self._disk_id = disk_id

    @abc.abstractmethod
    def _open(self):
        pass

    def _handle_exception(self, ex):
        LOG.exception(ex)

    @abc.abstractmethod
    def seek(self, pos):
        pass

    @abc.abstractmethod
    def truncate(self, size):
        pass

    @abc.abstractmethod
    def write(self, data):
        pass

    @abc.abstractmethod
    def close(self):
        pass


class BaseBackupWriter(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def _get_impl(self, path, disk_id):
        pass

    @contextlib.contextmanager
    def open(self, path, disk_id):
        impl = None
        try:
            impl = self._get_impl(path, disk_id)
            impl._open()
            yield impl
        except Exception as ex:
            if impl:
                impl._handle_exception(ex)
            raise
        finally:
            if impl:
                impl.close()


class FileBackupWriterImpl(BaseBackupWriterImpl):
    def __init__(self, path, disk_id):
        self._file = None
        super(FileBackupWriterImpl, self).__init__(path, disk_id)

    def _open(self):
        # Create file if it doesn't exist
        open(self._path, 'ab+').close()
        self._file = open(self._path, 'rb+')

    def seek(self, pos):
        self._file.seek(pos)

    def truncate(self, size):
        self._file.truncate(size)

    def write(self, data):
        self._file.write(data)

    def close(self):
        self._file.close()
        self._file = None


class FileBackupWriter(BaseBackupWriter):
    def _get_impl(self, path, disk_id):
        return FileBackupWriterImpl(path, disk_id)


class SSHBackupWriterImpl(BaseBackupWriterImpl):
    def __init__(self, path, disk_id, compress_transfer=None):
        self._msg_id = None
        self._stdin = None
        self._stdout = None
        self._stderr = None
        self._offset = None
        self._ssh = None
        self._compress_transfer = compress_transfer
        if self._compress_transfer is None:
            self._compress_transfer = CONF.compress_transfers
        super(SSHBackupWriterImpl, self).__init__(path, disk_id)

    def _set_ssh_client(self, ssh):
        self._ssh = ssh

    @utils.retry_on_error()
    def _exec_helper_cmd(self):
        self._msg_id = 0
        self._offset = 0
        self._stdin, self._stdout, self._stderr = self._ssh.exec_command(
            "chmod +x write_data && sudo ./write_data")

    def _encode_data(self, content):
        msg = data_transfer.encode_data(
            self._msg_id, self._path,
            self._offset, content,
            compress=self._compress_transfer)

        LOG.debug(
            "Guest path: %(path)s, offset: %(offset)d, content len: "
            "%(content_len)d, msg len: %(msg_len)d",
            {"path": self._path,
             "offset": self._offset,
             "content_len": len(content),
             "msg_len": len(msg)})
        return msg

    def _encode_eod(self):
        msg = data_transfer.encode_eod(self._msg_id)
        LOG.debug("EOD message len: %d", len(msg))
        return msg

    @utils.retry_on_error()
    def _send_msg(self, data):
        self._msg_id += 1
        self._stdin.write(data)
        self._stdin.flush()
        self._stdout.read(4)

    def _open(self):
        self._exec_helper_cmd()

    def seek(self, pos):
        self._offset = pos

    def truncate(self, size):
        pass

    def write(self, data):
        self._send_msg(self._encode_data(data))
        self._offset += len(data)

    def close(self):
        if self._ssh:
            self._send_msg(self._encode_eod())
            self._ssh.close()
            self._ssh = None

    def _handle_exception(self, ex):
        super(SSHBackupWriterImpl, self)._handle_exception(ex)

        ret_val = None
        # if the application is still running on the other side,
        # recv_exit_status() will block. Check that we have an
        # exit status before retrieving it
        if self._stdout.channel.exit_status_ready():
            ret_val = self._stdout.channel.recv_exit_status()

        # Don't send a message via ssh on exception
        self._ssh.close()
        self._ssh = None

        if ret_val:
            # TODO(alexpilotti): map error codes to error messages
            raise exception.CoriolisException(
                "An exception occurred while writing data on target. "
                "Exit code: %s" % ret_val)
        else:
            raise exception.CoriolisException(
                "An exception occurred while writing data on target: %s" %
                ex)


class SSHBackupWriter(BaseBackupWriter):
    def __init__(self, ip, port, username, pkey, password, volumes_info):
        self._ip = ip
        self._port = port
        self._username = username
        self._pkey = pkey
        self._password = password
        self._volumes_info = volumes_info
        self._ssh = None
        self._lock = threading.Lock()

    def _get_impl(self, path, disk_id):
        ssh = self._connect_ssh()

        matching_devs = [
            v for v in self._volumes_info if v["disk_id"] == disk_id]

        if not matching_devs:
            base_msg = (
                "Could not locate disk with ID '%s' in volumes_info" % disk_id)
            LOG.error("%s: %s", base_msg, self._volumes_info)
            raise exception.CoriolisException(base_msg)
        elif len(matching_devs) > 1:
            base_msg = (
                "Multiple disks with ID '%s' in volumes_info" % disk_id)
            LOG.error("%s: %s", base_msg, self._volumes_info)
            raise exception.CoriolisException(base_msg)

        path = matching_devs[0]["volume_dev"]
        impl = SSHBackupWriterImpl(path, disk_id)

        self._copy_helper_cmd(ssh)
        impl._set_ssh_client(ssh)
        return impl

    @utils.retry_on_error()
    def _copy_helper_cmd(self, ssh):
        with self._lock:
            sftp = ssh.open_sftp()
            local_path = os.path.join(
                utils.get_resources_dir(), 'write_data')
            try:
                # Check if the remote file already exists
                sftp.stat('write_data')
            except IOError as ex:
                if ex.errno != errno.ENOENT:
                    raise
                sftp.put(local_path, 'write_data')
            finally:
                sftp.close()

    @utils.retry_on_error(sleep_seconds=30)
    def _connect_ssh(self):
        LOG.info("Connecting to SSH host: %(ip)s:%(port)s" %
                 {"ip": self._ip, "port": self._port})
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(
                hostname=self._ip,
                port=self._port,
                username=self._username,
                pkey=self._pkey,
                password=self._password)
        except:
            # No need to log the error as we just raise
            ssh.close()
            raise
        return ssh
