# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import abc
import contextlib
import os

from oslo_log import log as logging
import paramiko

from coriolis import data_transfer
from coriolis import exception
from coriolis import utils

LOG = logging.getLogger(__name__)


class BaseBackupWriter(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def _open(self):
        pass

    @contextlib.contextmanager
    def open(self, path, disk_id):
        self._path = path
        self._disk_id = disk_id
        self._open()
        try:
            yield self
        finally:
            self.close()

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


class FileBackupWriter(BaseBackupWriter):
    def _open(self):
        # Create file if it doesnt exist
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


class SSHBackupWriter(BaseBackupWriter):
    def __init__(self, ip, port, username, pkey, password, volumes_info):
        self._ip = ip
        self._port = port
        self._username = username
        self._pkey = pkey
        self._password = password
        self._volumes_info = volumes_info
        self._ssh = None

    @contextlib.contextmanager
    def open(self, path, disk_id):
        self._path = path
        self._disk_id = disk_id
        self._open()
        try:
            yield self
            # Don't send a message via ssh on exception
            self.close()
        except:
            self._ssh.close()
            raise

    @utils.retry_on_error()
    def _connect_ssh(self):
        LOG.info("Connecting to SSH host: %(ip)s:%(port)s" %
                 {"ip": self._ip, "port": self._port})
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._ssh.connect(
            hostname=self._ip,
            port=self._port,
            username=self._username,
            pkey=self._pkey,
            password=self._password)

    @utils.retry_on_error()
    def _copy_helper_cmd(self):
        sftp = self._ssh.open_sftp()
        local_path = os.path.join(
            utils.get_resources_dir(), 'write_data')
        sftp.put(local_path, 'write_data')
        sftp.close()

    @utils.retry_on_error()
    def _exec_helper_cmd(self):
        self._msg_id = 0
        self._offset = 0
        self._stdin, self._stdout, self._stderr = self._ssh.exec_command(
            "chmod +x write_data && sudo ./write_data")

    def _encode_data(self, content):
        path = [v for v in self._volumes_info
                if v["disk_id"] == self._disk_id][0]["volume_dev"]

        msg = data_transfer.encode_data(
            self._msg_id, path, self._offset, content)

        LOG.debug(
            "Guest path: %(path)s, offset: %(offset)d, content len: "
            "%(content_len)d, msg len: %(msg_len)d",
            {"path": path, "offset": self._offset, "content_len": len(content),
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
        out_msg_id = self._stdout.read(4)

    def _open(self):
        self._connect_ssh()
        self._copy_helper_cmd()
        self._exec_helper_cmd()

    def seek(self, pos):
        self._offset = pos

    def truncate(self, size):
        pass

    def write(self, data):
        self._send_msg(self._encode_data(data))
        self._offset += len(data)

    def close(self):
        self._send_msg(self._encode_eod())
        ret_val = self._stdout.channel.recv_exit_status()
        if ret_val:
            raise exception.CoriolisException(
                "An exception occurred while writing data on target. "
                "Error code: %s" % ret_val)
        self._ssh.close()
