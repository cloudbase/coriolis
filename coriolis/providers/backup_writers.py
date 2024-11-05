# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import abc
import contextlib
import copy
import datetime
import errno
import os
import shutil
import tempfile
import threading
import time
import uuid

import eventlet
from oslo_config import cfg
from oslo_log import log as logging
import paramiko
from six import with_metaclass

from coriolis import constants
from coriolis import data_transfer
from coriolis import exception
from coriolis.providers import provider_utils
from coriolis import utils

CONF = cfg.CONF
opts = [
    cfg.BoolOpt('compress_transfers',
                default=True,
                help='Use compression if possible during disk transfers'),
]
CONF.register_opts(opts)
_CORIOLIS_HTTP_WRITER_CMD = "coriolis-writer"

LOG = logging.getLogger(__name__)
BACKUP_WRITER_SSH = "ssh_backup_writer"
BACKUP_WRITER_HTTP = "http_backup_writer"
BACKUP_WRITER_FILE = "file_backup_writer"

BACKUP_WRITERS = [
    BACKUP_WRITER_SSH,
    BACKUP_WRITER_HTTP,
    BACKUP_WRITER_FILE
]

_WRITER_ERR_MAP = {
    -1: "ERR_MORE_MSG",
    0: "ERR_DONE",
    1: "ERR_READ_MSG_SIZE",
    2: "ERR_MSG_SIZE",
    3: "ERR_OPEN_FILE",
    4: "ERR_DATA",
    5: "ERR_IO_OPEN",
    6: "ERR_IO_SEEK",
    7: "ERR_IO_WRITE",
    8: "ERR_IO_CLOSE",
    9: "ERR_NO_MEM",
    10: "ERR_INVALID_ARGS",
    11: "ERR_READ_MSG_ID",
    12: "ERR_MSG_SIZE_INFLATED",
    13: "ERR_ZLIB",
    14: "ERR_WRITE_MSG_ID",
    15: "ERR_OUT_OF_BOUDS",
}


def _disable_lvm2_lvmetad(ssh):
    """Disables lvm2-lvmetad service. This service is responsible
    for automatically activating LVM2 volume groups when a disk is
    attached or when a volume group is created. During disk replication
    this service needs to be disabled.
    """
    cfg = "/etc/lvm/lvm.conf"
    if utils.test_ssh_path(ssh, cfg):
        utils.exec_ssh_cmd(
            ssh,
            'sudo sed -i "s/use_lvmetad.*=.*1/use_lvmetad = 0/g" '
            '%s' % cfg, get_pty=True)
        # NOTE: lvm2-lvmetad is the name of the lvmetad service
        # on both debian and RHEL based systems. It needs to be stopped
        # before we begin disk replication. We disable it in the config
        # just in case some other process starts the daemon later on, as
        # a dependency. As the service may not actually exist, even though
        # the config is present, we ignore errors when stopping it.
        utils.ignore_exceptions(utils.exec_ssh_cmd)(
            ssh, "sudo service lvm2-lvmetad stop", get_pty=True)
        # disable volume groups. Any volume groups that have volumes in use
        # will remain online. However, volume groups belonging to disks
        # that have been synced at least once, will be deactivated.
        utils.ignore_exceptions(utils.exec_ssh_cmd)(
            ssh, "sudo vgchange -an", get_pty=True)


def _disable_lvm_metad_udev_rule(ssh):
    """
    Removes lvm-metad rule which creates lvm2-pvscan services for each disk
    detected with lvm partitions (after a transfer is complete). During normal
    migrations with multiple replications, these services need to be disabled,
    therefore we make it impossible for the minion OS to create them.
    """
    rule_paths = [
        "/lib/udev/rules.d/69-lvm-metad.rules",
        "/lib/udev/rules.d/69-dm-lvm.rules"]
    for path in rule_paths:
        if utils.test_ssh_path(ssh, path):
            utils.exec_ssh_cmd(ssh, "sudo rm %s" % path, get_pty=True)


def _check_deserialize_key(key):
    res = None
    if isinstance(key, paramiko.RSAKey):
        LOG.trace("Key is already in the proper format.")
        res = key
    elif type(key) is str:
        LOG.trace("Deserializing PEM-encoded private key.")
        res = utils.deserialize_key(
            key, CONF.serialization.temp_keypair_password)
    else:
        raise exception.CoriolisException(
            "Private key must be either a PEM-encoded string or "
            "a paramiko.RSAKey instance. Got type '%s'." % (
                type(key)))

    return res


class BackupWritersFactory(object):

    def __init__(self, writer_connection_info, volumes_info):
        self._validate_info(writer_connection_info)
        self._type = writer_connection_info["backend"]
        self._conn_info = writer_connection_info["connection_details"]
        self._volumes_info = volumes_info

    def get_writer(self):
        if self._type == BACKUP_WRITER_SSH:
            return SSHBackupWriter.from_connection_info(
                self._conn_info, self._volumes_info)
        elif self._type == BACKUP_WRITER_HTTP:
            return HTTPBackupWriter.from_connection_info(
                self._conn_info, self._volumes_info)
        elif self._type == BACKUP_WRITER_FILE:
            return FileBackupWriter.from_connection_info(
                self._conn_info, self._volumes_info)
        raise exception.CoriolisException(
            "Invalid backup writer type: %s" % self._type)

    def _validate_info(self, info):
        if type(info) is not dict:
            raise exception.CoriolisException(
                "Invalid backup writer connection info.")
        wrt_type = info.get("backend", None)
        if wrt_type is None:
            raise exception.CoriolisException(
                "Missing backend name in connection info")
        if wrt_type not in BACKUP_WRITERS:
            raise exception.CoriolisException(
                "Invalid backup writer type: %s" % wrt_type)
        wrt_conn_info = info.get("connection_details")
        if wrt_conn_info is None:
            raise exception.CoriolisException(
                "Missing credentials in connection info")


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

    @classmethod
    @abc.abstractmethod
    def from_connection_info(cls, info, volumes_info):
        pass


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
        os.system("sudo sync")
        self._file = None


class FileBackupWriter(BaseBackupWriter):
    def _get_impl(self, path, disk_id):
        return FileBackupWriterImpl(path, disk_id)

    @classmethod
    def from_connection_info(cls, info, volumes_info):
        return cls()


class SSHBackupWriterImpl(BaseBackupWriterImpl):
    def __init__(self, path, disk_id, compress_transfer=False,
                 encoder_count=3):
        self._msg_id = None
        self._stdin = None
        self._stdout = None
        self._stderr = None
        self._offset = None
        self._ssh = None
        self._sender_q = eventlet.Queue(maxsize=5)
        self._enc_q = eventlet.Queue(maxsize=5)
        self._sender_evt = None
        self._encoder_evt = []
        self._encoder_cnt = encoder_count
        self._exception = None
        self._closing = False

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

    def _encode_data(self, content, offset, msg_id):
        msg = data_transfer.encode_data(
            msg_id, self._path,
            offset, content,
            compress=self._compress_transfer)

        LOG.debug(
            "Guest path: %(path)s, offset: %(offset)d, content len: "
            "%(content_len)d, msg len: %(msg_len)d",
            {"path": self._path,
             "offset": offset,
             "content_len": len(content),
             "msg_len": len(msg)})
        return msg

    def _encode_eod(self):
        msg = data_transfer.encode_eod(self._msg_id)
        LOG.debug("EOD message len: %d", len(msg))
        return msg

    @utils.retry_on_error()
    def _send_msg(self, data):
        # check if write_data is still alive
        if self._stdout.channel.exit_status_ready():
            ret_val = self._stdout.channel.recv_exit_status()
            if int(ret_val) > 0:
                raise exception.CoriolisException(
                    "write_data exited with error code %r (%s)" % (
                        ret_val, _WRITER_ERR_MAP.get(int(ret_val))))

        self._stdin.write(data)
        self._stdin.flush()
        self._stdout.read(4)

    def _open(self):
        self._exec_helper_cmd()
        self._sender_evt = eventlet.spawn(
            self._sender)
        for _ in range(self._encoder_cnt):
            self._encoder_evt.append(
                eventlet.spawn(self._encoder))

    def seek(self, pos):
        self._offset = pos

    def truncate(self, size):
        pass

    def _sender(self):
        while True:
            data = self._sender_q.get()
            try:
                self._send_msg(data)
            except BaseException as err:
                self._exception = err
                raise
            finally:
                self._sender_q.task_done()
                del data

    def _encoder(self):
        while True:
            payload = self._enc_q.get()
            try:
                data = self._encode_data(
                    payload["data"],
                    payload["offset"],
                    payload["msg_id"])
                self._sender_q.put(data)
            except BaseException as err:
                self._exception = err
                raise
            finally:
                self._enc_q.task_done()

    def write(self, data):
        if self._closing:
            raise exception.CoriolisException(
                "Attempted to write to a closed writer.")

        if self._exception:
            raise exception.CoriolisException(
                "Failed to write data. See log "
                "for details.") from self._exception

        payload = {
            "offset": self._offset,
            "data": data,
            "msg_id": self._msg_id,
        }
        self._enc_q.put(payload)
        self._offset += len(data)
        self._msg_id += 1

    def _wait_for_queues(self):
        LOG.info("Waiting for unfinished transfers to complete")
        timeout = datetime.datetime.now() + datetime.timedelta(seconds=600)
        while (self._enc_q.unfinished_tasks or
               self._sender_q.unfinished_tasks) and not self._exception:
            time.sleep(0.5)
            now = datetime.datetime.now()
            if now >= timeout:
                raise exception.CoriolisException(
                    "Timed out waiting for data transfer to finish")

    def close(self):
        self._closing = True
        self._wait_for_queues()
        if self._exception:
            # We can raise here. Any SSH socket cleanup will happen
            # in _handle_exception()
            raise exception.CoriolisException(
                "Exception occurred during data transfer. "
                "Check logs for more details.") from self._exception

        if self._ssh:
            self._send_msg(self._encode_eod())
            self._ssh.exec_command("sudo sync")
            self._ssh.close()
            self._ssh = None
        if self._sender_evt:
            eventlet.kill(self._sender_evt)
            self._sender_evt = None

        for i in self._encoder_evt:
            eventlet.kill(i)
        self._encoder_evt = []

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

    @classmethod
    def from_connection_info(cls, info, volumes_info):
        required = ["ip", "port", "username"]
        ip = info.get("ip")
        port = info.get("port")
        username = info.get("username")
        pkey = info.get("pkey")
        password = info.get("password")

        if not all([ip, port, username]):
            raise exception.CoriolisException(
                "Connection info is invalid for SSHBackupWriter. "
                "The following fields are required: %s" % ", ".join(required))
        if pkey is None and password is None:
            raise exception.CoriolisException(
                "Either pkey or password are required")

        if pkey:
            pkey = _check_deserialize_key(pkey)

        return cls(ip, port, username, pkey, password, volumes_info)

    def _get_impl(self, path, disk_id):
        ssh = self._connect_ssh()
        _disable_lvm_metad_udev_rule(ssh)
        _disable_lvm2_lvmetad(ssh)

        matching_devs = [
            v for v in self._volumes_info if v["disk_id"] == disk_id]

        if not matching_devs:
            base_msg = (
                "Could not locate disk with ID '%s' in volumes_info" %
                disk_id)
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
                utils.get_resources_bin_dir(), 'write_data')
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
        except (Exception, KeyboardInterrupt):
            # No need to log the error as we just raise
            ssh.close()
            raise
        return ssh


class HTTPBackupWriterImpl(BaseBackupWriterImpl):
    def __init__(self, path, disk_id,
                 compress_transfer=None, compressor_count=3):
        self._offset = None
        self._session = None
        self._ip = None
        self._port = None
        self._crt = None
        self._key = None
        self._ca = None
        self._closing = False
        self._write_error = False
        self._id = None
        self._exception = None
        self._compressor_count = compressor_count
        self._comp_q = eventlet.Queue(maxsize=5)
        self._sender_q = eventlet.Queue(maxsize=5)

        self._sender_evt = None
        self._compressor_evt = None

        self._compress_transfer = compress_transfer
        if self._compress_transfer is None:
            self._compress_transfer = CONF.compress_transfers
        super(HTTPBackupWriterImpl, self).__init__(path, disk_id)

    def _set_info(self, info):
        self._ip = info.get("ip")
        self._port = info.get("port")
        self._crt = info.get("client_crt")
        self._key = info.get("client_key")
        self._ca = info.get("ca_crt")
        self._id = info.get("id")
        if not all([self._ip, self._port, self._crt,
                    self._key, self._ca, self._id]):
            raise exception.CoriolisException(
                "Missing required info when creating HTTPBackupWriter")

    @property
    def _uri(self):
        return "https://%s:%s/api/v1/%s" % (
            self._ip, self._port, self._path.lstrip('/')
        )

    @utils.retry_on_error()
    def _acquire(self):
        self._ensure_session()
        uri = "%s/acquire" % self._uri
        headers = {"X-Client-Token": self._id}
        resp = self._session.get(
            uri, headers=headers, timeout=CONF.default_requests_timeout)
        LOG.debug("Returned code: %d. Msg: %s" % (
            resp.status_code, resp.content))
        resp.raise_for_status()

    @utils.retry_on_error()
    def _release(self):
        self._ensure_session()
        uri = "%s/release" % self._uri
        headers = {"X-Client-Token": self._id}
        resp = self._session.get(
            uri, headers=headers, timeout=CONF.default_requests_timeout)
        LOG.debug("Returned code: %d. Msg: %s" %
                  (resp.status_code, resp.content))
        resp.raise_for_status()

    def _init_session(self):
        if self._session:
            self._session.close()
        sess = provider_utils.ProviderSession()
        sess.cert = (
            self._crt,
            self._key)
        sess.verify = self._ca
        self._session = sess

    def _open(self):
        self._closing = False
        self._init_session()
        self._acquire()
        self._sender_evt = eventlet.spawn(self._sender)
        if self._compressor_count is None or self._compressor_count == 0:
            self._compressor_count = 1
        self._compressor_evt = []
        for _ in range(self._compressor_count):
            self._compressor_evt.append(
                eventlet.spawn(self._compressor))

    def seek(self, pos):
        self._offset = pos

    def truncate(self, size):
        pass

    def _ensure_session(self):
        if not self._session:
            self._init_session()
            return
        if self._write_error:
            self._init_session()
            return

    def _compressor(self):
        while True:
            payload = self._comp_q.get()
            send_payload = {
                "encoding": None,
                "offset": payload["offset"],
            }
            chunk = payload["data"]
            if self._compress_transfer:
                try:
                    chunk, compressed = data_transfer.compression_proxy(
                        chunk, constants.COMPRESSION_FORMAT_GZIP)
                    if compressed:
                        send_payload["encoding"] = 'gzip'
                except BaseException as err:
                    LOG.exception(err)
                    self._exception = err
                    self._comp_q.task_done()
                    raise
            send_payload["chunk"] = chunk
            self._sender_q.put(send_payload)
            self._comp_q.task_done()

    def _sender(self):
        while True:
            payload = self._sender_q.get()
            offset = copy.copy(payload["offset"])
            headers = {
                "X-Write-Offset": str(offset),
                "X-Client-Token": copy.copy(self._id),
            }
            if payload.get("encoding", None):
                enc = copy.copy(payload["encoding"])
                headers["content-encoding"] = enc

            @utils.retry_on_error()
            def send():
                self._ensure_session()
                chunk = copy.copy(payload["chunk"])
                LOG.debug(
                    "Guest path: %(path)s, offset: %(offset)d, content len: "
                    "%(content_len)d",
                    {"path": self._path,
                     "offset": offset,
                     "content_len": len(chunk)})
                resp = self._session.post(
                    self._uri, headers=headers, data=chunk,
                    timeout=CONF.default_requests_timeout)
                LOG.debug(
                    "Response code: %r, content: %r" %
                    (resp.status_code, resp.content))
                try:
                    resp.raise_for_status()
                    self._write_error = False
                except Exception as err:
                    LOG.warning(
                        "Error writing chunk to disk %s at offset"
                        " %s: %s" % (self._path, payload["offset"], err))
                    self._write_error = True
                    raise
            try:
                send()
            except BaseException as err:
                # record the exception. We need to terminate
                # the writer if this is set
                LOG.exception(err)
                self._exception = err
                self._sender_q.task_done()
                raise
            finally:
                del headers
                del payload
            self._sender_q.task_done()

    @utils.retry_on_error()
    def write(self, data):
        if self._closing:
            raise exception.CoriolisException(
                "Attempted to write to a closed writer."
            )
        if self._exception:
            raise exception.CoriolisException(self._exception)

        payload = {
            "offset": self._offset,
            "data": data,
        }
        self._comp_q.put(payload)
        self._offset += len(data)

    def _wait_for_queues(self):
        while (self._comp_q.unfinished_tasks or
               self._sender_q.unfinished_tasks) and not self._exception:
            # No error recorded, and we have tasks in the queue
            LOG.info("Waiting for unfinished transfers to complete")
            time.sleep(0.5)

    def close(self):
        self._closing = True
        self._wait_for_queues()
        if self._exception:
            # There was an exception while writing. We still need to
            # release the disk.
            try:
                self._release()
            except Exception as err:
                LOG.error("Failed to release disk %s: %s. Ignoring." % (
                    self._path, err))
            raise exception.CoriolisException(self._exception)

        self._release()
        if self._session:
            self._session.close()
            self._session = None
        if self._sender_evt:
            eventlet.kill(self._sender_evt)
            self._sender_evt = None
        if self._compressor_evt:
            for i in self._compressor_evt:
                eventlet.kill(i)
            self._compressor_evt = None


class HTTPBackupWriterBootstrapper(object):

    def __init__(self, ssh_conn_info, writer_port):
        self._lock = threading.Lock()
        self._writer_cmd = os.path.join(
            "/usr/bin", _CORIOLIS_HTTP_WRITER_CMD)
        self._writer_port = writer_port
        self._ip = ssh_conn_info.get("ip")
        self._port = ssh_conn_info.get("port", 22)
        self._username = ssh_conn_info.get("username")
        self._password = ssh_conn_info.get("password")
        self._pkey = ssh_conn_info.get("pkey")
        if not all([self._ip, self._port, self._username]):
            raise exception.CoriolisException(
                "Invalid SSH connection info. IP, port and"
                " username are mandatory")
        if self._password is None and self._pkey is None:
            raise exception.CoriolisException(
                "Either password or pkey are required")
        if self._pkey:
            self._pkey = _check_deserialize_key(self._pkey)
        self._ssh = self._connect_ssh()

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
        except (Exception, KeyboardInterrupt):
            # No need to log the error as we just raise
            ssh.close()
            raise
        return ssh

    def _inject_dport_allow_rule(self, ssh):
        cmd = (
            "sudo nft insert rule ip filter INPUT tcp dport %(port)s counter "
            "accept || "
            "sudo iptables -I INPUT -p tcp --dport %(port)s -j ACCEPT" % {
                "port": self._writer_port})
        try:
            utils.exec_ssh_cmd(ssh, cmd, get_pty=True)
        except exception.CoriolisException:
            LOG.warn(
                "Could not inject TCP FW rule. Error was: %s",
                utils.get_exception_details())

    def _add_firewalld_port(self, ssh):
        cmd = "sudo firewall-cmd --add-port=%s/tcp" % self._writer_port
        try:
            utils.exec_ssh_cmd(ssh, cmd, get_pty=True)
        except exception.CoriolisException:
            LOG.warn("Could not add TCP port to firewalld. Error was: %s",
                     utils.get_exception_details())

    def _change_binary_se_context(self, ssh):
        cmd = "sudo chcon -t bin_t %s" % self._writer_cmd
        try:
            utils.exec_ssh_cmd(ssh, cmd, get_pty=True)
        except exception.CoriolisException:
            LOG.warn("Could not change SELinux context of writer binary. "
                     "Error was:%s", utils.get_exception_details())

    @utils.retry_on_error()
    def _copy_writer(self, ssh):
        local_path = os.path.join(
            utils.get_resources_bin_dir(), _CORIOLIS_HTTP_WRITER_CMD)
        remote_tmp_path = os.path.join("/tmp", _CORIOLIS_HTTP_WRITER_CMD)
        with self._lock:
            sftp = ssh.open_sftp()
            try:
                # Check if the remote file already exists
                sftp.stat(self._writer_cmd)
            except IOError as ex:
                if ex.errno != errno.ENOENT:
                    raise
                sftp.put(local_path, remote_tmp_path)
                utils.exec_ssh_cmd(
                    ssh,
                    "sudo mv %s %s" % (
                        remote_tmp_path, self._writer_cmd),
                    get_pty=True
                )
                utils.exec_ssh_cmd(
                    ssh,
                    "sudo chmod +x %s" % self._writer_cmd,
                    get_pty=True
                )
            finally:
                sftp.close()

    def _fetch_remote_file(self, ssh, remote_file, local_file):
        with open(local_file, 'wb') as fd:
            utils.exec_ssh_cmd(
                ssh,
                "sudo chmod +r %s" % remote_file, get_pty=True)
            data = utils.retry_on_error()(
                utils.read_ssh_file)(ssh, remote_file)
            fd.write(data)

    def _setup_certificates(self, ssh):
        remote_base_dir = "/etc/coriolis-writer"

        ca_crt_name = "ca-cert.pem"
        client_crt_name = "client-cert.pem"
        client_key_name = "client-key.pem"

        srv_crt_name = "srv-cert.pem"
        srv_key_name = "srv-key.pem"

        remote_ca_crt = os.path.join(remote_base_dir, ca_crt_name)
        remote_client_crt = os.path.join(remote_base_dir, client_crt_name)
        remote_client_key = os.path.join(remote_base_dir, client_key_name)
        remote_srv_crt = os.path.join(remote_base_dir, srv_crt_name)
        remote_srv_key = os.path.join(remote_base_dir, srv_key_name)

        exist = []
        for i in (remote_ca_crt, remote_client_crt, remote_client_key,
                  remote_srv_crt, remote_srv_key):
            exist.append(utils.test_ssh_path(ssh, i))

        if not all(exist):
            utils.exec_ssh_cmd(
                ssh, "sudo mkdir -p %s" % remote_base_dir, get_pty=True)
            utils.exec_ssh_cmd(
                ssh,
                "sudo %(writer_cmd)s generate-certificates -output-dir "
                "%(cert_dir)s -certificate-hosts %(extra_hosts)s" % {
                    "writer_cmd": self._writer_cmd,
                    "cert_dir": remote_base_dir,
                    "extra_hosts": self._ip,
                },
                get_pty=True)

        return {
            "srv_crt": remote_srv_crt,
            "srv_key": remote_srv_key,
            "ca_crt": remote_ca_crt,
            "client_crt": remote_client_crt,
            "client_key": remote_client_key
        }

    def _read_remote_file_sudo(self, remote_path):
        contents = utils.exec_ssh_cmd(
            self._ssh, "sudo cat %s" % remote_path, get_pty=True)
        return contents.decode()

    def _init_writer(self, ssh, cert_paths):
        cmdline = ("%(cmd)s run -ca-cert %(ca_cert)s -key "
                   "%(srv_key)s -cert %(srv_cert)s -listen-port "
                   "%(listen_port)s") % {
                       "cmd": self._writer_cmd,
                       "ca_cert": cert_paths["ca_crt"],
                       "srv_key": cert_paths["srv_key"],
                       "srv_cert": cert_paths["srv_crt"],
                       "listen_port": self._writer_port,
        }
        self._change_binary_se_context(ssh)
        utils.create_service(
            ssh, cmdline, _CORIOLIS_HTTP_WRITER_CMD, start=True)
        self._inject_dport_allow_rule(ssh)
        self._add_firewalld_port(ssh)

    def setup_writer(self):
        _disable_lvm_metad_udev_rule(self._ssh)
        _disable_lvm2_lvmetad(self._ssh)
        self._copy_writer(self._ssh)
        paths = utils.retry_on_error()(
            self._setup_certificates)(self._ssh)
        utils.retry_on_error()(
            self._init_writer)(self._ssh, paths)
        return {
            "ip": self._ip,
            "port": self._writer_port,
            "certificates": {
                "client_crt": self._read_remote_file_sudo(paths["client_crt"]),
                "client_key": self._read_remote_file_sudo(paths["client_key"]),
                "ca_crt": self._read_remote_file_sudo(paths["ca_crt"])
            }
        }


class HTTPBackupWriter(BaseBackupWriter):

    def __init__(self, ip, port, volumes_info, certificates,
                 compressor_count=3):
        self._ip = ip
        self._port = port
        self._volumes_info = volumes_info
        self._writer_port = port
        self._id = str(uuid.uuid4())
        self._compressor_count = compressor_count

        self._certificates = certificates
        self._crt_dir = tempfile.mkdtemp()
        if not self._certificates:
            raise exception.CoriolisException(
                "certificates is mandatory")
        self._cert_paths = None

    @classmethod
    def from_connection_info(cls, conn_info, volumes_info):
        """Instantiate a HTTP backup writer from connection info.

        Connection info has the following schema:

        {
            # IP address or hostname where we can reach the backup writer
            "ip": "192.168.0.1",
            # Backup writer port
            "port": 4433,
            "certificates": {
                # PEM encoded client certificate
                "client_crt": "",
                # PEM encoded client private key
                "client_key": "",
                # PEM encoded CA certificate we use to validate the server
                "ca_crt": ""
            }
        }
        """
        ip = conn_info.get("ip")
        port = conn_info.get("port")
        certs = conn_info.get("certificates", {})

        required = ["ip", "port", "certificates"]
        if not all([ip, port, certs]):
            raise exception.CoriolisException(
                "Missing required connection info: %s" % ", ".join(required))

        required_cert_options = ["client_crt", "client_key", "ca_crt"]
        missing_cert_options = [
            opt for opt in required_cert_options
            if opt not in certs]
        if missing_cert_options:
            raise exception.CoriolisException(
                "Missing the following HTTPBackupWriter fields from the "
                "'certificates' options: %s" % missing_cert_options)

        return cls(ip, port, volumes_info, certs)

    def __del__(self):
        if self._crt_dir and os.path.isdir(self._crt_dir):
            try:
                shutil.rmtree(self._crt_dir)
            except BaseException:
                pass

    def _wait_for_conn(self):
        LOG.debug(
            "waiting for coriolis-writer connectivity %s:%s" % (
                self._ip, self._writer_port))
        utils.wait_for_port_connectivity(
            self._ip, self._writer_port)

    def _write_cert_files(self):
        if not self._certificates:
            raise exception.CoriolisException(
                "certificates not set")
        if self._cert_paths:
            return self._cert_paths

        crt_file = tempfile.mkstemp(dir=self._crt_dir)[1]
        key_file = tempfile.mkstemp(dir=self._crt_dir)[1]
        ca_crt_file = tempfile.mkstemp(dir=self._crt_dir)[1]
        with open(crt_file, "w") as fd:
            fd.write(self._certificates["client_crt"])
        with open(key_file, "w") as fd:
            fd.write(self._certificates["client_key"])
        with open(ca_crt_file, "w") as fd:
            fd.write(self._certificates["ca_crt"])
        self._cert_paths = {
            "client_crt": crt_file,
            "client_key": key_file,
            "ca_crt": ca_crt_file,
        }
        return self._cert_paths

    def _get_impl(self, path, disk_id):
        cert_paths = self._write_cert_files()
        self._wait_for_conn()

        path = [v for v in self._volumes_info
                if v["disk_id"] == disk_id][0]["volume_dev"]
        impl = HTTPBackupWriterImpl(
            path, disk_id,
            compressor_count=self._compressor_count,
            compress_transfer=CONF.compress_transfers)
        impl._set_info({
            "ip": self._ip,
            "port": self._writer_port,
            "client_crt": cert_paths["client_crt"],
            "client_key": cert_paths["client_key"],
            "ca_crt": cert_paths["ca_crt"],
            "id": self._id,
        })
        return impl
