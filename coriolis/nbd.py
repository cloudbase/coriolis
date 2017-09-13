"""
There is currently no authentication support, nor does this implement
TLS in any way. It is a really basic implementation designed to be used
to stream blocks from images to coriolis workers.

TODO (gsamfira): implement authentication. Looks like qemu-nbd supports
something of the sort
TODO (gsamfira): implement TLS support

With the above 2 implemented, all sorts of fun things could potentially
be done.

Protocol documentation at:

https://sourceforge.net/p/nbd/code/ci/e6b56c12f8a18e7a7cc253c73d1f63c2cbc41e1b/tree/doc/proto.md
"""

import struct
import socket
import subprocess
import os
import netaddr
import time

from coriolis import exception
from coriolis import utils
from oslo_log import log as logging

LOG = logging.getLogger(__name__)


NBD_CMD_READ = 0
# Not really needed, we only care about read
NBD_CMD_WRITE = 1
NBD_CMD_DISC = 2
NBD_CMD_FLUSH = 3
NBD_CMD_TRIM = 4
NBD_CMD_WRITE_ZEROES = 6
NBD_CMD_BLOCK_STATUS = 7
NBD_CMD_RESIZE = 8

NBD_INIT_PASSWD = b'NBDMAGIC'

# Option types
# Client wants to select an export name. After setting this
# option, we move directly to transfer phase
NBD_OPT_EXPORT_NAME = 1
# Abort negotiation and terminate session
NBD_OPT_ABORT = 2
# return a list of exports
NBD_OPT_LIST = 3
# not in use
NBD_OPT_PEEK_EXPORT = 4
# client wants to initiate TLS
NBD_OPT_STARTTLS = 5
# Get more detailed info about an export
NBD_OPT_INFO = 6
# Client wishes to terminate the handshake phase
# and move to transmission phase.
NBD_OPT_GO = 7


# Option reply types
# Sent by server when it accepts the option,
# and no further data is available
NBD_REP_ACK = 1
# A description of an export
NBD_REP_SERVER = 2
# detailed description of an aspect of an export
NBD_REP_INFO = 3

# There are a number of error reply types, all of which are denoted by
# having bit 31 set. All error replies MAY have some data set, in which
# case that data is an error message string suitable for display to the user.
NBD_REP_ERR_UNSUP = 2147483649
NBD_REP_ERR_POLICY = 2147483650
NBD_REP_ERR_INVALID = 2147483651
NBD_REP_ERR_PLATFORM = 2147483652
NBD_REP_ERR_TLS_REQD = 2147483653
NBD_REP_ERR_UNKNOWN = 2147483654
NBD_REP_ERR_SHUTDOWN = 2147483655
NBD_REP_ERR_BLOCK_SIZE_REQD = 2147483656

# Error values
EPERM = 1
EIO = 5
ENOMEM = 12
EINVAL = 22
ENOSPC = 28
EOVERFLOW = 75
ESHUTDOWN = 108

# Transmission flags
NBD_FLAG_HAS_FLAGS = 1 << 0
NBD_FLAG_READ_ONLY = 1 << 1
NBD_FLAG_SEND_FLUSH = 1 << 2
NBD_FLAG_SEND_FUA = 1 << 3
NBD_FLAG_ROTATIONAL = 1 << 4
NBD_FLAG_SEND_TRIM = 1 << 5
NBD_FLAG_SEND_WRITE_ZEROES = 1 << 6
NBD_FLAG_SEND_DF = 1 << 7
NBD_FLAG_CAN_MULTI_CONN = 1 << 8
NBD_FLAG_SEND_BLOCK_STATUS = 1 << 9
NBD_FLAG_SEND_RESIZE = 1 << 10

# New style server that supports extending
NBD_FLAG_C_FIXED_NEWSTYLE = 1 << 0
# Do not send the 128 bytes of empty zeroes
NBD_FLAG_NO_ZEROES = 1 << 1

NBD_OPTS_MAGIC = 0x49484156454F5054
NBD_SERVER_REPLY_MAGIC = 0x3e889045565a9
NBD_CLISERV_MAGIC = 0x420281861253
NBD_REQUEST_MAGIC = 0x25609513
NBD_REPLY_MAGIC = 0x67446698


class NBDClient(object):
    """
    Really basic, READ-ONLY NBD client implementation. Only useful
    for consuming chunks of an export, or the entire thing.

    WARNING: Do not try to do parallel reads using this class. It will
    most likely result in garbage data, due to the fact that
    handles are not properly implemented. That whole song and
    dance requires more complex code. Sequential reads only
    at this point please.
    """
    def __init__(self, host=None, port=None,
                 unix_socket=None, export_name=None):
        self._client_flags = NBD_FLAG_C_FIXED_NEWSTYLE
        self.export_size = None
        self.export_name = export_name
        self._handle = b'1'
        self._host = host
        self._port = port
        self._unix_socket = unix_socket
        self._export_name = export_name
        self.sock = None

    def _select_export(self, sock, name):
        if type(name) is str:
            name = bytes(name.encode("ascii"))
        magic = struct.pack('>Q', NBD_OPTS_MAGIC)
        opt = struct.pack('>L', NBD_OPT_EXPORT_NAME)
        name_size = struct.pack('>L', len(name))

        payload = magic + opt + name_size + name
        sock.sendall(payload)
        response = sock.recv(64)
        if len(response) == 0:
            raise exception.NBDException(
                "Read failed. Likely export name is wrong")
        decoded = struct.unpack('>QH', response)
        return decoded[0]

    def _negotiate(self, sock, name=None):
        # fetch the init password. If this is invalid, either the
        # server erred or we are trying to start a negotiation a socket
        # that is already in transmission phase
        passwdSize = struct.calcsize('>8s')
        passwd = struct.unpack('>8s', sock.recv(passwdSize))
        if passwd[0] != NBD_INIT_PASSWD:
            raise exception.NBDException("Bad NBD passwd: %r. Expected: %r" % (
                passwd[0], NBD_INIT_PASSWD))

        magicSize = struct.calcsize('>Q')
        magic = struct.unpack('>Q', sock.recv(magicSize))
        if magic[0] == int(NBD_CLISERV_MAGIC):
            # Old style negotiation is not really a negotiation. It's more
            # like the server saying: "here you go do whatever". Not unlike
            # a school canteen lunch lady would do when you humbly
            # (but naively) ask for something edible.
            LOG.info("Using old style negotiation for %s" % self.export_name)
            info = struct.unpack('>Q128s', sock.recv(
                struct.calcsize('>Q128s')))
            self.export_size = info[0]
        else:
            # Looks like we're using new style negotiation.
            # Export name is required in this situation
            if name is None:
                raise ValueError("export name is required for"
                                 "new style negotiation")
            # Check that we're using the FIXED_NEWSTYLE
            # Flags are an unsigned short
            flags = struct.unpack('>H', sock.recv(struct.calcsize('>H')))
            needed = flags[0] & NBD_FLAG_C_FIXED_NEWSTYLE
            if needed != NBD_FLAG_C_FIXED_NEWSTYLE:
                raise exception.NBDException(
                    "Server does not support export listing")
            if flags[0] & NBD_FLAG_NO_ZEROES:
                self._client_flags |= NBD_FLAG_NO_ZEROES
            # Send client flags
            client_flags = struct.pack('>L', self._client_flags)
            sock.send(client_flags)
            self.export_size = self._select_export(sock, name)

    def connect(self, host=None, port=None,
                unix_socket=None, export_name=None):
        # WARNING: there is no TLS support. Make sure you only use this
        # for local connections, or in secure environments

        _host = host or self._host
        _port = port or self._port
        _unix_socket = unix_socket or self._unix_socket
        _export_name = export_name or self._export_name

        if self.sock:
            # we are reconnectiong. Clean up after ourselves
            self.close()

        sock = None
        addr = None
        if _unix_socket is not None:
            # no need to do extra checks, socket will raise
            # if the supplied path does not exist
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            addr = _unix_socket

        if None not in (_host, _port):
            ipVersion = netaddr.IPNetwork(_host).version
            inet = socket.AF_INET
            if ipVersion == 6:
                inet = socket.AF_INET6
            sock = socket.socket(inet, socket.SOCK_STREAM)
            addr = (_host, _port)

        if sock is None:
            raise exception.NBDException(
                "either host/port or socket needs to be set")

        try:
            sock.connect(addr)
        except socket.error as err:
            if err.errno == 106:
                # already connected, just return
                # NOTE (gsamfira): this assumes that negotiation
                # has already happened
                return sock
            raise
        self._negotiate(sock, name=_export_name)
        self.sock = sock

        self._host = _host
        self._port = _port
        self._unix_socket = _unix_socket
        self._export_name = _export_name
        return self.sock

    def close(self):
        if self.sock is None:
            return
        request = struct.pack(
            '>LL8sQL',
            NBD_REQUEST_MAGIC,
            NBD_CMD_DISC,
            self._handle,
            0,
            0)
        self.sock.send(request)
        self.sock.close()
        self.sock = None
        self.export_size = None

    def read(self, offset, length):
        if self.sock is None:
            raise exception.NBDConnectionException(
                "Socket is not connected")
        if offset > self.export_size:
            raise ValueError("Offset is outside the size of export")
        readEnd = offset + length
        if readEnd > self.export_size:
            length = self.export_size - offset
        request = struct.pack(
            '>LL8sQL',
            NBD_REQUEST_MAGIC,
            NBD_CMD_READ,
            # NOTE (gsamfira): this function is not safe for
            # concurrent reads! Must not run read() in parallel.
            # TODO (gsamfira): Implement concurrency. Needs to treat
            # handles appropriately. Responses are treated
            # asynchronously  and may come out of order
            self._handle,
            offset,
            length)

        self.sock.send(request)
        responseSize = struct.calcsize('>LL8s')
        response = self.sock.recv(responseSize)
        magic, error, handle = struct.unpack('>LL8s', response)
        if magic != int(NBD_REPLY_MAGIC):
            raise ValueError(
                "Got invalid magic from "
                "server: %r" % magic)
        if error != 0:
            # TODO (gsamfira): translate error codes to messages
            raise Exception(
                "Got invalid response from "
                "server: %r" % error)
        got = b''
        while len(got) < length:
            more = self.sock.recv(length - len(got))
            if more == "":
                raise exception.NBDException(length)
            got += more
        return got


class DiskImageReader(object):

    def __init__(self, path, name):
        """
        param: path: str: The path to the virtual disk image you want to read
        param: name: str: The name of the export
        """
        self.image_path = path
        self.export_name = name
        self.socket_path = "/tmp/%s.sock" % self.export_name
        self._nbd_client = None
        self._qemu_process = None

    @property
    def export_size(self):
        if self._nbd_client is not None:
            return self._nbd_client.export_size
        return None

    def _wait_for_socket(self, process, socket_path):
        count = 0
        while True:
            # arbitrary. wait 5 seconds
            if count >= 50:
                raise TimeoutError("timed out waiting for"
                                   " socket: %s" % socket_path)
            status = process.poll()
            if status:
                stdout, stderr = process.communicate()
                raise exception.NBDException(
                    "process failed with status: %r" % status)
            if os.path.exists(socket_path):
                return socket_path
            time.sleep(0.1)

    def _supports_newstyle(self):
        for i in subprocess.check_output(["qemu-nbd", "-h"]).splitlines():
            if b'--export-name' in i:
                return True
        return False

    def connect(self):
        if os.path.isfile(self.image_path) is False:
            raise ValueError("Image file %s does not exist" % self.image_path)
        if self._qemu_process is not None:
            raise exception.NBDException("qemu-nbd is already running")
        if self._nbd_client is not None:
            raise exception.NBDException("client already created")

        if os.path.exists(self.socket_path):
            raise exception.NBDException(
                "socket %s already exists" % self.socket_path)

        qemu_cmd = [
            "qemu-nbd", "-k", self.socket_path,
            self.image_path
        ]

        if self._supports_newstyle():
            qemu_cmd = [
                "qemu-nbd", "-k", self.socket_path,
                "-x", self.export_name, self.image_path
            ]

        LOG.debug("Running command: %s" % ' '.join(qemu_cmd))
        self._qemu_process = subprocess.Popen(qemu_cmd)
        self._wait_for_socket(self._qemu_process, self.socket_path)

        self._nbd_client = NBDClient(
            unix_socket=self.socket_path,
            export_name=self.export_name)
        self._nbd_client.connect()

    def close(self):
        if self._nbd_client:
            self._nbd_client.close()
            self._nbd_client = None
        if self._qemu_process:
            # the qemu-nbd binary should already have exited on
            # self._nbd_client.close(), but we kill it for good
            # measure.
            self._qemu_process.kill()
            self._qemu_process.wait()
            self._qemu_process = None
        try:
            os.remove(self.socket_path)
        except BaseException:
            pass

    @utils.retry_on_error(terminal_exceptions=[
        exception.NBDException, exception.NBDConnectionException])
    def read(self, offset, length):
        if self._nbd_client is None:
            raise exception.NBDException("not initialized properly")
        return self._nbd_client.read(offset, length)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()
