# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import functools
import io
import json
import os
import pickle
import re
import socket
import subprocess
import time
import traceback

import OpenSSL
from oslo_config import cfg
from oslo_log import log as logging
from oslo_serialization import jsonutils
import paramiko

from coriolis import constants
from coriolis import exception

opts = [
    cfg.StrOpt('qemu_img_path',
               default='qemu-img',
               help='The path of the qemu-img tool.'),
]

CONF = cfg.CONF
logging.register_options(CONF)
CONF.register_opts(opts)

LOG = logging.getLogger(__name__)


def setup_logging():
    logging.setup(CONF, 'coriolis')


def ignore_exceptions(func):
    @functools.wraps(func)
    def _ignore_exceptions(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as ex:
            LOG.exception(ex)
    return _ignore_exceptions


def retry_on_error(max_attempts=5, sleep_seconds=0,
                   terminal_exceptions=[]):
    def _retry_on_error(func):
        @functools.wraps(func)
        def _exec_retry(*args, **kwargs):
            i = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except KeyboardInterrupt as ex:
                    LOG.debug("Got a KeyboardInterrupt, skip retrying")
                    LOG.exception(ex)
                    raise
                except Exception as ex:
                    if any([isinstance(ex, tex)
                            for tex in terminal_exceptions]):
                        raise

                    i += 1
                    if i < max_attempts:
                        LOG.warn("Exception occurred, retrying: %s", ex)
                        time.sleep(sleep_seconds)
                    else:
                        raise
        return _exec_retry
    return _retry_on_error


def get_udev_net_rules(net_ifaces_info):
    content = ""
    for name, mac_address in net_ifaces_info:
        content += ('SUBSYSTEM=="net", ACTION=="add", DRIVERS=="?*", '
                    'ATTR{address}=="%(mac_address)s", NAME="%(name)s"\n' %
                    {"name": name, "mac_address": mac_address.lower()})
    return content


def get_linux_os_info(ssh):
    out = exec_ssh_cmd(ssh, "lsb_release -a || true").decode()
    dist_id = re.findall('^Distributor ID:\s(.*)$', out, re.MULTILINE)
    release = re.findall('^Release:\s(.*)$', out, re.MULTILINE)
    if dist_id and release:
        return (dist_id[0], release[0])


@retry_on_error()
def test_ssh_path(ssh, remote_path):
    sftp = ssh.open_sftp()
    try:
        sftp.stat(remote_path)
        return True
    except IOError as ex:
        if ex.args[0] == 2:
            return False
        raise


@retry_on_error()
def read_ssh_file(ssh, remote_path):
    sftp = ssh.open_sftp()
    return sftp.open(remote_path, 'rb').read()


@retry_on_error()
def write_ssh_file(ssh, remote_path, content):
    sftp = ssh.open_sftp()
    sftp.open(remote_path, 'wb').write(content)


@retry_on_error()
def list_ssh_dir(ssh, remote_path):
    sftp = ssh.open_sftp()
    return sftp.listdir(remote_path)


@retry_on_error()
def exec_ssh_cmd(ssh, cmd):
    LOG.debug("Executing SSH command: %s", cmd)
    stdin, stdout, stderr = ssh.exec_command(cmd)
    exit_code = stdout.channel.recv_exit_status()
    std_out = stdout.read()
    std_err = stderr.read()
    if exit_code:
        raise exception.CoriolisException(
            "Command \"%s\" failed with exit code: %s\n"
            "stdout: %s\nstd_err: %s" %
            (cmd, exit_code, std_out, std_err))
    return std_out


def exec_ssh_cmd_chroot(ssh, chroot_dir, cmd):
    return exec_ssh_cmd(ssh, "sudo chroot %s %s" % (chroot_dir, cmd))


def check_fs(ssh, fs_type, dev_path):
    try:
        out = exec_ssh_cmd(
            ssh, "sudo fsck -p -t %s %s" % (fs_type, dev_path)).decode()
        LOG.debug("File system checked:\n%s", out)
    except Exception as ex:
        LOG.warn("Checking file system returned an error:\n%s", str(ex))


def _check_port_open(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.settimeout(1)
        s.connect((host, port))
        return True
    except (ConnectionRefusedError, socket.timeout, OSError):
        return False
    finally:
        s.close()


def wait_for_port_connectivity(address, port, max_wait=300):
    i = 0
    while not _check_port_open(address, port) and i < max_wait:
        time.sleep(1)
        i += 1
    if i == max_wait:
        raise exception.CoriolisException("Connection failed on port %s" %
                                          port)


def exec_process(args):
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    std_out, std_err = p.communicate()
    if p.returncode:
        raise exception.CoriolisException(
            "Command \"%s\" failed with exit code: %s\nstdout: %s\nstd_err: %s"
            % (args, p.returncode, std_out, std_err))
    return std_out


def get_disk_info(disk_path):
    out = exec_process([CONF.qemu_img_path, 'info', '--output=json',
                        disk_path])
    disk_info = json.loads(out.decode())

    if disk_info["format"] == "vpc":
        disk_info["format"] = constants.DISK_FORMAT_VHD
    return disk_info


def convert_disk_format(disk_path, target_disk_path, target_format,
                        preallocated=False):
    allocation_args = []

    if preallocated:
        if target_format != constants.DISK_FORMAT_VHD:
            raise NotImplementedError(
                "Preallocation is supported only for the VHD format.")

        allocation_args = ['-o', 'subformat=fixed']

    if target_format == constants.DISK_FORMAT_VHD:
        target_format = "vpc"

    args = ([CONF.qemu_img_path, 'convert', '-O', target_format] +
            allocation_args +
            [disk_path, target_disk_path])

    exec_process(args)


def get_hostname():
    return socket.gethostname()


def get_exception_details():
    return traceback.format_exc()


def walk_class_hierarchy(clazz, encountered=None):
    """Walk class hierarchy, yielding most derived classes first."""
    if not encountered:
        encountered = []
    for subclass in clazz.__subclasses__():
        if subclass not in encountered:
            encountered.append(subclass)
            # drill down to leaves first
            for subsubclass in walk_class_hierarchy(subclass, encountered):
                yield subsubclass
            yield subclass


def get_ssl_cert_thumbprint(context, host, port=443, digest_algorithm="sha1"):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ssl_sock = context.wrap_socket(sock, server_hostname=host)
    ssl_sock.connect((host, port))
    # binary_form is the only option when the certificate is not validated
    cert = ssl_sock.getpeercert(binary_form=True)
    sock.close()

    x509 = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_ASN1, cert)
    return x509.digest('sha1').decode()


def _get_base_dir():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_resources_dir():
    return os.path.join(_get_base_dir(), "resources")


def serialize_key(key, password=None):
    key_io = io.StringIO()
    key.write_private_key(key_io, password)
    return key_io.getvalue()


def deserialize_key(key_bytes, password=None):
    key_io = io.StringIO(key_bytes)
    return paramiko.RSAKey.from_private_key(key_io, password)


def is_serializable(obj):
    pickle.dumps(obj)


def to_dict(obj, max_depth=10):
    # jsonutils.dumps() has a max_depth of 3 by default
    def _to_primitive(value, convert_instances=False,
                      convert_datetime=True, level=0,
                      max_depth=max_depth):
        return jsonutils.to_primitive(
            value, convert_instances, convert_datetime, level, max_depth)
    return jsonutils.loads(jsonutils.dumps(obj, default=_to_primitive))


def topological_graph_sorting(items, id="id", depends_on="depends_on",
                              sort_key=None):
    """
    Kahn's algorithm
    """
    if sort_key:
        # Sort siblings
        items = sorted(items, key=lambda t: t[sort_key], reverse=True)

    a = []
    for i in items:
        a.append({"id": i[id],
                  "depends_on": list(i[depends_on] or []),
                  "item": i})

    s = []
    l = []
    for n in a:
        if not n["depends_on"]:
            s.append(n)
    while s:
        n = s.pop()
        l.append(n["item"])

        for m in a:
            if n["id"] in m["depends_on"]:
                m["depends_on"].remove(n["id"])
                if not m["depends_on"]:
                    s.append(m)

    if len(l) != len(a):
        raise ValueError("The graph contains cycles")

    return l
