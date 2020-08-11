# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.
# pylint: disable=anomalous-backslash-in-string

import base64
import binascii
import copy
import functools
import hashlib
import io
import json
import os
import pickle
import platform
import re
import socket
import string
import subprocess
import sys
import time
import traceback
import uuid
import __main__ as main

from io import StringIO

import OpenSSL
from oslo_config import cfg
from oslo_log import log as logging
from oslo_serialization import jsonutils

import netifaces
import paramiko
# NOTE(gsamfira): I am aware that this is not ideal, but pip
# developers have decided to move all logic inside an _internal
# package, and I really don't want to do an exec call to pip
# just to get installed packages and their versions, when I can
# simply call a function.
from pip._internal.operations import freeze
from six.moves.urllib import parse
from webob import exc

from coriolis import constants
from coriolis import exception
from coriolis import secrets

opts = [
    cfg.StrOpt('qemu_img_path',
               default='qemu-img',
               help='The path of the qemu-img tool.'),
]

CONF = cfg.CONF
logging.register_options(CONF)
CONF.register_opts(opts)

LOG = logging.getLogger(__name__)

UNSPACED_MAC_ADDRESS_REGEX = "^([0-9a-f]{12})$"
SPACED_MAC_ADDRESS_REGEX = "^(([0-9a-f]{2}:){5}([0-9a-f]{2}))$"

SYSTEMD_TEMPLATE = """
[Unit]
Description=Coriolis %(svc_name)s
After=network-online.target

[Service]
Type=simple
ExecStart=%(cmdline)s
Restart=always
RestartSec=5s
User=%(username)s

[Install]
WantedBy=multi-user.target
"""

UPSTART_TEMPLATE = """
# %(svc_name)s - Coriolis %(svc_name)s service
#

description     "%(svc_name)s service"

start on runlevel [2345]
stop on runlevel [!2345]

respawn
umask 022

exec %(cmdline)s
"""


def _get_local_ips():
    ifaces = netifaces.interfaces()
    ret = []
    for iface in ifaces:
        if iface == "lo":
            continue
        addrs = netifaces.ifaddresses(iface)
        ret.append(
            {
                iface: {
                    "ipv4": addrs.get(netifaces.AF_INET),
                    "ipv6": addrs.get(netifaces.AF_INET6),
                },
            }
        )
    return ret


def _get_host_os_info():
    info = None
    # This exists on all modern Linux systems
    if os.path.isfile("/etc/os-release"):
        with open("/etc/os-release") as fd:
            info = fd.read().split('\n')
    return info


def get_diagnostics_info():
    # TODO(gsamfira): decide if we want any other kind of
    # diagnostics.
    packages = list(freeze.freeze())
    return {
        "application": get_binary_name(),
        "packages": packages,
        "os_info": _get_host_os_info(),
        "hostname": get_hostname(),
        "ip_addresses": _get_local_ips(),
    }


def setup_logging():
    logging.setup(CONF, 'coriolis')


def ignore_exceptions(func):
    @functools.wraps(func)
    def _ignore_exceptions(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            LOG.warn("Ignoring exception:\n%s", get_exception_details())
    return _ignore_exceptions


def get_single_result(lis):
    """Indexes the head of a single element list.

    :raises KeyError: if the list is empty or its length is greater than 1.
    """
    if len(lis) == 0:
        raise KeyError("Result list is empty.")
    elif len(lis) > 1:
        raise KeyError("More than one result in list: '%s'" % lis)

    return lis[0]


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
                        LOG.warn(
                            "Exception occurred, retrying (%d/%d):\n%s",
                            i, max_attempts, get_exception_details())
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


def parse_os_release(ssh):
    os_release_info = exec_ssh_cmd(
        ssh,
        "[ -f '/etc/os-release' ] && cat /etc/os-release || true").decode()
    info = {}
    for line in os_release_info.splitlines():
        if "=" not in line:
            continue
        k, v = line.split("=")
        k = k.strip()
        v = v.strip()
        info[k] = v.strip('"')
    if info.get("ID") and info.get("VERSION_ID"):
        return (info.get("ID"), info.get("VERSION_ID"))


def parse_lsb_release(ssh):
    out = exec_ssh_cmd(ssh, "lsb_release -a || true").decode()
    dist_id = re.findall('^Distributor ID:\s(.*)$', out, re.MULTILINE)
    release = re.findall('^Release:\s(.*)$', out, re.MULTILINE)
    if dist_id and release:
        return (dist_id[0], release[0])


def get_linux_os_info(ssh):
    info = parse_os_release(ssh)
    if info is None:
        # Fall back to lsb_release
        return parse_lsb_release(ssh)
    return info


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
    fd = sftp.open(remote_path, 'wb')
    # Enabling pipelined transfers here will make
    # SFTP transfers much faster, but in combination
    # with eventlet, it seems to cause some lock-ups
    fd.write(content)
    fd.close()


def write_winrm_file(conn, remote_path, content, overwrite=True):
    """This is a poor man's scp command that transfers small
    files, in chunks, over WinRM.
    """
    if conn.test_path(remote_path):
        if overwrite:
            conn.exec_ps_command(
                'Remove-Item -Force "%s"' % remote_path)
        else:
            raise exception.CoriolisException(
                "File %s already exists" % remote_path)
    idx = 0
    while True:
        data = content[idx:idx+2048]
        if not data:
            break

        if type(data) is str:
            data = data.encode()
        asb64 = base64.b64encode(data).decode()
        cmd = ("$ErrorActionPreference = 'Stop';"
               "$x = [System.IO.FileStream]::new(\"%s\", "
               "[System.IO.FileMode]::Append); $bytes = "
               "[Convert]::FromBase64String('%s'); $x.Write($bytes, "
               "0, $bytes.Length); $x.Close()") % (
                    remote_path, asb64)
        conn.exec_ps_command(cmd)
        idx += 2048


@retry_on_error()
def list_ssh_dir(ssh, remote_path):
    sftp = ssh.open_sftp()
    return sftp.listdir(remote_path)


@retry_on_error()
def exec_ssh_cmd(ssh, cmd, environment=None, get_pty=False):
    remote_str = "<undeterminable>"
    try:
        remote_str = "%s:%s" % ssh.get_transport().sock.getpeername()
    except (ValueError, AttributeError, TypeError):
        LOG.warn(
            "Failed to determine connection string for SSH connection: %s",
            get_exception_details())
    LOG.debug(
        "Executing the following SSH command on '%s' with "
        "environment %s: '%s'", remote_str, environment, cmd)

    _, stdout, stderr = ssh.exec_command(
        cmd, environment=environment, get_pty=get_pty)
    exit_code = stdout.channel.recv_exit_status()
    std_out = stdout.read()
    std_err = stderr.read()
    if exit_code:
        raise exception.CoriolisException(
            "Command \"%s\" failed on host '%s' with exit code: %s\n"
            "stdout: %s\nstd_err: %s" %
            (cmd, remote_str, exit_code, std_out, std_err))
    # Most of the commands will use pseudo-terminal which unfortunately will
    # include a '\r' to every newline. This will affect all plugins too, so
    # best we can do now is replace them.
    return std_out.replace(b'\r\n', b'\n').replace(b'\n\r', b'\n')


def exec_ssh_cmd_chroot(ssh, chroot_dir, cmd, environment=None, get_pty=False):
    return exec_ssh_cmd(ssh, "sudo -E chroot %s %s" % (chroot_dir, cmd),
                        environment=environment, get_pty=get_pty)


def check_fs(ssh, fs_type, dev_path):
    try:
        out = exec_ssh_cmd(
            ssh, "sudo fsck -p -t %s %s" % (fs_type, dev_path),
            get_pty=True).decode()
        LOG.debug("File system checked:\n%s", out)
    except Exception as ex:
        LOG.warn("Checking file system returned an error:\n%s", str(ex))


def run_xfs_repair(ssh, dev_path):
    try:
        tmp_dir = exec_ssh_cmd(
            ssh, "mktemp -d").decode().rstrip("\n")
        LOG.debug("mounting %s on %s" % (dev_path, tmp_dir))
        mount_out = exec_ssh_cmd(
            ssh, "sudo mount %s %s" % (dev_path, tmp_dir),
            get_pty=True).decode()
        LOG.debug("mount returned: %s" % mount_out)
        LOG.debug("Umounting %s" % tmp_dir)
        umount_out = exec_ssh_cmd(
            ssh, "sudo umount %s" % tmp_dir, get_pty=True).decode()
        LOG.debug("umounting returned: %s" % umount_out)
        out = exec_ssh_cmd(
            ssh, "sudo xfs_repair %s" % dev_path, get_pty=True).decode()
        LOG.debug("File system repaired:\n%s", out)
    except Exception as ex:
        LOG.warn("xfs_repair returned an error:\n%s", str(ex))


def _check_port_open(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.settimeout(1)
        s.connect((host, port))
        return True
    except (exception.ConnectionRefusedError, socket.timeout, OSError):
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

    try:
        exec_process(args)
    except Exception:
        ignore_exceptions(os.remove)(target_disk_path)
        raise


def get_hostname():
    return socket.gethostname()


def get_binary_name():
    return os.path.basename(sys.argv[0])


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


def get_resources_dir():
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "resources")


def get_resources_bin_dir():
    return os.path.join(get_resources_dir(), "bin")


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


def load_class(class_path):
    LOG.debug('Loading class \'%s\'' % class_path)
    parts = class_path.rsplit('.', 1)
    module = __import__(parts[0], fromlist=parts[1])
    return getattr(module, parts[1])


def check_md5(data, md5):
    m = hashlib.md5()
    m.update(data)
    new_md5 = m.hexdigest()
    if new_md5 != md5:
        raise exception.CoriolisException("MD5 check failed")


def get_secret_connection_info(ctxt, connection_info):
    secret_ref = connection_info.get("secret_ref")
    if secret_ref:
        LOG.info("Retrieving connection info from secret: %s", secret_ref)
        connection_info = secrets.get_secret(ctxt, secret_ref)
    return connection_info


def parse_int_value(value):
    try:
        return int(str(value))
    except ValueError:
        raise exception.InvalidInput("Invalid integer: %s" % value)


def decode_base64_param(value, is_json=False):
    try:
        decoded = base64.b64decode(value).decode()
        if is_json:
            decoded = json.loads(decoded)
        return decoded
    except (binascii.Error, TypeError, json.decoder.JSONDecodeError) as ex:
        raise exception.InvalidInput(reason=str(ex))


def quote_url(text):
    return parse.quote(text.encode('UTF-8'), safe='')


def normalize_mac_address(original_mac_address):
    """ Normalizez capitalized MAC addresses with or without '-' or ':'
    as separators into all-lower-case ':'-separated form. """
    if not isinstance(original_mac_address, str):
        raise ValueError(
            "MAC address must be a str, got type '%s': %s" % (
                type(original_mac_address), original_mac_address))

    res = ""
    mac_address = original_mac_address.strip().lower().replace('-', ':')
    if re.match(SPACED_MAC_ADDRESS_REGEX, mac_address):
        res = mac_address
    elif re.match(UNSPACED_MAC_ADDRESS_REGEX, mac_address):
        for i in range(0, len(mac_address), 2):
            res = "%s:%s" % (res, mac_address[i:i+2])
        res = res.strip(':')
        if not re.match(SPACED_MAC_ADDRESS_REGEX, res):
            raise ValueError(
                "Failed to normalize MAC address '%s': ended up "
                "with: '%s'" % (original_mac_address, res))
    else:
        raise ValueError(
            "Improperly formatted MAC address: %s" % original_mac_address)

    LOG.debug(
        "Normalized MAC address '%s' to '%s'",
        original_mac_address, res)
    return res


def get_url_with_credentials(url, username, password):
    parts = parse.urlsplit(url)
    # Remove previous credentials if set
    netloc = parts.netloc[parts.netloc.find('@') + 1:]
    netloc = "%s:%s@%s" % (
        quote_url(username), quote_url(password or ''), netloc)
    parts = parts._replace(netloc=netloc)
    return parse.urlunsplit(parts)


def get_unique_option_ids(resources, id_key="id", name_key="name"):
    """Given a list of dictionaries with both the specified 'id_key' and
    'name_key' in each, returns a list of strings, each identifying a certain
    dictionary thusly:
       - if the value under the 'name_key' of a dict is not unique among all
         others, returns the value of the 'id_key'
       - else, returns the value of the 'name_key
    """
    if not all([name_key in d and id_key in d for d in resources]):
        raise KeyError(
            "Some resources are missing the name key '%s' "
            "or ID key '%s': %s" % (name_key, id_key, resources))

    name_mappings = {}
    for resource in resources:
        if resource[name_key] in name_mappings:
            name_mappings[resource[name_key]].append(resource[id_key])
        else:
            name_mappings[resource[name_key]] = [resource[id_key]]

    identifiers = []
    for name, ids in name_mappings.items():
        # if it has only one id, it is unique, append name
        if len(ids) == 1:
            identifiers.append(name)
        else:
            # if it has multiple ids, append ids
            identifiers.extend(ids)

    return identifiers


def bad_request_on_error(error_message):
    def _bad_request_on_error(func):
        def wrapper(*args, **kwargs):
            (is_valid, message) = func(*args, **kwargs)
            if not is_valid:
                raise exc.HTTPBadRequest(explanation=(error_message % message))
            return (is_valid, message)
        return wrapper
    return _bad_request_on_error


def sanitize_task_info(task_info):
    """ Returns a copy of the given task info with any chunking
    info for volumes and sensitive credentials removed.
    """
    new = {}

    special_keys = ['volumes_info', 'origin', 'destination']

    for key in task_info.keys():
        if key not in special_keys:
            new[key] = copy.deepcopy(task_info[key])

    for key in ['origin', 'destination']:
        if key in task_info.keys():
            new[key] = copy.deepcopy(task_info[key])
            if type(new[key]) is dict and 'connection_info' in new[key]:
                new[key]['connection_info'] = {"got": "redacted"}

    if 'volumes_info' in task_info:
        new['volumes_info'] = []
        for vol in task_info['volumes_info']:
            vol_cpy = {}
            for key in vol:
                if key != "replica_state":
                    vol_cpy[key] = copy.deepcopy(vol[key])
                else:
                    vol_cpy['replica_state'] = {}
                    for statekey in vol['replica_state']:
                        if statekey != "chunks":
                            vol_cpy['replica_state'][statekey] = (
                                copy.deepcopy(
                                    vol['replica_state'][statekey]))
                        else:
                            vol_cpy['replica_state']["chunks"] = (
                                ["<redacted>"])
            new['volumes_info'].append(vol_cpy)

    return new


def parse_ini_config(file_contents):
    """ Parses the contents of the given .ini config file and
    returns a dict with the options/values within it.
    """
    config = {}
    regex_expr = '([^#]*[^-\\s#])\\s*=\\s*(?:"|\')?([^#"\']*)(?:"|\')?\\s*'
    for config_line in file_contents.splitlines():
        m = re.match(regex_expr, config_line)
        if m:
            name, value = m.groups()
            config[name] = value
    return config


def read_ssh_ini_config_file(ssh, path, check_exists=False):
    """ Reads and parses the contents of an .ini file at the given path. """
    if not check_exists or test_ssh_path(ssh, path):
        content = read_ssh_file(ssh, path).decode()
        return parse_ini_config(content)
    else:
        return {}


def _write_systemd(ssh, cmdline, svcname, run_as=None, start=True):
    serviceFilePath = "/lib/systemd/system/%s.service" % svcname

    if test_ssh_path(ssh, serviceFilePath):
        return

    def _reload_and_start(start=True):
        exec_ssh_cmd(
            ssh, "sudo systemctl daemon-reload",
            get_pty=True)
        if start:
            exec_ssh_cmd(
                ssh, "sudo systemctl start %s" % svcname,
                get_pty=True)

    systemd_args = {
        "cmdline": cmdline,
        "username": "root",
        "svc_name": svcname,
    }
    if run_as:
        systemd_args["username"] = run_as

    systemdService = SYSTEMD_TEMPLATE % systemd_args

    name = str(uuid.uuid4())
    write_ssh_file(
        ssh, '/tmp/%s.service' % name, systemdService)
    exec_ssh_cmd(
        ssh,
        "sudo mv /tmp/%s.service %s" % (name, serviceFilePath),
        get_pty=True)
    _reload_and_start(start=start)


def _write_upstart(ssh, cmdline, svcname, run_as=None, start=True):
    serviceFilePath = "/etc/init/%s.conf" % svcname

    if test_ssh_path(ssh, serviceFilePath):
        return

    if run_as:
        cmdline = "sudo -u %s -- %s" % (run_as, cmdline)

    upstartService = UPSTART_TEMPLATE % {
        "cmdline": cmdline,
        "svc_name": svcname,
    }
    name = str(uuid.uuid4())
    write_ssh_file(
        ssh, '/tmp/%s.conf' % name, upstartService)
    exec_ssh_cmd(
        ssh,
        "sudo mv /tmp/%s.conf %s" % (name, serviceFilePath),
        get_pty=True)
    if start:
        exec_ssh_cmd(ssh, "start %s" % svcname)


@retry_on_error()
def create_service(ssh, cmdline, svcname, run_as=None, start=True):
    # Simplistic check for init system. We usually use official images,
    # and none of the supported operating systems come with both upstart
    # and systemd installed side by side. So if /lib/systemd/system
    # exists, it's usually systemd enabled. If not, but /etc/init
    # exists, it's upstart
    if test_ssh_path(ssh, "/lib/systemd/system"):
        _write_systemd(ssh, cmdline, svcname, run_as=run_as, start=start)
    elif test_ssh_path(ssh, "/etc/init"):
        _write_upstart(ssh, cmdline, svcname, run_as=run_as, start=start)
    else:
        raise exception.CoriolisException(
            "could not determine init system")


@retry_on_error()
def restart_service(ssh, svcname):
    if test_ssh_path(ssh, "/lib/systemd/system"):
        exec_ssh_cmd(ssh, "sudo systemctl restart %s" % svcname, get_pty=True)
    elif test_ssh_path(ssh, "/etc/init"):
        exec_ssh_cmd(ssh, "restart %s" % svcname)
    else:
        raise exception.UnrecognizedWorkerInitSystem()


@retry_on_error()
def start_service(ssh, svcname):
    if test_ssh_path(ssh, "/lib/systemd/system"):
        exec_ssh_cmd(ssh, "sudo systemctl start %s" % svcname, get_pty=True)
    elif test_ssh_path(ssh, "/etc/init"):
        exec_ssh_cmd(ssh, "start %s" % svcname)
    else:
        raise exception.UnrecognizedWorkerInitSystem()


@retry_on_error()
def stop_service(ssh, svcname):
    if test_ssh_path(ssh, "/lib/systemd/system"):
        exec_ssh_cmd(ssh, "sudo systemctl stop %s" % svcname, get_pty=True)
    elif test_ssh_path(ssh, "/etc/init"):
        exec_ssh_cmd(ssh, "stop %s" % svcname)
    else:
        raise exception.UnrecognizedWorkerInitSystem()


class Grub2ConfigEditor(object):
    """This class edits GRUB2 configs, normally found in
    /etc/default/grub. This class tries to preserve commented
    and empty lines.
    NOTE: This class does not actually write to file during
    commit. Rhather, it will mutate it's internal view of the
    contents of that file with the latest changes made.
    Use dump() to get the file contents.
    """
    def __init__(self, cfg):
        self._cfg = cfg
        self._parsed = self._parse_cfg(self._cfg)

    def _parse_cfg(self, cfg):
        ret = []
        for line in cfg.splitlines():
            if line.startswith("#") or len(line.strip()) == 0:
                ret.append(
                    {
                        "type": "raw",
                        "payload": line
                    }
                )
                continue
            vals = line.split("=", 1)
            if len(vals) != 2:
                ret.append(
                    {
                        "type": "raw",
                        "payload": line
                    }
                )
                continue

            quoted = False
            # should extend to single quotes
            if vals[1].startswith('"') and vals[1].endswith('"'):
                quoted = True
                vals[1] = vals[1].strip('"')

            if len(vals[1]) == 0 or vals[1][0] in string.punctuation:
                ret.append(
                    {
                        "type": "option",
                        "payload": line,
                        "quoted": quoted,
                        "option_name": vals[0],
                        "option_value": [
                            {
                                "opt_type": "single",
                                "opt_val": vals[1],
                            },
                        ]
                    }
                )
                continue
            val_sections = vals[1].split()
            opt_vals = []
            for sect in val_sections:
                fields = sect.split("=", 1)
                if len(fields) == 1:
                    opt_vals.append(
                        {
                            "opt_type": "single",
                            "opt_val": sect,
                        }
                    )
                else:
                    opt_vals.append(
                        {
                            "opt_type": "key_val",
                            "opt_val": fields[1],
                            "opt_key": fields[0],
                        }
                    )
            ret.append(
                {
                    "type": "option",
                    "payload": line,
                    "quoted": quoted,
                    "option_name": vals[0],
                    "option_value": opt_vals,
                }
            )
        return ret

    def _validate_value(self, value):
        if type(value) is not dict:
            raise ValueError("value was not dict")
        opt_type = value.get("opt_type")
        if opt_type not in ("key_val", "single"):
            raise ValueError("invalid value type %s" % opt_type)
        if opt_type == "key_val":
            if "opt_val" not in value or "opt_key" not in value:
                raise ValueError(
                        "key_val option type requires "
                        "opt_key key and opt_val")
        elif opt_type == "single":
            if "opt_val" not in value:
                raise ValueError(
                        "single option type requires opt_val")
        else:
            raise ValueError("unknown option type: %s" % opt_type)

    def set_option(self, option, value):
        """Replaces the value of an option completely
        """
        self._validate_value(value)
        opt_found = False
        for opt in self._parsed:
            if opt.get("option_name") == option:
                opt_found = True
                opt["option_value"] = [value, ]
                break
        if not opt_found:
            self._parsed.append({
                "type": "option",
                "quoted": True,
                "option_name": option,
                "option_value": [
                    value
                ],
            })

    def append_to_option(self, option, value):
        """Appends a value to the specified option. If we're passing
        in a key_val type and the option already exists, the value
        will be replaced. Options of type "single", if absent from the
        list, will be appended. If a single value already exists
        it will be ignored.
        """
        self._validate_value(value)
        opt_found = False
        for opt in self._parsed:
            if opt.get("option_name") == option:
                opt_found = True
                found = False
                for val in opt["option_value"]:
                    if (val["opt_type"] == "key_val" and
                            value["opt_type"] == "key_val"):
                        if str(val["opt_key"]) == str(value["opt_key"]):
                            val["opt_val"] = value["opt_val"]
                            found = True
                    elif (val["opt_type"] == "single" and
                            value["opt_type"] == "single"):
                        if str(val["opt_val"]) == str(value["opt_val"]):
                            found = True
                if not found:
                    opt["option_value"].append(value)
                break
        if not opt_found:
            self._parsed.append({
                "type": "option",
                "quoted": True,
                "option_name": option,
                "option_value": [
                    value
                ],
            })

    def dump(self):
        """dumps the contents of the file"""
        tmp = StringIO()
        for line in self._parsed:
            if line["type"] == "raw":
                tmp.write("%s\n" % line["payload"])
                continue
            vals = line["option_value"]
            flat = []
            for val in vals:
                if val["opt_type"] == "key_val":
                    flat.append("%s=%s" % (val["opt_key"], val["opt_val"]))
                else:
                    flat.append(str(val["opt_val"]))

            if len(flat) == 0:
                tmp.write("%s=\n" % line["option_name"])
                continue

            val = " ".join(flat)
            quoted = line["quoted"]
            if len(flat) > 1:
                quoted = True

            fmt = '%s=%s' % (line["option_name"], val)
            if quoted:
                fmt = '%s="%s"' % (line["option_name"], val)
            tmp.write("%s\n" % fmt)
        tmp.seek(0)
        return tmp.read()
