import functools
import json
import re
import socket
import subprocess
import time
import traceback

from oslo_config import cfg
from oslo_log import log as logging

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


def retry_on_error(max_attempts=5, sleep_seconds=0):
    def _retry_on_error(func):
        @functools.wraps(func)
        def _exec_retry(*args, **kwargs):
            i = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
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


def convert_disk_format(disk_path, target_disk_path, target_format):
    if target_format == constants.DISK_FORMAT_VHD:
        target_format = "vpc"
    exec_process([CONF.qemu_img_path, 'convert', '-O', target_format,
                  disk_path, target_disk_path])


def get_hostname():
    return socket.gethostname()


def get_exception_details():
    return traceback.format_exc()
