import abc

from oslo_log import log as logging
import paramiko

from coriolis import utils

LOG = logging.getLogger(__name__)


class BaseOSMountTools(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, connection_info, event_manager):
        self._event_manager = event_manager
        self._connect(connection_info)

    @abc.abstractmethod
    def _connect(self, connection_info):
        pass

    @abc.abstractmethod
    def get_connection(self):
        pass

    @abc.abstractmethod
    def check_os(self):
        pass

    @abc.abstractmethod
    def mount_os(self, volume_devs):
        pass

    @abc.abstractmethod
    def dismount_os(self, dirs):
        pass


class BaseSSHOSMountTools(BaseOSMountTools):
    def _connect(self, connection_info):
        ip = connection_info["ip"]
        port = connection_info.get("port", 22)
        username = connection_info["username"]
        pkey = connection_info.get("pkey")
        password = connection_info.get("password")

        LOG.info("Waiting for connectivity on host: %(ip)s:%(port)s",
                 {"ip": ip, "port": port})
        utils.wait_for_port_connectivity(ip, port)

        self._event_manager.progress_update(
            "Connecting to SSH host: %(ip)s:%(port)s" %
            {"ip": ip, "port": port})
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=ip, port=port, username=username, pkey=pkey,
                    password=password)
        self._ssh = ssh

    def get_connection(self):
        return self._ssh
