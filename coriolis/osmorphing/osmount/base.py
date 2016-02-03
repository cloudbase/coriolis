import abc

from coriolis import utils


class BaseOSMountTools(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, ssh):
        self._ssh = ssh

    @abc.abstractmethod
    def check_os(self):
        pass

    @abc.abstractmethod
    def mount_os(self, volume_devs):
        pass

    @abc.abstractmethod
    def dismount_os(self, dirs):
        pass

    def _exec_cmd(self, cmd):
        return utils.exec_ssh_cmd(self._ssh, cmd)
