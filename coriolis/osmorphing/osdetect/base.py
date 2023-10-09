# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.


import abc
import os

from six import with_metaclass

from coriolis import exception
from coriolis import utils

# Required OS release fields to be returned as declared in the
# 'schemas.CORIOLIS_DETECTED_OS_MORPHING_INFO_SCHEMA' schema:
REQUIRED_DETECTED_OS_FIELDS = [
    "os_type", "distribution_name", "release_version",
    "friendly_release_name"]


class BaseOSDetectTools(object, with_metaclass(abc.ABCMeta)):

    def __init__(self, conn, os_root_dir, operation_timeout):
        self._conn = conn
        self._os_root_dir = os_root_dir
        self._environment = {}
        self._osdetect_operation_timeout = operation_timeout

    @abc.abstractclassmethod
    def returned_detected_os_info_fields(cls):
        raise NotImplementedError(
            "No returned OS info fields by class '%s'" % cls.__name__)

    @abc.abstractmethod
    def detect_os(self):
        """ Attempts to detect the mounted OS and return all relevant
        release info as a dict.

        Must conform to the 'schemas.CORIOLIS_DETECTED_OS_MORPHING_INFO_SCHEMA'
        """
        raise NotImplementedError(
            "`detect_os` not implemented for OS detection tools '%s'" % (
                type(self)))

    def set_environment(self, environment):
        self._environment = environment

    # @abc.abstractmethod
    # def get_friendly_release_name(self, detailed_release_info):
    #     """ Returns a friendly name/identifier for the OS based on the
    #     detailed release info as returned by 'detect_os'. """
    #     raise NotImplementedError(
    #         "`get_friendly_release_name` not implemented for OS detection "
    #         "tools '%s'" % type(self))


class BaseLinuxOSDetectTools(BaseOSDetectTools):

    @classmethod
    def returned_detected_os_info_fields(cls):
        return REQUIRED_DETECTED_OS_FIELDS

    def _read_file(self, chroot_path):
        path = os.path.join(self._os_root_dir, chroot_path)
        return utils.read_ssh_file(self._conn, path)

    def _read_config_file(self, chroot_path, check_exists=False):
        full_path = os.path.join(self._os_root_dir, chroot_path)
        return utils.read_ssh_ini_config_file(
            self._conn, full_path, check_exists=check_exists)

    def _get_os_release(self):
        return self._read_config_file(
            "etc/os-release", check_exists=True)

    def _test_path(self, chroot_path):
        full_path = os.path.join(self._os_root_dir, chroot_path)
        return utils.test_ssh_path(self._conn, full_path)

    def _exec_cmd(self, cmd, timeout=None):
        if not timeout:
            timeout = self._osdetect_operation_timeout
        try:
            return utils.exec_ssh_cmd(
                self._conn, cmd, environment=self._environment, get_pty=True,
                timeout=timeout)
        except exception.MinionMachineCommandTimeout as ex:
            raise exception.OSMorphingSSHOperationTimeout(
                cmd=cmd, timeout=timeout) from ex

    def _exec_cmd_chroot(self, cmd, timeout=None):
        if not timeout:
            timeout = self._osdetect_operation_timeout
        try:
            return utils.exec_ssh_cmd_chroot(
                self._conn, self._os_root_dir, cmd,
                environment=self._environment, get_pty=True, timeout=timeout)
        except exception.MinionMachineCommandTimeout as ex:
            raise exception.OSMorphingSSHOperationTimeout(
                cmd=cmd, timeout=timeout) from ex
