# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import abc
import paramiko

from oslo_config import cfg
from oslo_log import log as logging
from six import with_metaclass

from coriolis import constants
from coriolis import exception
from coriolis import utils
from coriolis.providers import factory as providers_factory

serialization_opts = [
    cfg.StrOpt('temp_keypair_password',
               default=None,
               help='Password to be used when serializing temporary keys'),
]

CONF = cfg.CONF
CONF.register_opts(serialization_opts, 'serialization')
LOG = logging.getLogger(__name__)


class TaskRunner(with_metaclass(abc.ABCMeta)):

    def get_shared_libs_for_providers(
            self, ctxt, origin, destination, event_handler):
        """ Returns a list of directories containing libraries needed
        for both the source and destination providers. """
        required_libs = []

        origin_provider = providers_factory.get_provider(
            origin["type"], constants.PROVIDER_TYPE_SETUP_LIBS, event_handler,
            raise_if_not_found=False)
        if origin_provider:
            conn_info = get_connection_info(ctxt, origin)
            required_libs.extend(
                origin_provider.get_shared_library_directories(
                    ctxt, conn_info))

        destination_provider = providers_factory.get_provider(
            destination["type"], constants.PROVIDER_TYPE_SETUP_LIBS,
            event_handler, raise_if_not_found=False)
        if destination_provider:
            conn_info = get_connection_info(ctxt, destination)
            required_libs.extend(
                destination_provider.get_shared_library_directories(
                    ctxt, conn_info))

        return required_libs

    @property
    @abc.abstractmethod
    def required_task_info_properties(self):
        """ Returns a list of the string fields which are required
        to be present during the tasks' run method. """
        pass

    @property
    @abc.abstractmethod
    def returned_task_info_properties(self):
        """ Returns a list of the string fields which are returned by the
        tasks' run method to be added to the task info.
        """
        pass

    @abc.abstractmethod
    def _run(self, ctxt, instance, origin, destination, task_info,
             event_handler):
        """ The actual logic run by the task.
        Should return a dict with all the fields declared by
        'self.returned_task_info_properties'.
        Must be implemented in all child classes.
        """
        pass

    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        """ Runs the task with the given params and returns
        a dict with the results.
        NOTE: This should NOT modify the existing task_info in any way.
        """
        missing_info_props = [
            prop for prop in self.required_task_info_properties
            if prop not in task_info]
        if missing_info_props:
            raise exception.CoriolisException(
                "Task type '%s' asked to run on task info with "
                "missing properties: %s" % (
                    self.__class__, missing_info_props))

        result = self._run(
            ctxt, instance, origin, destination, task_info, event_handler)

        if type(result) is not dict:
            raise exception.CoriolisException(
                "Task type '%s' returned result of type %s "
                "instead of a dict: %s" % (
                    self.__class__, type(result), result))

        missing_returns = [
            prop for prop in self.returned_task_info_properties
            if prop not in result.keys()]
        if missing_returns:
            raise exception.CoriolisException(
                "Task type '%s' failed to return the following "
                "declared return values in its result: %s. "
                "Result was: %s" % (
                    self.__class__, missing_returns, result))

        undeclared_returns = [
            prop for prop in result.keys()
            if prop not in self.returned_task_info_properties]
        if undeclared_returns:
            raise exception.CoriolisException(
                "Task type '%s' returned the following undeclared "
                "keys in its result: %s" % (
                    self.__class__, undeclared_returns))

        return result


def get_connection_info(ctxt, data):
    connection_info = data.get("connection_info") or {}
    return utils.get_secret_connection_info(ctxt, connection_info)


def marshal_migr_conn_info(migr_connection_info):
    if migr_connection_info and "pkey" in migr_connection_info:
        migr_connection_info = migr_connection_info.copy()
        pkey = migr_connection_info["pkey"]
        if isinstance(pkey, str) is False:
            migr_connection_info["pkey"] = utils.serialize_key(
                pkey, CONF.serialization.temp_keypair_password)
    return migr_connection_info


def unmarshal_migr_conn_info(migr_connection_info):
    if migr_connection_info and "pkey" in migr_connection_info:
        migr_connection_info = migr_connection_info.copy()
        pkey_str = migr_connection_info["pkey"]
        if isinstance(pkey_str, paramiko.rsakey.RSAKey) is False:
            migr_connection_info["pkey"] = utils.deserialize_key(
                pkey_str, CONF.serialization.temp_keypair_password)
    return migr_connection_info
