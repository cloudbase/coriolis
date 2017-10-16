# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import abc

from oslo_config import cfg
from oslo_log import log as logging
from six import with_metaclass

from coriolis import utils

serialization_opts = [
    cfg.StrOpt('temp_keypair_password',
               default=None,
               help='Password to be used when serializing temporary keys'),
]

CONF = cfg.CONF
CONF.register_opts(serialization_opts, 'serialization')
LOG = logging.getLogger(__name__)


class TaskRunner(with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        pass


def get_connection_info(ctxt, data):
    connection_info = data.get("connection_info") or {}
    return utils.get_secret_connection_info(ctxt, connection_info)


def marshal_migr_conn_info(migr_connection_info):
    if migr_connection_info and "pkey" in migr_connection_info:
        migr_connection_info = migr_connection_info.copy()
        migr_connection_info["pkey"] = utils.serialize_key(
            migr_connection_info["pkey"],
            CONF.serialization.temp_keypair_password)
    return migr_connection_info


def unmarshal_migr_conn_info(migr_connection_info):
    if migr_connection_info and "pkey" in migr_connection_info:
        migr_connection_info = migr_connection_info.copy()
        pkey_str = migr_connection_info["pkey"]
        migr_connection_info["pkey"] = utils.deserialize_key(
            pkey_str, CONF.serialization.temp_keypair_password)
    return migr_connection_info
