# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import abc

from coriolis import secrets

from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class TaskRunner(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def run(self, ctxt, instance, origin, destination, task_info,
            event_handler):
        pass


def get_connection_info(ctxt, data):
    connection_info = data.get("connection_info") or {}
    secret_ref = connection_info.get("secret_ref")
    if secret_ref:
        LOG.info("Retrieving connection info from secret: %s", secret_ref)
        connection_info = secrets.get_secret(ctxt, secret_ref)
    return connection_info
