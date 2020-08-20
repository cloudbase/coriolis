# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import time

from oslo_log import log as logging

from coriolis import utils


LOG = logging.getLogger(__name__)


def check_create_registration_for_service(
        conductor_rpc, request_context, host, binary, topic, enabled=False,
        mapped_regions=None, providers=None, specs=None, retry_period=30):
    """ Checks with the conductor whether or not a service has already been
    registered for this host and topic and creates one if not.
    If the service is already registered, directs the conductor to refresh the
    service status.
    """
    props = "host='%s', binary='%s', topic='%s'" % (host, binary, topic)
    while True:
        try:
            # check is service already exists:
            LOG.info(
                "Checking with conductor if service with following porperties "
                "was already registered: %s", props)
            worker_service = conductor_rpc.check_service_registered(
                request_context, host, binary, topic)
            if worker_service:
                LOG.info(
                    "A service with properties %s has already been registered "
                    "under ID '%s'. Updating existing registration.",
                    props, worker_service['id'])
                worker_service = conductor_rpc.update_service(
                    request_context, worker_service['id'], updated_values={
                        "providers": providers,
                        "specs": specs})
            else:
                LOG.debug(
                    "Attempting to register new service with properties: %s",
                    props)
                worker_service = conductor_rpc.register_service(
                    request_context, host, binary, topic, enabled,
                    mapped_regions=mapped_regions, providers=providers,
                    specs=specs)
            return worker_service
        except Exception as ex:
            LOG.warn(
                "Failed to register service with specs %s. Retrying again in "
                "%d seconds. Error was: %s", props, retry_period,
                utils.get_exception_details())
            time.sleep(retry_period)
