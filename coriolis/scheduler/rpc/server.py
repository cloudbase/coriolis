# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

import copy
import functools
import random
import uuid

from oslo_config import cfg
from oslo_log import log as logging

from coriolis import constants
from coriolis import exception
from coriolis import utils
from coriolis.conductor.rpc import client as rpc_conductor_client
from coriolis.scheduler.filters import trivial_filters
from coriolis.db import api as db_api


VERSION = "1.0"

LOG = logging.getLogger(__name__)


SCHEDULER_OPTS = []

CONF = cfg.CONF
CONF.register_opts(SCHEDULER_OPTS, 'scheduler')


class SchedulerServerEndpoint(object):
    def __init__(self):
        self._rpc_conductor_client = rpc_conductor_client.ConductorClient()

    def get_diagnostics(self, ctxt):
        return utils.get_diagnostics_info()

    def _get_all_worker_services(self, ctxt):
        services = db_api.get_services(ctxt)
        services = trivial_filters.TopicFilter(
            constants.WORKER_MAIN_MESSAGING_TOPIC).filter_services(
                services)
        if not services:
            raise exception.NoWorkerServiceError()

        return services

    def _get_weighted_filtered_services(
            self, services, filters, minimum_per_filter_rating=1):
        """ Returns list of services and their scores for the given filters.
        Services which are rejected by any filter will be excluded.
        """
        if not filters:
            LOG.warn(
                "No filters provided. Presuming all services acceptable.")
            return [(service, 100) for service in services]

        scores = []


        service_ids = [service.id for service in services]
        LOG.debug(
            "Running following filters on worker services '%s': %s",
            service_ids, filters)
        for service in services:
            total_score = 0

            acceptable = True
            flt = None
            for flt in filters:
                rating = flt.rate_service(service)
                if rating < minimum_per_filter_rating:
                    acceptable = False
                    break
                total_score = total_score + rating
            if not acceptable:
                LOG.debug(
                    "Service with ID '%s' was rejected by filter %r",
                    service.id, flt)
                continue

            scores.append((service, total_score))

        if not scores:
            message = (
                "None of the inspected Coriolis Worker services (IDs %s) "
                "matched the requested filtering criteria (minimum score %d) "
                "for the following required filters: %s" % (
                    [s.id for s in services],
                    minimum_per_filter_rating, filters))
            raise exception.NoSuitableWorkerServiceError(message)

        LOG.debug(
            "Determined following scores for services based on filters '%s': "
            "%s", filters, scores)

        return sorted(
            scores, key=lambda s: s[1], reverse=True)

    def get_workers_for_specs(
            self, ctxt, provider_requirements=None,
            region_ids=None, enabled=None):
        """ Returns a list of enabled Worker Services with the specified
        parameters.
        :param provider_requirements: dict of the form {
            "<platform_type>": [constants.PROVIDER_TYPE_*, ...]}
        """
        filters = []
        worker_services = self._get_all_worker_services(ctxt)

        LOG.debug(
            "Searching for Worker Services with specs: %s" % {
                "provider_requirements": provider_requirements,
                "region_ids": region_ids, "enabled": enabled})

        if enabled is not None:
            filters.append(trivial_filters.EnabledFilter(enabled=enabled))
        if region_ids:
            filters.append(trivial_filters.RegionsFilter(region_ids))
        if provider_requirements:
            filters.append(
                trivial_filters.ProviderTypesFilter(provider_requirements))

        filtered_services = self._get_weighted_filtered_services(
            worker_services, filters)
        LOG.info(
            "Found Worker Services %s for specs: %s" % (
                filtered_services, {
                    "provider_requirements": provider_requirements,
                    "region_ids": region_ids, "enabled": enabled}))

        return [s[0] for s in filtered_services]

    '''
    def get_workers_for_action(
            self, ctxt, endpoint_type, provider_type, region_ids=None):
        """ Returns a list of worker services which would be able to
        perform the required provider-related action.

        :param endpoint_type: the type of the platform of the endpoint
        :param provider_type: the type of the required plugin as defined
                              in constants.PROVIDER_TYPE_*
        :param region_ids: list of region IDs to carry out the operation in.
                           Will be carried out ar random if not provided.
        """
        # TODO
        return self._get_all_worker_services(ctxt)

    def get_workers_for_task(
            self, ctxt, task_type, source_endpoint_type,
            destination_endpoint_type,
            source_region_ids=None,
            destination_region_ids=None):
        """ Returns a list of worker services which would be
        able and willing to accomplish the given task.
        """
        # TODO
        return self._get_all_worker_services(ctxt)
    '''
