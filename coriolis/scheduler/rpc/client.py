# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import random
import time

import oslo_messaging as messaging
from oslo_config import cfg
from oslo_log import log as logging

from coriolis import constants
from coriolis import exception
from coriolis import rpc
from coriolis import utils
from coriolis.tasks import factory as tasks_factory


VERSION = "1.0"
LOG = logging.getLogger(__name__)

scheduler_opts = [
    cfg.IntOpt("scheduler_rpc_timeout",
               help="Number of seconds until RPC calls to the "
                    "scheduler timeout.")
]

CONF = cfg.CONF
CONF.register_opts(scheduler_opts, 'scheduler')


class SchedulerClient(rpc.BaseRPCClient):
    def __init__(self, timeout=None):
        target = messaging.Target(topic='coriolis_scheduler', version=VERSION)
        if timeout is None:
            timeout = CONF.scheduler.scheduler_rpc_timeout
        super(SchedulerClient, self).__init__(
            target, timeout=timeout)

    def get_diagnostics(self, ctxt):
        return self._call(ctxt, 'get_diagnostics')

    def get_workers_for_specs(
            self, ctxt, provider_requirements=None,
            region_sets=None, enabled=None):
        return self._call(
            ctxt, 'get_workers_for_specs', region_sets=region_sets,
            enabled=enabled, provider_requirements=provider_requirements)

    def get_any_worker_service(
            self, ctxt, random_choice=False, raise_if_none=True):
        services = self.get_workers_for_specs(ctxt)
        if not services:
            if raise_if_none:
                raise exception.NoWorkerServiceError()
            return None
        service = services[0]
        if random_choice:
            service = random.choice(services)
        LOG.debug(
            "Selected service with ID '%s' for any-worker request.",
            service['id'])
        return service

    def get_worker_service_for_specs(
            self, ctxt, provider_requirements=None, region_sets=None,
            enabled=True, random_choice=False, raise_on_no_matches=True):
        """Utility method which ensures at least one service matching
        the provided requirements exists and is usable.
        """
        requirements_str = (
            "enabled=%s; region_sets=%s; provider_requirements=%s" % (
                enabled, region_sets, provider_requirements))
        LOG.info(
            "Requesting Worker Service from scheduler with the following "
            "specifications: %s", requirements_str)
        services = self.get_workers_for_specs(
            ctxt, provider_requirements=provider_requirements,
            region_sets=region_sets, enabled=enabled)
        if not services:
            if raise_on_no_matches:
                raise exception.NoSuitableWorkerServiceError()
            return None
        LOG.debug(
            "Was offered Worker Services with the following IDs for "
            "requirements '%s': %s",
            requirements_str, [s["id"] for s in services])

        selected_service = services[0]
        if random_choice:
            selected_service = random.choice(services)

        LOG.info(
            "Was offered Worker Service with ID '%s' for requirements: %s",
            selected_service['id'], requirements_str)
        return selected_service

    def get_worker_service_for_task(
            self, ctxt, task, origin_endpoint, destination_endpoint,
            retry_count=5, retry_period=2, random_choice=True):
        """ Gets a worker service for the task with the given properties
        and source/target endpoints.

        :param task: Dict of the form: {
            "id": "<task_id>",
            "task_type": "<constants.TASK_TYPE_*>"}
        :param origin_endpoint: Dict of the form {
            "id": "<ID>",
            "mapped_regions": ["List of mapped endpoint regions"]}
        :param destination_endpoint: Same as origin_endpoint
        """
        LOG.debug(
            "Compiling required Worker Service specs for task with "
            "ID '%s' (type '%s') from endpoints '%s' to '%s'",
            task['id'], task['task_type'], origin_endpoint['id'],
            destination_endpoint['id'])
        task_cls = tasks_factory.get_task_runner_class(
            task['task_type'])

        # determine required Coriolis regions based on the endpoints:
        required_region_sets = []
        origin_endpoint_region_ids = [
            r.id for r in origin_endpoint['mapped_regions']]
        destination_endpoint_region_ids = [
            r.id for r in destination_endpoint['mapped_regions']]

        required_platform = task_cls.get_required_platform()
        if required_platform in (
                constants.TASK_PLATFORM_SOURCE,
                constants.TASK_PLATFORM_BILATERAL):
            required_region_sets.append(origin_endpoint_region_ids)
        if required_platform in (
                constants.TASK_PLATFORM_DESTINATION,
                constants.TASK_PLATFORM_BILATERAL):
            required_region_sets.append(destination_endpoint_region_ids)

        # determine provider requirements:
        provider_requirements = {}
        required_provider_types = task_cls.get_required_provider_types()
        if constants.PROVIDER_PLATFORM_SOURCE in required_provider_types:
            provider_requirements[origin_endpoint['type']] = (
                required_provider_types[
                    constants.PROVIDER_PLATFORM_SOURCE])
        if constants.PROVIDER_PLATFORM_DESTINATION in required_provider_types:
            provider_requirements[destination_endpoint['type']] = (
                required_provider_types[
                    constants.PROVIDER_PLATFORM_DESTINATION])

        worker_service = None
        for i in range(retry_count):
            try:
                LOG.debug(
                    "Requesting Worker Service for task with ID '%s' (type "
                    "'%s') from endpoints '%s' to '%s'", task['id'],
                    task['task_type'], origin_endpoint['id'],
                    destination_endpoint['id'])
                worker_service = self.get_worker_service_for_specs(
                    ctxt, provider_requirements=provider_requirements,
                    region_sets=required_region_sets, enabled=True,
                    random_choice=random_choice)
                LOG.debug(
                    "Scheduler has granted Worker Service '%s' for task with "
                    "ID '%s' (type '%s') from endpoints '%s' to '%s'",
                    worker_service['id'], task['id'], task['task_type'],
                    origin_endpoint['id'], destination_endpoint['id'])
                return worker_service
            except Exception as ex:
                LOG.warn(
                    "Failed to schedule task with ID '%s' (attempt %d/%d). "
                    "Waiting %d seconds and then retrying. Error was: %s",
                    task['id'], i+1, retry_count, retry_period,
                    utils.get_exception_details())
                time.sleep(retry_period)

        message = (
            "Failed to schedule task %s after %d tries. This may indicate that"
            " there are no Coriolis Worker services able to perform the task "
            "on the platforms and in the Coriolis Regions required by the "
            "selected source/destination Coriolis Endpoints. Please review"
            " the Conductor and Scheduler logs for more exact details." % (
                task['id'], retry_count))
        # db_api.set_task_status(
        #     ctxt, task.id, constants.TASK_STATUS_FAILED_TO_SCHEDULE,
        #     exception_details=message)
        raise exception.NoSuitableWorkerServiceError(message)
