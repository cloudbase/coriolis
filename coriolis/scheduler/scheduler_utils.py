# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import random

from oslo_log import log as logging

from coriolis import constants
from coriolis.db import api as db_api
from coriolis import exception
from coriolis.scheduler.rpc import client as rpc_scheduler_client
from coriolis.transfer_cron.rpc import client as rpc_cron_client
from coriolis.worker.rpc import client as rpc_worker_client


VERSION = "1.0"

LOG = logging.getLogger(__name__)

RPC_TOPIC_TO_CLIENT_CLASS_MAP = {
    constants.WORKER_MAIN_MESSAGING_TOPIC: rpc_worker_client.WorkerClient,
    constants.SCHEDULER_MAIN_MESSAGING_TOPIC: (
        rpc_scheduler_client.SchedulerClient),
    constants.TRANSFER_CRON_MAIN_MESSAGING_TOPIC: (
        rpc_cron_client.TransferCronClient)
}


def get_rpc_client_for_service(service, *client_args, **client_kwargs):
    rpc_client_class = RPC_TOPIC_TO_CLIENT_CLASS_MAP.get(service.topic)
    if not rpc_client_class:
        raise exception.NotFound(
            "No RPC client class for service with topic '%s'." % (
                service.topic))

    topic = service.topic
    if service.topic == constants.WORKER_MAIN_MESSAGING_TOPIC:
        # NOTE: coriolis.service.MessagingService-type services (such
        # as the worker), always have a dedicated per-host queue
        # which can be used to target the service:
        topic = constants.SERVICE_MESSAGING_TOPIC_FORMAT % ({
            "main_topic": constants.WORKER_MAIN_MESSAGING_TOPIC,
            "host": service.host})

    return rpc_client_class(*client_args, topic=topic, **client_kwargs)


def get_any_worker_service(
        scheduler_client, ctxt, random_choice=False, raw_dict=False):
    services = scheduler_client.get_workers_for_specs(ctxt)
    if not services:
        raise exception.NoWorkerServiceError()
    service = services[0]
    if random_choice:
        service = random.choice(services)
    if raw_dict:
        return service
    return db_api.get_service(ctxt, service['id'])


def get_worker_rpc_for_host(host, *client_args, **client_kwargs):
    rpc_client_class = RPC_TOPIC_TO_CLIENT_CLASS_MAP[
        constants.WORKER_MAIN_MESSAGING_TOPIC]
    topic = constants.SERVICE_MESSAGING_TOPIC_FORMAT % ({
        "main_topic": constants.WORKER_MAIN_MESSAGING_TOPIC,
        "host": host})
    return rpc_client_class(*client_args, topic=topic, **client_kwargs)
