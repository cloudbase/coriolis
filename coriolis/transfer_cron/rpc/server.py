# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

import json

from oslo_log import log as logging
from oslo_utils import timeutils

from coriolis.conductor.rpc import client as rpc_client
from coriolis import context
from coriolis.cron import cron
from coriolis import exception
from coriolis import utils

LOG = logging.getLogger(__name__)

VERSION = "1.0"


def _trigger_transfer(ctxt, conductor_client, transfer_id, shutdown_instance,
                      auto_deploy):
    try:
        execution = conductor_client.execute_transfer_tasks(
            ctxt, transfer_id, shutdown_instance=shutdown_instance,
            auto_deploy=auto_deploy)
        result_msg = 'Execution %s for Transfer %s' % (
            execution.get('id'), execution.get('action_id'))
        return result_msg
    except (exception.InvalidTransferState,
            exception.InvalidActionTasksExecutionState):
        LOG.info("A replica or migration already running")


class TransferCronServerEndpoint(object):

    def __init__(self):
        self._rpc_client = rpc_client.ConductorClient()
        # Setup cron loop
        self._cron = cron.Cron()
        self._admin_ctx = context.get_admin_context()
        self._init_cron()

    def _deserialize_schedule(self, sched):
        expires = sched.get("expiration_date")
        if expires:
            sched["expiration_date"] = timeutils.normalize_time(
                timeutils.parse_isotime(expires))
        tmp = sched["schedule"]
        if type(tmp) is str:
            sched["schedule"] = json.loads(tmp)
        return sched

    def _register_schedule(self, schedule, date=None):
        date = date or timeutils.utcnow()
        sched = self._deserialize_schedule(schedule)
        expires = sched.get("expiration_date")
        if expires and expires <= date:
            LOG.info("Not registering expired schedule: %s" % sched["id"])
            return
        trust_ctxt = context.get_admin_context(
            trust_id=schedule["trust_id"])
        description = "Scheduled job for %s" % sched["id"]
        job = cron.CronJob(
            sched["id"], description, sched["schedule"],
            sched["enabled"], sched["expiration_date"],
            None, None, _trigger_transfer, trust_ctxt,
            self._rpc_client, schedule["transfer_id"],
            schedule["shutdown_instance"], schedule["auto_deploy"])
        self._cron.register(job)

    def _init_cron(self):
        now = timeutils.utcnow()
        schedules = self._get_all_schedules()
        for schedule in schedules:
            try:
                self._register_schedule(schedule, date=now)
            except Exception as err:
                # NOTE(gsamfira): If we fail here, the service will
                # not be able to start. Should we fail here because
                # of an invalid schedule that managed to creep its
                # way into the DB, or just ignore that one schedule?
                LOG.exception(err)
        self._cron.start()

    def _get_all_schedules(self):
        schedules = self._rpc_client.get_transfer_schedules(
            self._admin_ctx, expired=False)
        return schedules

    def register(self, ctxt, schedule):
        now = timeutils.utcnow()
        LOG.debug("Registering new schedule %s: %r" % (
            schedule["id"], schedule["schedule"]))
        self._register_schedule(schedule, date=now)

    def unregister(self, ctxt, schedule):
        schedule_id = schedule["id"]
        LOG.debug("removing schedule %s" % schedule_id)
        self._cron.unregister(schedule_id)

    def get_diagnostics(self, ctxt):
        return utils.get_diagnostics_info()
