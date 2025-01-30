# Copyright 2017 Cloudbase Solutions Srl
# All Rights Reserved.

from coriolis.api.v1.views import transfer_schedule_view
from coriolis.api import wsgi as api_wsgi
from coriolis import exception
from coriolis.policies import transfer_schedules as schedules_policies
from coriolis import schemas
from coriolis.transfer_cron import api

import jsonschema
from oslo_log import log as logging
from oslo_utils import strutils
from oslo_utils import timeutils
from webob import exc


LOG = logging.getLogger(__name__)


class TransferScheduleController(api_wsgi.Controller):
    def __init__(self):
        self._schedule_api = api.API()
        super(TransferScheduleController, self).__init__()

    def show(self, req, transfer_id, id):
        context = req.environ["coriolis.context"]
        context.can(
            schedules_policies.get_transfer_schedules_policy_label("show"))
        schedule = self._schedule_api.get_schedule(context, transfer_id, id)
        if not schedule:
            raise exc.HTTPNotFound()

        return transfer_schedule_view.single(schedule)

    def index(self, req, transfer_id):
        context = req.environ["coriolis.context"]
        context.can(
            schedules_policies.get_transfer_schedules_policy_label("list"))

        show_expired = strutils.bool_from_string(
            req.GET.get("show_expired", True), strict=True)
        return transfer_schedule_view.collection(
            self._schedule_api.get_schedules(
                context, transfer_id, expired=show_expired))

    def _validate_schedule(self, schedule):
        schema = schemas.SCHEDULE_API_BODY_SCHEMA["properties"]["schedule"]
        schemas.validate_value(schedule, schema)
        return schedule

    def _validate_expiration_date(self, expiration_date):
        if expiration_date is None:
            return expiration_date
        exp = timeutils.normalize_time(
            timeutils.parse_isotime(expiration_date))
        now = timeutils.utcnow()
        if now > exp:
            raise exception.InvalidInput(
                "expiration_date is in the past")
        return exp

    def _validate_create_body(self, body):
        schedule = body.get("schedule")
        if schedule is None:
            raise exception.InvalidInput(
                "schedule is required")
        schedule = self._validate_schedule(schedule)
        schemas.validate_value(
            body, schemas.SCHEDULE_API_BODY_SCHEMA,
            format_checker=jsonschema.FormatChecker())

        enabled = body.get("enabled", True)
        exp = body.get("expiration_date", None)
        if exp is not None:
            exp = self._validate_expiration_date(exp)
        shutdown = body.get("shutdown_instance", False)
        auto_deploy = body.get('auto_deploy', False)
        return schedule, enabled, exp, shutdown, auto_deploy

    def _validate_update_body(self, update_body):
        body = {}
        schedule = update_body.get("schedule")
        if schedule is not None:
            schedule = self._validate_schedule(schedule)
            body["schedule"] = schedule
        enabled = update_body.get("enabled")
        if enabled is not None:
            body["enabled"] = enabled
        shutdown = update_body.get("shutdown_instance")
        if shutdown is not None:
            body["shutdown_instance"] = shutdown
        auto_deploy = update_body.get('auto_deploy')
        if auto_deploy is not None:
            body['auto_deploy'] = auto_deploy
        schemas.validate_value(
            body, schemas.SCHEDULE_API_BODY_SCHEMA,
            format_checker=jsonschema.FormatChecker())

        exp = None
        if "expiration_date" in update_body:
            exp = self._validate_expiration_date(
                update_body.get("expiration_date"))
            body["expiration_date"] = exp
        return body

    def create(self, req, transfer_id, body):
        context = req.environ["coriolis.context"]
        context.can(
            schedules_policies.get_transfer_schedules_policy_label("create"))

        LOG.debug("Got request: %r %r %r" % (req, transfer_id, body))
        try:
            schedule, enabled, exp_date, shutdown, auto_deploy = (
                self._validate_create_body(body))
        except Exception as err:
            raise exception.InvalidInput(err)

        return transfer_schedule_view.single(self._schedule_api.create(
            context, transfer_id, schedule, enabled, exp_date, shutdown,
            auto_deploy))

    def update(self, req, transfer_id, id, body):
        context = req.environ["coriolis.context"]
        context.can(
            schedules_policies.get_transfer_schedules_policy_label("update"))

        LOG.debug("Got request: %r %r %r %r" % (
            req, transfer_id, id, body))

        try:
            update_values = self._validate_update_body(body)
        except Exception as err:
            raise exception.InvalidInput(err)

        return transfer_schedule_view.single(self._schedule_api.update(
            context, transfer_id, id, update_values))

    def delete(self, req, transfer_id, id):
        context = req.environ["coriolis.context"]
        context.can(
            schedules_policies.get_transfer_schedules_policy_label("delete"))

        self._schedule_api.delete(context, transfer_id, id)
        raise exc.HTTPNoContent()


def create_resource():
    return api_wsgi.Resource(TransferScheduleController())
