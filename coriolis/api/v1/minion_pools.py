# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging
from webob import exc

from coriolis import exception
from coriolis.api.v1.views import minion_pool_view
from coriolis.api.v1.views import minion_pool_tasks_execution_view
from coriolis.api import wsgi as api_wsgi
from coriolis.policies import minion_pools as pools_policies
from coriolis.minion_pools import api

LOG = logging.getLogger(__name__)


class MinionPoolController(api_wsgi.Controller):
    def __init__(self):
        self._minion_pool_api = api.API()
        super(MinionPoolController, self).__init__()

    def show(self, req, id):
        context = req.environ["coriolis.context"]
        context.can(pools_policies.get_minion_pools_policy_label("show"))
        minion_pool = self._minion_pool_api.get_minion_pool(context, id)
        if not minion_pool:
            raise exc.HTTPNotFound()

        return minion_pool_view.single(req, minion_pool)

    def index(self, req):
        context = req.environ["coriolis.context"]
        context.can(pools_policies.get_minion_pools_policy_label("list"))
        return minion_pool_view.collection(
            req, self._minion_pool_api.get_minion_pools(context))

    def _validate_create_body(self, body):
        try:
            minion_pool = body["minion_pool"]
            name = minion_pool["pool_name"]
            endpoint_id = minion_pool["endpoint_id"]
            # TODO(aznashwan): validate pool schema:
            environment_options = minion_pool["environment_options"]
            minimum_minions = minion_pool.get("minimum_minions", 0)
            maximum_minions = minion_pool.get("maximum_minions", 1)
            minion_max_idle_time = minion_pool.get(
                "minion_max_idle_time", 1)
            minion_retention_strategy = minion_pool.get(
                "minion_retention_strategy")
            notes = minion_pool.get("notes")
            return (
                name, endpoint_id, environment_options, minimum_minions,
                maximum_minions, minion_max_idle_time,
                minion_retention_strategy, notes)
        except Exception as ex:
            LOG.exception(ex)
            if hasattr(ex, "message"):
                msg = ex.message
            else:
                msg = str(ex)
            raise exception.InvalidInput(msg)

    def create(self, req, body):
        context = req.environ["coriolis.context"]
        context.can(pools_policies.get_minion_pools_policy_label("create"))
        (name, endpoint_id, environment_options, minimum_minions,
         maximum_minions, minion_max_idle_time, minion_retention_strategy,
         notes) = (
            self._validate_create_body(body))
        return minion_pool_view.single(req, self._minion_pool_api.create(
            context, name, endpoint_id, environment_options, minimum_minions,
            maximum_minions, minion_max_idle_time, minion_retention_strategy,
            notes=notes))

    def _validate_update_body(self, body):
        try:
            minion_pool = body["minion_pool"]
            return {k: minion_pool[k] for k in minion_pool.keys() &
                    {"name", "environment_options", "minimum_minions",
                     "maximum_minions", "minion_max_idle_time",
                     "minion_retention_strategy", "notes"}}
        except Exception as ex:
            LOG.exception(ex)
            if hasattr(ex, "message"):
                msg = ex.message
            else:
                msg = str(ex)
            raise exception.InvalidInput(msg)

    def update(self, req, id, body):
        context = req.environ["coriolis.context"]
        context.can(pools_policies.get_minion_pools_policy_label("update"))
        updated_values = self._validate_update_body(body)
        return minion_pool_tasks_execution_view.single(
            req, self._minion_pool_api.update(
                req.environ['coriolis.context'], id, updated_values))

    def delete(self, req, id):
        context = req.environ["coriolis.context"]
        context.can(pools_policies.get_minion_pools_policy_label("delete"))
        try:
            self._minion_pool_api.delete(req.environ['coriolis.context'], id)
            raise exc.HTTPNoContent()
        except exception.NotFound as ex:
            raise exc.HTTPNotFound(explanation=ex.msg)


def create_resource():
    return api_wsgi.Resource(MinionPoolController())
