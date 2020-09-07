# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging
from webob import exc

from coriolis import constants
from coriolis import exception
from coriolis.api.v1.views import minion_pool_view
from coriolis.api.v1.views import minion_pool_tasks_execution_view
from coriolis.api import wsgi as api_wsgi
from coriolis.endpoints import api as endpoints_api
from coriolis.policies import minion_pools as pools_policies
from coriolis.minion_pools import api

LOG = logging.getLogger(__name__)


class MinionPoolController(api_wsgi.Controller):
    def __init__(self):
        self._minion_pool_api = api.API()
        self._endpoints_api = endpoints_api.API()
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

    def _validate_create_body(self, ctxt, body):
        try:
            minion_pool = body["minion_pool"]
            name = minion_pool["pool_name"]
            endpoint_id = minion_pool["endpoint_id"]
            pool_os_type = minion_pool["pool_os_type"]
            if pool_os_type not in constants.VALID_OS_TYPES:
                raise Exception(
                    "The provided pool OS type '%s' is invalid. Must be one "
                    "of the following: %s" % (
                        pool_os_type, constants.VALID_OS_TYPES))
            environment_options = minion_pool["environment_options"]
            self._endpoints_api.validate_endpoint_minion_pool_options(
                ctxt, endpoint_id, environment_options)

            minimum_minions = minion_pool.get("minimum_minions", 1)
            maximum_minions = minion_pool.get(
                "maximum_minions", minimum_minions)
            minion_max_idle_time = minion_pool.get(
                "minion_max_idle_time", 1)
            minion_retention_strategy = minion_pool.get(
                "minion_retention_strategy",
                constants.MINION_POOL_MACHINE_RETENTION_STRATEGY_DELETE)
            suppoted_retention_strategies = [
                constants.MINION_POOL_MACHINE_RETENTION_STRATEGY_DELETE]
            if minion_retention_strategy not in suppoted_retention_strategies:
                raise Exception(
                    "Unsupported minion retention strategy '%s'. Must be "
                    "one of: %s" % (
                        minion_retention_strategy,
                        suppoted_retention_strategies))
            notes = minion_pool.get("notes")
            return (
                name, endpoint_id, pool_os_type, environment_options,
                minimum_minions, maximum_minions, minion_max_idle_time,
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
        (name, endpoint_id, pool_os_type, environment_options, minimum_minions,
         maximum_minions, minion_max_idle_time, minion_retention_strategy,
         notes) = (
            self._validate_create_body(context, body))
        return minion_pool_view.single(req, self._minion_pool_api.create(
            context, name, endpoint_id, pool_os_type, environment_options,
            minimum_minions, maximum_minions, minion_max_idle_time,
            minion_retention_strategy, notes=notes))

    def _validate_update_body(self, id, context, body):
        try:
            minion_pool = body["minion_pool"]
            if 'endpoint_id' in minion_pool:
                raise exception.InvalidInput(
                    "The 'endpoint_id' of a minion pool cannot be "
                    "updated.")
            vals = {k: minion_pool[k] for k in minion_pool.keys() &
                    {"name", "environment_options", "minimum_minions",
                     "maximum_minions", "minion_max_idle_time",
                     "minion_retention_strategy", "notes", "pool_os_type"}}
            if 'environment_options' in vals:
                minion_pool = self._minion_pool_api.get_minion_pool(
                    context, id)
                self._endpoints_api.validate_endpoint_minion_pool_options(
                    # TODO(aznashwan): remove endpoint ID fields reduncancy
                    # once DB models are overhauled:
                    context, minion_pool['origin_endpoint_id'],
                    vals['environment_options'])
            return vals
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
        updated_values = self._validate_update_body(id, context, body)
        return minion_pool_view.single(
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
