# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging
from webob import exc

from coriolis import constants
from coriolis import exception
from coriolis.api.v1.views import minion_pool_view
from coriolis.api.v1 import utils as api_utils
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

    def _check_pool_retention_strategy(self, pool_retention_strategy):
        if not pool_retention_strategy:
            LOG.debug(
                "Ignoring void minion pool retention strategy '%s'",
                pool_retention_strategy)
        valid_strats = [
            constants.MINION_POOL_MACHINE_RETENTION_STRATEGY_DELETE,
            constants.MINION_POOL_MACHINE_RETENTION_STRATEGY_POWEROFF]
        if pool_retention_strategy not in valid_strats:
            raise Exception(
                "Invalid minion pool retention strategy '%s'. Must be one of "
                "the following: %s" % (pool_retention_strategy, valid_strats))

    def _check_pool_numeric_values(
            self, minimum_minions, maximum_minions, minion_max_idle_time):
        if minimum_minions is not None:
            if minimum_minions <= 0:
                raise Exception(
                    "'minimum_minions' must be a strictly positive integer. "
                    "Got: %s" % minimum_minions)
        if maximum_minions is not None:
            if maximum_minions <= 0:
                raise Exception(
                    "'maximum_minions' must be a strictly positive integer. "
                    "Got: %s" % maximum_minions)
            if maximum_minions < minimum_minions:
                raise Exception(
                    "'maximum_minions' value (%s) must be at least as large as"
                    " the 'minimum_minions' value (%s)." % (
                        maximum_minions, minimum_minions))
        if minion_max_idle_time is not None:
            if minion_max_idle_time <= 0:
                raise Exception(
                    "'minion_max_idle_time' must be a strictly positive "
                    "integer. Got: %s" % maximum_minions)

    @api_utils.format_keyerror_message(resource='minion_pool', method='create')
    def _validate_create_body(self, ctxt, body):
        minion_pool = body["minion_pool"]
        name = minion_pool["name"]
        endpoint_id = minion_pool["endpoint_id"]
        pool_os_type = minion_pool["os_type"]
        if pool_os_type not in constants.VALID_OS_TYPES:
            raise Exception(
                "The provided pool OS type '%s' is invalid. Must be one "
                "of the following: %s" % (
                    pool_os_type, constants.VALID_OS_TYPES))
        pool_platform = minion_pool["platform"]
        supported_pool_platforms = [
            constants.PROVIDER_PLATFORM_SOURCE,
            constants.PROVIDER_PLATFORM_DESTINATION]
        if pool_platform not in supported_pool_platforms:
            raise Exception(
                "The provided pool platform ('%s') is invalid. Must be one"
                " of the following: %s" % (
                    pool_platform, supported_pool_platforms))
        if pool_platform == constants.PROVIDER_PLATFORM_SOURCE and (
                pool_os_type != constants.OS_TYPE_LINUX):
            raise Exception(
                "Source Minion Pools are required to be of OS type "
                "'%s', not '%s'." % (
                    constants.OS_TYPE_LINUX, pool_os_type))
        environment_options = minion_pool["environment_options"]
        if pool_platform == constants.PROVIDER_PLATFORM_SOURCE:
            self._endpoints_api.validate_endpoint_source_minion_pool_options(
                ctxt, endpoint_id, environment_options)
        elif pool_platform == constants.PROVIDER_PLATFORM_DESTINATION:
            self._endpoints_api.validate_endpoint_destination_minion_pool_options(
                ctxt, endpoint_id, environment_options)

        minimum_minions = minion_pool.get("minimum_minions", 1)
        maximum_minions = minion_pool.get(
            "maximum_minions", minimum_minions)
        minion_max_idle_time = minion_pool.get(
            "minion_max_idle_time", 1)
        self._check_pool_numeric_values(
            minimum_minions, maximum_minions, minion_max_idle_time)
        minion_retention_strategy = minion_pool.get(
            "minion_retention_strategy",
            constants.MINION_POOL_MACHINE_RETENTION_STRATEGY_DELETE)
        self._check_pool_retention_strategy(
            minion_retention_strategy)
        notes = minion_pool.get("notes")

        skip_allocation = minion_pool.get('skip_allocation', False)
        return (
            name, endpoint_id, pool_platform, pool_os_type,
            environment_options, minimum_minions, maximum_minions,
            minion_max_idle_time, minion_retention_strategy, notes,
            skip_allocation)

    def create(self, req, body):
        context = req.environ["coriolis.context"]
        context.can(pools_policies.get_minion_pools_policy_label("create"))
        (name, endpoint_id, pool_platform, pool_os_type, environment_options,
         minimum_minions, maximum_minions, minion_max_idle_time,
         minion_retention_strategy, notes, skip_allocation) = (
            self._validate_create_body(context, body))
        return minion_pool_view.single(req, self._minion_pool_api.create(
            context, name, endpoint_id, pool_platform, pool_os_type,
            environment_options, minimum_minions, maximum_minions,
            minion_max_idle_time, minion_retention_strategy, notes=notes,
            skip_allocation=skip_allocation))

    @api_utils.format_keyerror_message(resource='minion_pool', method='update')
    def _validate_update_body(self, id, context, body):
        minion_pool = body["minion_pool"]
        if 'endpoint_id' in minion_pool:
            raise Exception(
                "The 'endpoint_id' of a minion pool cannot be updated.")
        if 'platform' in minion_pool:
            raise Exception(
                "The 'platform' of a minion pool cannot be updated.")
        vals = {k: minion_pool[k] for k in minion_pool.keys() &
                {"name", "environment_options", "minimum_minions",
                 "maximum_minions", "minion_max_idle_time",
                 "minion_retention_strategy", "notes", "os_type"}}
        if 'minion_retention_strategy' in vals:
            self._check_pool_retention_strategy(
                vals['minion_retention_strategy'])
        if any([
                f in vals for f in [
                    'environment_options', 'minimum_minions',
                    'maximum_minions', 'minion_max_idle_time']]):
            minion_pool = self._minion_pool_api.get_minion_pool(
                context, id)
            self._check_pool_numeric_values(
                vals.get(
                    'minimum_minions', minion_pool['minimum_minions']),
                vals.get(
                    'maximum_minions', minion_pool['maximum_minions']),
                vals.get('minion_max_idle_time'))

            if 'environment_options' in vals:
                if minion_pool['platform'] == (
                        constants.PROVIDER_PLATFORM_SOURCE):
                    self._endpoints_api.validate_endpoint_source_minion_pool_options(
                        context, minion_pool['endpoint_id'],
                        vals['environment_options'])
                elif minion_pool['platform'] == (
                        constants.PROVIDER_PLATFORM_DESTINATION):
                    self._endpoints_api.validate_endpoint_destination_minion_pool_options(
                        context, minion_pool['endpoint_id'],
                        vals['environment_options'])
                else:
                    raise Exception(
                        "Unknown pool platform: %s" % minion_pool[
                            'platform'])
        return vals

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
