# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

import itertools

from oslo_config import cfg as conf
from oslo_log import log as logging
from oslo_policy import policy

from coriolis import exception
from coriolis.policies import base
from coriolis.policies import deployments
from coriolis.policies import diagnostics
from coriolis.policies import endpoints
from coriolis.policies import general
from coriolis.policies import migrations
from coriolis.policies import minion_pools
from coriolis.policies import regions
from coriolis.policies import replica_schedules
from coriolis.policies import replica_tasks_executions
from coriolis.policies import replicas
from coriolis.policies import services
from coriolis import utils


LOG = logging.getLogger(__name__)

CONF = conf.CONF
_ENFORCER = None

DEFAULT_POLICIES_MODULES = [
    base, deployments, endpoints, general, migrations, replicas,
    replica_schedules, replica_tasks_executions, diagnostics, regions,
    services, minion_pools]


def reset():
    global _ENFORCER
    if _ENFORCER:
        _ENFORCER.clear()
    _ENFORCER = None


def init():
    global _ENFORCER
    global saved_file_rules

    if not _ENFORCER:
        _ENFORCER = policy.Enforcer(CONF)
        register_rules(_ENFORCER)
        _ENFORCER.load_rules()


def register_rules(enforcer):
    enforcer.register_defaults(itertools.chain(*[
        m.list_rules() for m in DEFAULT_POLICIES_MODULES]))


def get_enforcer():
    init()
    return _ENFORCER


def check_policy_for_context(
        context, action, target, exc=None, do_raise=True):
    """ Checks the validity of the given action of the given target based on
    set policies.
    On success, returns a value where bool(val) == True.
    On failure and if `do_raise` is False, returns False.
    Raises `exception.PolicyNotAuthorized` or `exc` if the policy is
    not authorized.
    """
    init()
    credentials = context.to_policy_values()
    if not exc:
        exc = exception.PolicyNotAuthorized
    try:
        result = _ENFORCER.authorize(
            action, target, credentials,
            do_raise=do_raise, exc=exc, action=action)
    except Exception as ex:
        LOG.debug(
            "Policy check for '%(action)s' with target '%(target)s' failed "
            "with credentials: %(credentials)s.\nException: '%(trace)s'", {
                'action': action, 'target': target,
                'credentials': credentials, 'trace':
                utils.get_exception_details()})

        raise exc(str(ex))

    return result
