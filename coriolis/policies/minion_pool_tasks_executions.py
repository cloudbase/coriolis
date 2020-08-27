# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_policy import policy

from coriolis.policies import base


MINION_POOL_EXECUTIONS_POLICY_PREFIX = "%s:minion_pool_executions" % (
    base.CORIOLIS_POLICIES_PREFIX)
MINION_POOL_EXECUTIONS_POLICY_DEFAULT_RULE = "rule:admin_or_owner"


def get_minion_pool_executions_policy_label(rule_label):
    return "%s:%s" % (
        MINION_POOL_EXECUTIONS_POLICY_PREFIX, rule_label)


MINION_POOL_EXECUTIONS_POLICY_DEFAULT_RULES = [
    policy.DocumentedRuleDefault(
        get_minion_pool_executions_policy_label('create'),
        MINION_POOL_EXECUTIONS_POLICY_DEFAULT_RULE,
        "Create a new execution for a given Minion Pool",
        [
            {
                "path": "/minion_pools/{minion_pool_id}/executions",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_minion_pool_executions_policy_label('list'),
        MINION_POOL_EXECUTIONS_POLICY_DEFAULT_RULE,
        "List Executions for a given Minion Pool",
        [
            {
                "path": "/minion_pools/{minion_pool_id}/executions",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_minion_pool_executions_policy_label('show'),
        MINION_POOL_EXECUTIONS_POLICY_DEFAULT_RULE,
        "Show details for Minion Pool execution",
        [
            {
                "path": "/minion_pools/{minion_pool_id}/executions/{execution_id}",
                "method": "GET"
            }
        ]
    ),
    # TODO(aznashwan): minion pool execution actions should ideally be
    # declared in a separate module
    policy.DocumentedRuleDefault(
        get_minion_pool_executions_policy_label('cancel'),
        MINION_POOL_EXECUTIONS_POLICY_DEFAULT_RULE,
        "Cancel a Minion Pool execution",
        [
            {
                "path": (
                    "/minion_pools/{minion_pool_id}/executions/"
                    "{execution_id}/actions"),
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_minion_pool_executions_policy_label('delete'),
        MINION_POOL_EXECUTIONS_POLICY_DEFAULT_RULE,
        "Delete an execution for a given Minion Pool",
        [
            {
                "path": "/minion_pools/{minion_pool_id}/executions/{execution_id}",
                "method": "DELETE"
            }
        ]
    )
]


def list_rules():
    return MINION_POOL_EXECUTIONS_POLICY_DEFAULT_RULES
