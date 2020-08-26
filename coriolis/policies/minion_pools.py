# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.


from oslo_policy import policy

from coriolis.policies import base


MINION_POOLS_POLICY_PREFIX = "%s:minion_pools" % base.CORIOLIS_POLICIES_PREFIX
MINION_POOLS_DEFAULT_RULE = "rule:admin_or_owner"


def get_minion_pools_policy_label(rule_label):
    return "%s:%s" % (
        MINION_POOLS_POLICY_PREFIX, rule_label)


MINION_POOLS_DEFAULT_RULES = [
    policy.DocumentedRuleDefault(
        get_minion_pools_policy_label('create'),
        MINION_POOLS_DEFAULT_RULE,
        "Create a minion_pool",
        [
            {
                "path": "/minion_pools",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_minion_pools_policy_label('list'),
        MINION_POOLS_DEFAULT_RULE,
        "List minion_pools",
        [
            {
                "path": "/minion_pools",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_minion_pools_policy_label('show'),
        MINION_POOLS_DEFAULT_RULE,
        "Show details for minion_pool",
        [
            {
                "path": "/minion_pools/{minion_pool_id}",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_minion_pools_policy_label('update'),
        MINION_POOLS_DEFAULT_RULE,
        "Update details for minion_pool",
        [
            {
                "path": "/minion_pools/{minion_pool_id}",
                "method": "PUT"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_minion_pools_policy_label('delete'),
        MINION_POOLS_DEFAULT_RULE,
        "Delete minion_pool",
        [
            {
                "path": "/minion_pools/{minion_pool_id}",
                "method": "DELETE"
            }
        ]
    )
]


def list_rules():
    return MINION_POOLS_DEFAULT_RULES
