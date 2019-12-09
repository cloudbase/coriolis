# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_policy import policy

from coriolis.policies import base


DIAGNOSTICS_POLICY_PREFIX = "%s:diagnostics" % (
    base.CORIOLIS_POLICIES_PREFIX)
DIAGNOSTICS_POLICY_DEFAULT_RULE = "rule:admin_or_owner"


def get_diagnostics_policy_label(rule_label):
    return "%s:%s" % (
        DIAGNOSTICS_POLICY_PREFIX, rule_label)


DIAGNOSTICS_POLICY_DEFAULT_RULES = [
    policy.DocumentedRuleDefault(
        get_diagnostics_policy_label('get'),
        DIAGNOSTICS_POLICY_DEFAULT_RULE,
        "Get diagnostics information from coriolis components",
        [
            {
                "path": "/diagnostics",
                "method": "GET"
            }
        ]
    )
]


def list_rules():
    return DIAGNOSTICS_POLICY_DEFAULT_RULES
