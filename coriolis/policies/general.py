# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_policy import policy

from coriolis.policies import base


PROVIDERS_POLICY_PREFIX = "%s:providers" % base.CORIOLIS_POLICIES_PREFIX
PROVIDERS_POLICY_DEFAULT_RULE = "rule:admin_or_owner"


def get_providers_policy_label(rule_label):
    return "%s:%s" % (
        PROVIDERS_POLICY_PREFIX, rule_label)


PROVIDERS_POLICY_DEFAULT_RULES = [
    policy.DocumentedRuleDefault(
        get_providers_policy_label('list'),
        PROVIDERS_POLICY_DEFAULT_RULE,
        "List available provider plugins and their capabilities",
        [
            {
                "path": "/providers",
                "method": "GET"
            }
        ]
    )
]


def list_rules():
    return PROVIDERS_POLICY_DEFAULT_RULES
