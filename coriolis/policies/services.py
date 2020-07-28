# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.


from oslo_policy import policy

from coriolis.policies import base


SERVICES_POLICY_PREFIX = "%s:services" % base.CORIOLIS_POLICIES_PREFIX
SERVICES_POLICY_DEFAULT_RULE = "rule:admin_or_owner"


def get_services_policy_label(rule_label):
    return "%s:%s" % (
        SERVICES_POLICY_PREFIX, rule_label)


SERVICES_POLICY_DEFAULT_RULES = [
    policy.DocumentedRuleDefault(
        get_services_policy_label('create'),
        SERVICES_POLICY_DEFAULT_RULE,
        "Create a service",
        [
            {
                "path": "/services",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_services_policy_label('list'),
        SERVICES_POLICY_DEFAULT_RULE,
        "List services",
        [
            {
                "path": "/services",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_services_policy_label('show'),
        SERVICES_POLICY_DEFAULT_RULE,
        "Show details for service",
        [
            {
                "path": "/services/{service_id}",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_services_policy_label('update'),
        SERVICES_POLICY_DEFAULT_RULE,
        "Update details for service",
        [
            {
                "path": "/services/{service_id}",
                "method": "PUT"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_services_policy_label('delete'),
        SERVICES_POLICY_DEFAULT_RULE,
        "Delete service",
        [
            {
                "path": "/services/{service_id}",
                "method": "DELETE"
            }
        ]
    )
]


def list_rules():
    return SERVICES_POLICY_DEFAULT_RULES
