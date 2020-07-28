# Copyright 2020 Cloudbase Solutions Srl
# All Rights Reserved.


from oslo_policy import policy

from coriolis.policies import base


REGIONS_POLICY_PREFIX = "%s:regions" % base.CORIOLIS_POLICIES_PREFIX
REGIONS_POLICY_DEFAULT_RULE = "rule:admin_or_owner"


def get_regions_policy_label(rule_label):
    return "%s:%s" % (
        REGIONS_POLICY_PREFIX, rule_label)


REGIONS_POLICY_DEFAULT_RULES = [
    policy.DocumentedRuleDefault(
        get_regions_policy_label('create'),
        REGIONS_POLICY_DEFAULT_RULE,
        "Create a region",
        [
            {
                "path": "/regions",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_regions_policy_label('list'),
        REGIONS_POLICY_DEFAULT_RULE,
        "List regions",
        [
            {
                "path": "/regions",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_regions_policy_label('show'),
        REGIONS_POLICY_DEFAULT_RULE,
        "Show details for region",
        [
            {
                "path": "/regions/{region_id}",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_regions_policy_label('update'),
        REGIONS_POLICY_DEFAULT_RULE,
        "Update details for region",
        [
            {
                "path": "/regions/{region_id}",
                "method": "PUT"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_regions_policy_label('delete'),
        REGIONS_POLICY_DEFAULT_RULE,
        "Delete region",
        [
            {
                "path": "/regions/{region_id}",
                "method": "DELETE"
            }
        ]
    )
]


def list_rules():
    return REGIONS_POLICY_DEFAULT_RULES
