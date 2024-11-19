# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_policy import policy

from coriolis.policies import base


TRANSFER_SCHEDULES_POLICY_PREFIX = "%s:transfer_schedules" % (
    base.CORIOLIS_POLICIES_PREFIX)
TRANSFER_SCHEDULES_POLICY_DEFAULT_RULE = "rule:admin_or_owner"


def get_transfer_schedules_policy_label(rule_label):
    return "%s:%s" % (
        TRANSFER_SCHEDULES_POLICY_PREFIX, rule_label)


TRANSFER_SCHEDULES_POLICY_DEFAULT_RULES = [
    policy.DocumentedRuleDefault(
        get_transfer_schedules_policy_label('create'),
        TRANSFER_SCHEDULES_POLICY_DEFAULT_RULE,
        "Create a new execution schedule for a given Transfer",
        [
            {
                "path": "/transfers/{transfer_id}/schedules",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_transfer_schedules_policy_label('list'),
        TRANSFER_SCHEDULES_POLICY_DEFAULT_RULE,
        "List execution schedules for a given Transfer",
        [
            {
                "path": "/transfers/{transfer_id}/schedules",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_transfer_schedules_policy_label('show'),
        TRANSFER_SCHEDULES_POLICY_DEFAULT_RULE,
        "Show details for an execution schedule for a given Transfer",
        [
            {
                "path": "/transfers/{transfer_id}/schedules/{schedule_id}",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_transfer_schedules_policy_label('update'),
        TRANSFER_SCHEDULES_POLICY_DEFAULT_RULE,
        "Update an existing execution schedule for a given Transfer",
        [
            {
                "path": (
                    "/transfers/{transfer_id}/schedules/{schedule_id}"),
                "method": "PUT"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_transfer_schedules_policy_label('delete'),
        TRANSFER_SCHEDULES_POLICY_DEFAULT_RULE,
        "Delete an execution schedule for a given Transfer",
        [
            {
                "path": "/transfers/{transfer_id}/schedules/{schedule_id}",
                "method": "DELETE"
            }
        ]
    )
]


def list_rules():
    return TRANSFER_SCHEDULES_POLICY_DEFAULT_RULES
