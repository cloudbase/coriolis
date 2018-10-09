# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_policy import policy

from coriolis.policies import base


REPLICA_SCHEDULES_POLICY_PREFIX = "%s:replica_schedules" % (
    base.CORIOLIS_POLICIES_PREFIX)
REPLICA_SCHEDULES_POLICY_DEFAULT_RULE = "rule:admin_or_owner"


def get_replica_schedules_policy_label(rule_label):
    return "%s:%s" % (
        REPLICA_SCHEDULES_POLICY_PREFIX, rule_label)


REPLICA_SCHEDULES_POLICY_DEFAULT_RULES = [
    policy.DocumentedRuleDefault(
        get_replica_schedules_policy_label('create'),
        REPLICA_SCHEDULES_POLICY_DEFAULT_RULE,
        "Create a new execution schedule for a given Replica",
        [
            {
                "path": "/replicas/{replica_id}/schedules",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_replica_schedules_policy_label('list'),
        REPLICA_SCHEDULES_POLICY_DEFAULT_RULE,
        "List execution schedules for a given Replica",
        [
            {
                "path": "/replicas/{replica_id}/schedules",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_replica_schedules_policy_label('show'),
        REPLICA_SCHEDULES_POLICY_DEFAULT_RULE,
        "Show details for an execution schedule for a given Replica",
        [
            {
                "path": "/replicas/{replica_id}/schedules/{schedule_id}",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_replica_schedules_policy_label('update'),
        REPLICA_SCHEDULES_POLICY_DEFAULT_RULE,
        "Update an existing execution schedule for a given Replica",
        [
            {
                "path": (
                    "/replicas/{replica_id}/schedules/{schedule_id}"),
                "method": "PUT"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_replica_schedules_policy_label('delete'),
        REPLICA_SCHEDULES_POLICY_DEFAULT_RULE,
        "Delete an execution schedule for a given Replica",
        [
            {
                "path": "/replicas/{replica_id}/schedules/{schedule_id}",
                "method": "DELETE"
            }
        ]
    )
]


def list_rules():
    return REPLICA_SCHEDULES_POLICY_DEFAULT_RULES
