# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_policy import policy

from coriolis.policies import base


REPLICAS_POLICY_PREFIX = "%s:replicas" % base.CORIOLIS_POLICIES_PREFIX
REPLICAS_POLICY_DEFAULT_RULE = "rule:admin_or_owner"


def get_replicas_policy_label(rule_label):
    return "%s:%s" % (
        REPLICAS_POLICY_PREFIX, rule_label)


REPLICAS_POLICY_DEFAULT_RULES = [
    policy.DocumentedRuleDefault(
        get_replicas_policy_label('create'),
        REPLICAS_POLICY_DEFAULT_RULE,
        "Create a Replica",
        [
            {
                "path": "/replicas",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_replicas_policy_label('list'),
        REPLICAS_POLICY_DEFAULT_RULE,
        "List Replicas",
        [
            {
                "path": "/replicas",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_replicas_policy_label('show'),
        REPLICAS_POLICY_DEFAULT_RULE,
        "Show details for Replica",
        [
            {
                "path": "/replicas/{replica_id}",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_replicas_policy_label('show_executions'),
        REPLICAS_POLICY_DEFAULT_RULE,
        "Show details for Replica (including tasks executions)",
        [
            {
                "path": "/replicas/{replica_id}",
                "method": "GET"
            }
        ]
    ),
    # TODO(aznashwan): replica actions should ideally be
    # declared in a separate module
    policy.DocumentedRuleDefault(
        get_replicas_policy_label('delete_disks'),
        REPLICAS_POLICY_DEFAULT_RULE,
        "Delete Replica Disks",
        [
            {
                "path": "/replicas/{replica_id}/actions",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_replicas_policy_label('delete'),
        REPLICAS_POLICY_DEFAULT_RULE,
        "Delete Replica",
        [
            {
                "path": "/replicas/{replica_id}",
                "method": "DELETE"
            }
        ]
    )
]


def list_rules():
    return REPLICAS_POLICY_DEFAULT_RULES
