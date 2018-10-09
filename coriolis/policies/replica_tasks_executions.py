# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_policy import policy

from coriolis.policies import base


REPLICA_EXECUTIONS_POLICY_PREFIX = "%s:replica_executions" % (
    base.CORIOLIS_POLICIES_PREFIX)
REPLICA_EXECUTIONS_POLICY_DEFAULT_RULE = "rule:admin_or_owner"


def get_replica_executions_policy_label(rule_label):
    return "%s:%s" % (
        REPLICA_EXECUTIONS_POLICY_PREFIX, rule_label)


REPLICA_EXECUTIONS_POLICY_DEFAULT_RULES = [
    policy.DocumentedRuleDefault(
        get_replica_executions_policy_label('create'),
        REPLICA_EXECUTIONS_POLICY_DEFAULT_RULE,
        "Create a new execution for a given Replica",
        [
            {
                "path": "/replicas/{replica_id}/executions",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_replica_executions_policy_label('list'),
        REPLICA_EXECUTIONS_POLICY_DEFAULT_RULE,
        "List Executions for a given Replica",
        [
            {
                "path": "/replicas/{replica_id}/executions",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_replica_executions_policy_label('show'),
        REPLICA_EXECUTIONS_POLICY_DEFAULT_RULE,
        "Show details for Replica execution",
        [
            {
                "path": "/replicas/{replica_id}/executions/{execution_id}",
                "method": "GET"
            }
        ]
    ),
    # TODO(aznashwan): replica actions should ideally be
    # declared in a separate module
    policy.DocumentedRuleDefault(
        get_replica_executions_policy_label('cancel'),
        REPLICA_EXECUTIONS_POLICY_DEFAULT_RULE,
        "Cancel a Replica execution",
        [
            {
                "path": (
                    "/replicas/{replica_id}/executions/"
                    "{execution_id}/actions"),
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_replica_executions_policy_label('delete'),
        REPLICA_EXECUTIONS_POLICY_DEFAULT_RULE,
        "Delete an execution for a given Replica",
        [
            {
                "path": "/replicas/{replica_id}/executions/{execution_id}",
                "method": "DELETE"
            }
        ]
    )
]


def list_rules():
    return REPLICA_EXECUTIONS_POLICY_DEFAULT_RULES
