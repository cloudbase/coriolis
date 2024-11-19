# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_policy import policy

from coriolis.policies import base


TRANSFER_EXECUTIONS_POLICY_PREFIX = "%s:transfer_executions" % (
    base.CORIOLIS_POLICIES_PREFIX)
TRANSFER_EXECUTIONS_POLICY_DEFAULT_RULE = "rule:admin_or_owner"


def get_transfer_executions_policy_label(rule_label):
    return "%s:%s" % (
        TRANSFER_EXECUTIONS_POLICY_PREFIX, rule_label)


TRANSFER_EXECUTIONS_POLICY_DEFAULT_RULES = [
    policy.DocumentedRuleDefault(
        get_transfer_executions_policy_label('create'),
        TRANSFER_EXECUTIONS_POLICY_DEFAULT_RULE,
        "Create a new execution for a given Transfer",
        [
            {
                "path": "/transfers/{transfer_id}/executions",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_transfer_executions_policy_label('list'),
        TRANSFER_EXECUTIONS_POLICY_DEFAULT_RULE,
        "List Executions for a given Transfer",
        [
            {
                "path": "/transfers/{transfer_id}/executions",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_transfer_executions_policy_label('show'),
        TRANSFER_EXECUTIONS_POLICY_DEFAULT_RULE,
        "Show details for Transfer execution",
        [
            {
                "path": "/transfers/{transfer_id}/executions/{execution_id}",
                "method": "GET"
            }
        ]
    ),
    # TODO(aznashwan): transfer actions should ideally be
    # declared in a separate module
    policy.DocumentedRuleDefault(
        get_transfer_executions_policy_label('cancel'),
        TRANSFER_EXECUTIONS_POLICY_DEFAULT_RULE,
        "Cancel a Transfer execution",
        [
            {
                "path": (
                    "/transfers/{transfer_id}/executions/"
                    "{execution_id}/actions"),
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_transfer_executions_policy_label('delete'),
        TRANSFER_EXECUTIONS_POLICY_DEFAULT_RULE,
        "Delete an execution for a given Transfer",
        [
            {
                "path": "/transfers/{transfer_id}/executions/{execution_id}",
                "method": "DELETE"
            }
        ]
    )
]


def list_rules():
    return TRANSFER_EXECUTIONS_POLICY_DEFAULT_RULES
