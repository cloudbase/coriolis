# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_policy import policy

from coriolis.policies import base


DEPLOYMENTS_POLICY_PREFIX = "%s:deployments" % base.CORIOLIS_POLICIES_PREFIX
DEPLOYMENTS_POLICY_DEFAULT_RULE = "rule:admin_or_owner"


def get_deployments_policy_label(rule_label):
    return "%s:%s" % (
        DEPLOYMENTS_POLICY_PREFIX, rule_label)


DEPLOYMENTS_POLICY_DEFAULT_RULES = [
    policy.DocumentedRuleDefault(
        get_deployments_policy_label('create'),
        DEPLOYMENTS_POLICY_DEFAULT_RULE,
        "Create a deployment",
        [
            {
                "path": "/deployments",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_deployments_policy_label('list'),
        DEPLOYMENTS_POLICY_DEFAULT_RULE,
        "List deployments",
        [
            {
                "path": "/deployments",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_deployments_policy_label('show'),
        DEPLOYMENTS_POLICY_DEFAULT_RULE,
        "Show details for a deployment",
        [
            {
                "path": "/deployment/{deployment_id}",
                "method": "GET"
            }
        ]
    ),
    # TODO(aznashwan): deployment actions should ideally be
    # declared in a separate module
    policy.DocumentedRuleDefault(
        get_deployments_policy_label('cancel'),
        DEPLOYMENTS_POLICY_DEFAULT_RULE,
        "Cancel a running Deployment",
        [
            {
                "path": "/deployments/{deployment_id}/actions/",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_deployments_policy_label('delete'),
        DEPLOYMENTS_POLICY_DEFAULT_RULE,
        "Delete Deployment",
        [
            {
                "path": "/deployment/{deployment_id}",
                "method": "DELETE"
            }
        ]
    )
]


def list_rules():
    return DEPLOYMENTS_POLICY_DEFAULT_RULES
