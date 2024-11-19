# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_policy import policy

from coriolis.policies import base


TRANSFERS_POLICY_PREFIX = "%s:transfers" % base.CORIOLIS_POLICIES_PREFIX
TRANSFERS_POLICY_DEFAULT_RULE = "rule:admin_or_owner"


def get_transfers_policy_label(rule_label):
    return "%s:%s" % (
        TRANSFERS_POLICY_PREFIX, rule_label)


TRANSFERS_POLICY_DEFAULT_RULES = [
    policy.DocumentedRuleDefault(
        get_transfers_policy_label('create'),
        TRANSFERS_POLICY_DEFAULT_RULE,
        "Create a Transfer",
        [
            {
                "path": "/transfers",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_transfers_policy_label('list'),
        TRANSFERS_POLICY_DEFAULT_RULE,
        "List Transfers",
        [
            {
                "path": "/transfers",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_transfers_policy_label('show'),
        TRANSFERS_POLICY_DEFAULT_RULE,
        "Show details for Transfer",
        [
            {
                "path": "/transfers/{transfer_id}",
                "method": "GET"
            }
        ]
    ),
    # TODO(aznashwan): transfer actions should ideally be
    # declared in a separate module
    policy.DocumentedRuleDefault(
        get_transfers_policy_label('delete_disks'),
        TRANSFERS_POLICY_DEFAULT_RULE,
        "Delete Transfer Disks",
        [
            {
                "path": "/transfers/{transfer_id}/actions",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_transfers_policy_label('delete'),
        TRANSFERS_POLICY_DEFAULT_RULE,
        "Delete Transfer",
        [
            {
                "path": "/transfers/{transfer_id}",
                "method": "DELETE"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_transfers_policy_label('update'),
        TRANSFERS_POLICY_DEFAULT_RULE,
        "Update Transfer",
        [
            {
                "path": "/transfers/{transfer_id}",
                "method": "POST"
            }
        ]
    )

]


def list_rules():
    return TRANSFERS_POLICY_DEFAULT_RULES
