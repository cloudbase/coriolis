# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_policy import policy

from coriolis.policies import base


MIGRATIONS_POLICY_PREFIX = "%s:migrations" % base.CORIOLIS_POLICIES_PREFIX
MIGRATIONS_POLICY_DEFAULT_RULE = "rule:admin_or_owner"


def get_migrations_policy_label(rule_label):
    return "%s:%s" % (
        MIGRATIONS_POLICY_PREFIX, rule_label)


MIGRATIONS_POLICY_DEFAULT_RULES = [
    policy.DocumentedRuleDefault(
        get_migrations_policy_label('create'),
        MIGRATIONS_POLICY_DEFAULT_RULE,
        "Create a migration",
        [
            {
                "path": "/migrations",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_migrations_policy_label('list'),
        MIGRATIONS_POLICY_DEFAULT_RULE,
        "List migrations",
        [
            {
                "path": "/migrations",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_migrations_policy_label('show'),
        MIGRATIONS_POLICY_DEFAULT_RULE,
        "Show details for a migration",
        [
            {
                "path": "/migrations/{migration_id}",
                "method": "GET"
            }
        ]
    ),
    # TODO(aznashwan): migration actions should ideally be
    # declared in a separate module
    policy.DocumentedRuleDefault(
        get_migrations_policy_label('cancel'),
        MIGRATIONS_POLICY_DEFAULT_RULE,
        "Cancel a running Migration",
        [
            {
                "path": "/migrations/{migration_id}/actions",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_migrations_policy_label('delete'),
        MIGRATIONS_POLICY_DEFAULT_RULE,
        "Delete Migration",
        [
            {
                "path": "/migrations/{migration_id}",
                "method": "DELETE"
            }
        ]
    )
]


def list_rules():
    return MIGRATIONS_POLICY_DEFAULT_RULES
