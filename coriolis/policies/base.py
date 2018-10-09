# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_policy import policy


# NOTE: the policy prefix is convened to be the 'service_type' of
# the Coriolis endpoint in the Keystone Service Catalog.
CORIOLIS_POLICIES_PREFIX = "migration"

# NOTE: the below are default rules which can be fallen back to:
POLICY_DEFAULT_RULES = [
    policy.RuleDefault(
        "admin",
        "is_admin:True",
        "Default admin rule which is omnipotent."),
    policy.RuleDefault(
        "admin_or_owner",
        "is_admin:True or project_id:%(project_id)s",
        "Default rule for most API accesses."),
]


def list_rules():
    return POLICY_DEFAULT_RULES
