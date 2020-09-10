# Copyright 2018 Cloudbase Solutions Srl
# All Rights Reserved.


from oslo_policy import policy

from coriolis.policies import base


ENDPOINTS_POLICY_PREFIX = "%s:endpoints" % base.CORIOLIS_POLICIES_PREFIX
ENDPOINTS_POLICY_DEFAULT_RULE = "rule:admin_or_owner"


def get_endpoints_policy_label(rule_label):
    return "%s:%s" % (
        ENDPOINTS_POLICY_PREFIX, rule_label)


ENDPOINTS_POLICY_DEFAULT_RULES = [
    policy.DocumentedRuleDefault(
        get_endpoints_policy_label('create'),
        ENDPOINTS_POLICY_DEFAULT_RULE,
        "Create an endpoint",
        [
            {
                "path": "/endpoints",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_endpoints_policy_label('list'),
        ENDPOINTS_POLICY_DEFAULT_RULE,
        "List endpoints",
        [
            {
                "path": "/endpoints",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_endpoints_policy_label('show'),
        ENDPOINTS_POLICY_DEFAULT_RULE,
        "Show details for endpoint",
        [
            {
                "path": "/endpoint/{endpoint_id}",
                "method": "GET"
            }
        ]
    ),
    # TODO(aznashwan): double-check this:
    policy.DocumentedRuleDefault(
        get_endpoints_policy_label('update'),
        ENDPOINTS_POLICY_DEFAULT_RULE,
        "Update details for endpoint",
        [
            {
                "path": "/endpoint/{endpoint_id}",
                "method": "PUT"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_endpoints_policy_label('delete'),
        ENDPOINTS_POLICY_DEFAULT_RULE,
        "Delete endpoint",
        [
            {
                "path": "/endpoint/{endpoint_id}",
                "method": "DELETE"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_endpoints_policy_label('validate_connection'),
        ENDPOINTS_POLICY_DEFAULT_RULE,
        "Validate endpoint connection info",
        [
            {
                "path": "/endpoint/{endpoint_id}/action",
                "method": "POST"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_endpoints_policy_label('list_instances'),
        ENDPOINTS_POLICY_DEFAULT_RULE,
        "List instances available for migration/replication",
        [
            {
                "path": "/endpoint/{endpoint_id}/instances",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_endpoints_policy_label('get_instance'),
        ENDPOINTS_POLICY_DEFAULT_RULE,
        "Get details for given instance available for migration/replication",
        [
            {
                "path": "/endpoint/{endpoint_id}/instances/{instance_name}",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_endpoints_policy_label('list_networks'),
        ENDPOINTS_POLICY_DEFAULT_RULE,
        "List networks available on the given endpoint",
        [
            {
                "path": "/endpoint/{endpoint_id}/networks",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_endpoints_policy_label('list_storage'),
        ENDPOINTS_POLICY_DEFAULT_RULE,
        "List storage types available on the given endpoint",
        [
            {
                "path": "/endpoint/{endpoint_id}/storage",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_endpoints_policy_label('list_destination_options'),
        ENDPOINTS_POLICY_DEFAULT_RULE,
        "List available destination options for endpoint",
        [
            {
                "path": "/endpoint/{endpoint_id}/destination-options",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_endpoints_policy_label('list_source_options'),
        ENDPOINTS_POLICY_DEFAULT_RULE,
        "List available source options for endpoint",
        [
            {
                "path": "/endpoint/{endpoint_id}/source-options",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_endpoints_policy_label('list_source_minion_pool_options'),
        ENDPOINTS_POLICY_DEFAULT_RULE,
        "List available source minion pool options for endpoint",
        [
            {
                "path": "/endpoint/{endpoint_id}/source-minion-pool-options",
                "method": "GET"
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        get_endpoints_policy_label('list_destination_minion_pool_options'),
        ENDPOINTS_POLICY_DEFAULT_RULE,
        "List available destination pool options for endpoint",
        [
            {
                "path": (
                    "/endpoint/{endpoint_id}/destination-minion-pool-options"),
                "method": "GET"
            }
        ]
    ),
]


def list_rules():
    return ENDPOINTS_POLICY_DEFAULT_RULES
