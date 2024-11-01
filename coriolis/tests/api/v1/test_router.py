# Copyright 2023 Cloudbase Solutions Srl
# All Rights Reserved.

from unittest import mock

from coriolis.api.v1 import deployment_actions
from coriolis.api.v1 import deployments
from coriolis.api.v1 import diagnostics
from coriolis.api.v1 import endpoint_actions
from coriolis.api.v1 import endpoint_destination_minion_pool_options
from coriolis.api.v1 import endpoint_destination_options
from coriolis.api.v1 import endpoint_instances
from coriolis.api.v1 import endpoint_networks
from coriolis.api.v1 import endpoint_source_minion_pool_options
from coriolis.api.v1 import endpoint_source_options
from coriolis.api.v1 import endpoint_storage
from coriolis.api.v1 import endpoints
from coriolis.api.v1 import minion_pool_actions
from coriolis.api.v1 import minion_pools
from coriolis.api.v1 import provider_schemas
from coriolis.api.v1 import providers
from coriolis.api.v1 import regions
from coriolis.api.v1 import replica_actions
from coriolis.api.v1 import replica_schedules
from coriolis.api.v1 import replica_tasks_execution_actions
from coriolis.api.v1 import replica_tasks_executions
from coriolis.api.v1 import replicas
from coriolis.api.v1 import router
from coriolis.api.v1 import services
from coriolis.tests import test_base


class APIRouterTestCase(test_base.CoriolisBaseTestCase):
    """Test suite for the Coriolis APIRouter v1 API"""

    def setUp(self):
        super(APIRouterTestCase, self).setUp()
        self.router = router.APIRouter()

    @mock.patch.object(deployments, 'create_resource')
    @mock.patch.object(deployment_actions, 'create_resource')
    @mock.patch.object(diagnostics, 'create_resource')
    @mock.patch.object(replica_schedules, 'create_resource')
    @mock.patch.object(replica_tasks_execution_actions, 'create_resource')
    @mock.patch.object(replica_tasks_executions, 'create_resource')
    @mock.patch.object(replica_actions, 'create_resource')
    @mock.patch.object(replicas, 'create_resource')
    @mock.patch.object(provider_schemas, 'create_resource')
    @mock.patch.object(endpoint_source_options, 'create_resource')
    @mock.patch.object(endpoint_destination_options, 'create_resource')
    @mock.patch.object(endpoint_storage, 'create_resource')
    @mock.patch.object(endpoint_networks, 'create_resource')
    @mock.patch.object(endpoint_instances, 'create_resource')
    @mock.patch.object(endpoint_actions, 'create_resource')
    @mock.patch.object(endpoint_destination_minion_pool_options,
                       'create_resource')
    @mock.patch.object(endpoint_source_minion_pool_options, 'create_resource')
    @mock.patch.object(minion_pool_actions, 'create_resource')
    @mock.patch.object(minion_pools, 'create_resource')
    @mock.patch.object(services, 'create_resource')
    @mock.patch.object(endpoints, 'create_resource')
    @mock.patch.object(regions, 'create_resource')
    @mock.patch.object(providers, 'create_resource')
    def test_setup_routes(
        self,
        mock_providers_create_resource,
        mock_regions_create_resource,
        mock_endpoints_create_resource,
        mock_services_create_resource,
        mock_minion_pools_create_resource,
        mock_minion_pool_actions_create_resource,
        mock_endpoint_source_minion_pool_options_create_resource,
        mock_endpoint_destination_minion_pool_options_create_resource,
        mock_endpoint_actions_create_resource,
        mock_endpoint_instances_create_resource,
        mock_endpoint_networks_create_resource,
        mock_endpoint_storage_create_resource,
        mock_endpoint_destination_options_create_resource,
        mock_endpoint_source_options_create_resource,
        mock_provider_schemas_create_resource,
        mock_replicas_create_resource,
        mock_replica_actions_create_resource,
        mock_replica_tasks_executions_create_resource,
        mock_replica_tasks_execution_actions_create_resource,
        mock_replica_schedules_create_resource,
        mock_diagnostics_create_resource,
        mock_deployment_actions_create_resource,
        mock_deployments_create_resource
    ):
        ext_mgr = mock.sentinel.ext_mgr
        mapper = mock.Mock()

        resource_calls = [
            mock.call(
                'provider', 'providers',
                controller=mock_providers_create_resource.return_value
            ),
            mock.call(
                'region', 'regions',
                controller=mock_regions_create_resource.return_value,
                collection={'detail': 'GET'}
            ),
            mock.call(
                'endpoint', 'endpoints',
                controller=mock_endpoints_create_resource.return_value,
                collection={'detail': 'GET'},
                member={'action': 'POST'}
            ),
            mock.call(
                'service', 'services',
                controller=mock_services_create_resource.return_value,
                collection={'detail': 'GET'}
            ),
            mock.call(
                'minion_pool', 'minion_pools',
                controller=mock_minion_pools_create_resource.return_value,
                collection={'detail': 'GET'}
            ),
            mock.call(
                'minion_pool_options',
                'endpoints/{endpoint_id}/source-minion-pool-options',
                controller=
                mock_endpoint_source_minion_pool_options_create_resource.
                return_value,
            ),
            mock.call(
                'minion_pool_options',
                'endpoints/{endpoint_id}/destination-minion-pool-options',
                controller=
                mock_endpoint_destination_minion_pool_options_create_resource.
                return_value,
            ),
            mock.call(
                'instance', 'endpoints/{endpoint_id}/instances',
                controller=
                mock_endpoint_instances_create_resource.return_value,
            ),
            mock.call(
                'network', 'endpoints/{endpoint_id}/networks',
                controller=mock_endpoint_networks_create_resource.return_value,
            ),
            mock.call(
                'storage', 'endpoints/{endpoint_id}/storage',
                controller=mock_endpoint_storage_create_resource.return_value,
            ),
            mock.call(
                'destination_options',
                'endpoints/{endpoint_id}/destination-options',
                controller=
                mock_endpoint_destination_options_create_resource.return_value,
            ),
            mock.call(
                'source_options',
                'endpoints/{endpoint_id}/source-options',
                controller=
                mock_endpoint_source_options_create_resource.return_value,
            ),
            mock.call(
                'provider_schemas',
                'providers/{platform_name}/schemas/{provider_type}',
                controller=mock_provider_schemas_create_resource.return_value,
            ),
            mock.call(
                'replica', 'replicas',
                controller=mock_replicas_create_resource.return_value,
                collection={'detail': 'GET'},
                member={'action': 'POST'}
            ),
            mock.call(
                'execution',
                'replicas/{replica_id}/executions',
                controller=
                mock_replica_tasks_executions_create_resource.return_value,
                collection={'detail': 'GET'},
                member={'action': 'POST'}
            ),
            mock.call(
                'replica_schedule',
                'replicas/{replica_id}/schedules',
                controller=
                mock_replica_schedules_create_resource.return_value,
                collection={'index': 'GET'},
                member={'action': 'POST'}
            ),
            mock.call(
                'diagnostics', 'diagnostics',
                controller=mock_diagnostics_create_resource.return_value,
            ),
            mock.call(
                'deployment', 'deployments',
                controller=mock_deployments_create_resource.return_value,
                collection={'detail': 'GET'},
                member={'action': 'POST'}
            ),
        ]

        connect_calls = [
            mock.call(
                'minion_pool_actions',
                '/{project_id}/minion_pools/{id}/actions',
                controller=
                mock_minion_pool_actions_create_resource.return_value,
                action='action',
                conditions={'method': 'POST'}
            ),
            mock.call(
                'endpoint_actions',
                '/{project_id}/endpoints/{id}/actions',
                controller=
                mock_endpoint_actions_create_resource.return_value,
                action='action',
                conditions={'method': 'POST'}
            ),
            mock.call(
                'replica_actions',
                '/{project_id}/replicas/{id}/actions',
                controller=mock_replica_actions_create_resource.return_value,
                action='action',
                conditions={'method': 'POST'}
            ),
            mock.call(
                'replica_tasks_execution_actions',
                '/{project_id}/replicas/{replica_id}/executions/{id}/actions',
                controller=
                mock_replica_tasks_execution_actions_create_resource.
                return_value,
                action='action',
                conditions={'method': 'POST'}
            ),
            mock.call(
                'deployment_actions', '/{project_id}/deployments/{id}/actions',
                controller=(
                    mock_deployment_actions_create_resource.return_value),
                action='action',
                conditions={"method": "POST"}
            ),
        ]

        self.router._setup_routes(mapper, ext_mgr)

        mapper.redirect.assert_called_once_with("", "/")
        mapper.resource.assert_has_calls(resource_calls, any_order=True)
        mapper.connect.assert_has_calls(connect_calls, any_order=True)
