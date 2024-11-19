# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis import api
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
from coriolis.api.v1 import services
from coriolis.api.v1 import transfer_actions
from coriolis.api.v1 import transfer_schedules
from coriolis.api.v1 import transfer_tasks_execution_actions
from coriolis.api.v1 import transfer_tasks_executions
from coriolis.api.v1 import transfers

LOG = logging.getLogger(__name__)


class ExtensionManager(object):
    def get_resources(self):
        return []

    def get_controller_extensions(self):
        return []


class APIRouter(api.APIRouter):
    ExtensionManager = ExtensionManager

    def _setup_routes(self, mapper, ext_mgr):
        mapper.redirect("", "/")

        self.resources['providers'] = providers.create_resource()
        mapper.resource('provider', 'providers',
                        controller=self.resources['providers'])

        self.resources['regions'] = regions.create_resource()
        mapper.resource('region', 'regions',
                        controller=self.resources['regions'],
                        collection={'detail': 'GET'})

        self.resources['endpoints'] = endpoints.create_resource()
        mapper.resource('endpoint', 'endpoints',
                        controller=self.resources['endpoints'],
                        collection={'detail': 'GET'},
                        member={'action': 'POST'})

        self.resources['services'] = services.create_resource()
        mapper.resource('service', 'services',
                        controller=self.resources['services'],
                        collection={'detail': 'GET'})

        self.resources['minion_pools'] = minion_pools.create_resource()
        mapper.resource('minion_pool', 'minion_pools',
                        controller=self.resources['minion_pools'],
                        collection={'detail': 'GET'})

        minion_pool_actions_resource = minion_pool_actions.create_resource()
        self.resources['minion_pool_actions'] = minion_pool_actions_resource
        minion_pool_path = '/{project_id}/minion_pools/{id}'
        mapper.connect('minion_pool_actions',
                       minion_pool_path + '/actions',
                       controller=self.resources['minion_pool_actions'],
                       action='action',
                       conditions={'method': 'POST'})

        self.resources['endpoint_source_minion_pool_options'] = \
            endpoint_source_minion_pool_options.create_resource()
        mapper.resource('minion_pool_options',
                        'endpoints/{endpoint_id}/source-minion-pool-options',
                        controller=(
                            self.resources[
                                'endpoint_source_minion_pool_options']))

        self.resources['endpoint_destination_minion_pool_options'] = \
            endpoint_destination_minion_pool_options.create_resource()
        mapper.resource(
            'minion_pool_options',
            'endpoints/{endpoint_id}/destination-minion-pool-options',
            controller=(self.resources
                        ['endpoint_destination_minion_pool_options']))

        endpoint_actions_resource = endpoint_actions.create_resource()
        self.resources['endpoint_actions'] = endpoint_actions_resource
        endpoint_path = '/{project_id}/endpoints/{id}'
        mapper.connect('endpoint_actions',
                       endpoint_path + '/actions',
                       controller=self.resources['endpoint_actions'],
                       action='action',
                       conditions={'method': 'POST'})

        self.resources['endpoint_instances'] = \
            endpoint_instances.create_resource()
        mapper.resource('instance', 'endpoints/{endpoint_id}/instances',
                        controller=self.resources['endpoint_instances'])

        self.resources['endpoint_networks'] = \
            endpoint_networks.create_resource()
        mapper.resource('network', 'endpoints/{endpoint_id}/networks',
                        controller=self.resources['endpoint_networks'])

        self.resources['endpoint_storage'] = \
            endpoint_storage.create_resource()
        mapper.resource('storage', 'endpoints/{endpoint_id}/storage',
                        controller=self.resources['endpoint_storage'])

        self.resources['endpoint_destination_options'] = \
            endpoint_destination_options.create_resource()
        mapper.resource('destination_options',
                        'endpoints/{endpoint_id}/destination-options',
                        controller=(
                            self.resources['endpoint_destination_options']))

        self.resources['endpoint_source_options'] = \
            endpoint_source_options.create_resource()
        mapper.resource('source_options',
                        'endpoints/{endpoint_id}/source-options',
                        controller=(
                            self.resources['endpoint_source_options']))

        self.resources['provider_schemas'] = \
            provider_schemas.create_resource()
        mapper.resource('provider_schemas',
                        'providers/{platform_name}/schemas/{provider_type}',
                        controller=self.resources['provider_schemas'])

        self.resources['deployments'] = deployments.create_resource()
        mapper.resource('deployment', 'deployments',
                        controller=self.resources['deployments'],
                        collection={'detail': 'GET'},
                        member={'action': 'POST'})

        deployments_actions_resource = deployment_actions.create_resource()
        self.resources['deployment_actions'] = deployments_actions_resource
        deployment_path = '/{project_id}/deployments/{id}'
        mapper.connect('deployment_actions',
                       deployment_path + '/actions',
                       controller=self.resources['deployment_actions'],
                       action='action',
                       conditions={'method': 'POST'})

        self.resources['transfers'] = transfers.create_resource()
        mapper.resource('transfer', 'transfers',
                        controller=self.resources['transfers'],
                        collection={'detail': 'GET'},
                        member={'action': 'POST'})

        transfer_actions_resource = transfer_actions.create_resource()
        self.resources['transfer_actions'] = transfer_actions_resource
        migration_path = '/{project_id}/transfers/{id}'
        mapper.connect('transfer_actions',
                       migration_path + '/actions',
                       controller=self.resources['transfer_actions'],
                       action='action',
                       conditions={'method': 'POST'})

        self.resources['transfer_tasks_executions'] = \
            transfer_tasks_executions.create_resource()
        mapper.resource('execution', 'transfers/{transfer_id}/executions',
                        controller=self.resources['transfer_tasks_executions'],
                        collection={'detail': 'GET'},
                        member={'action': 'POST'})

        transfer_tasks_execution_actions_resource = \
            transfer_tasks_execution_actions.create_resource()
        self.resources['transfer_tasks_execution_actions'] = \
            transfer_tasks_execution_actions_resource
        migration_path = ('/{project_id}/transfers/{transfer_id}/'
                          'executions/{id}')
        mapper.connect('transfer_tasks_execution_actions',
                       migration_path + '/actions',
                       controller=self.resources[
                           'transfer_tasks_execution_actions'],
                       action='action',
                       conditions={'method': 'POST'})

        sched = transfer_schedules.create_resource()
        self.resources['transfer_schedules'] = sched
        mapper.resource('transfer_schedule',
                        'transfers/{transfer_id}/schedules',
                        controller=self.resources['transfer_schedules'],
                        collection={'index': 'GET'},
                        member={'action': 'POST'})

        diag = diagnostics.create_resource()
        self.resources['diagnostics'] = diag
        mapper.resource('diagnostics', 'diagnostics',
                        controller=self.resources['diagnostics'])
