# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

from oslo_log import log as logging

from coriolis import api
from coriolis.api.v1 import diagnostics
from coriolis.api.v1 import endpoint_actions
from coriolis.api.v1 import endpoint_destination_options
from coriolis.api.v1 import endpoint_instances
from coriolis.api.v1 import endpoint_networks
from coriolis.api.v1 import endpoint_source_options
from coriolis.api.v1 import endpoint_storage
from coriolis.api.v1 import endpoints
from coriolis.api.v1 import migration_actions
from coriolis.api.v1 import migrations
from coriolis.api.v1 import provider_schemas
from coriolis.api.v1 import providers
from coriolis.api.v1 import replica_actions
from coriolis.api.v1 import replica_schedules
from coriolis.api.v1 import replica_tasks_execution_actions
from coriolis.api.v1 import replica_tasks_executions
from coriolis.api.v1 import replicas

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

        self.resources['endpoints'] = endpoints.create_resource()
        mapper.resource('endpoint', 'endpoints',
                        controller=self.resources['endpoints'],
                        collection={'detail': 'GET'},
                        member={'action': 'POST'})

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

        self.resources['migrations'] = migrations.create_resource()
        mapper.resource('migration', 'migrations',
                        controller=self.resources['migrations'],
                        collection={'detail': 'GET'},
                        member={'action': 'POST'})

        migration_actions_resource = migration_actions.create_resource()
        self.resources['migration_actions'] = migration_actions_resource
        migration_path = '/{project_id}/migrations/{id}'
        mapper.connect('migration_actions',
                       migration_path + '/actions',
                       controller=self.resources['migration_actions'],
                       action='action',
                       conditions={'method': 'POST'})

        self.resources['replicas'] = replicas.create_resource()
        mapper.resource('replica', 'replicas',
                        controller=self.resources['replicas'],
                        collection={'detail': 'GET'},
                        member={'action': 'POST'})

        replica_actions_resource = replica_actions.create_resource()
        self.resources['replica_actions'] = replica_actions_resource
        migration_path = '/{project_id}/replicas/{id}'
        mapper.connect('replica_actions',
                       migration_path + '/actions',
                       controller=self.resources['replica_actions'],
                       action='action',
                       conditions={'method': 'POST'})

        self.resources['replica_tasks_executions'] = \
            replica_tasks_executions.create_resource()
        mapper.resource('execution', 'replicas/{replica_id}/executions',
                        controller=self.resources['replica_tasks_executions'],
                        collection={'detail': 'GET'},
                        member={'action': 'POST'})

        replica_tasks_execution_actions_resource = \
            replica_tasks_execution_actions.create_resource()
        self.resources['replica_tasks_execution_actions'] = \
            replica_tasks_execution_actions_resource
        migration_path = '/{project_id}/replicas/{replica_id}/executions/{id}'
        mapper.connect('replica_tasks_execution_actions',
                       migration_path + '/actions',
                       controller=self.resources[
                           'replica_tasks_execution_actions'],
                       action='action',
                       conditions={'method': 'POST'})

        sched = replica_schedules.create_resource()
        self.resources['replica_schedules'] = sched
        mapper.resource('replica_schedule', 'replicas/{replica_id}/schedules',
                        controller=self.resources['replica_schedules'],
                        collection={'index': 'GET'},
                        member={'action': 'POST'})

        diag = diagnostics.create_resource()
        self.resources['diagnostics'] = diag
        mapper.resource('diagnostics', 'diagnostics',
                        controller=self.resources['diagnostics'])
